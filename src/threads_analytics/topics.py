"""Claude-powered topic extraction from the user's own posts."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from anthropic import Anthropic
from sqlalchemy import select

from .config import get_settings
from .db import session_scope
from .models import MyPost, PostTopic, Topic

log = logging.getLogger(__name__)

SYSTEM = (
    "You are a content strategist analyzing a single creator's Threads posts. "
    "Identify the 5 to 10 distinct topics they actually post about. "
    "Prefer specific, usable labels ('building AI agents', 'Nigerian tech scene') "
    "over vague ones ('technology', 'life'). "
    "Topics should be mutually exclusive and collectively cover the posts given."
)

SCHEMA_INSTRUCTION = (
    "Respond with ONLY a JSON object, no prose, no fences. Shape:\n"
    "{\n"
    '  "topics": [\n'
    '    {"label": "...", "description": "one sentence", "post_ids": ["id1", "id2"]}\n'
    "  ]\n"
    "}\n"
    "Each post_id must match an id from the input. A post can belong to multiple topics."
)


def extract_and_persist_topics(min_new_posts: int = 10) -> list[Topic]:
    """Call Claude to extract topics if there are enough new posts since last extraction."""
    settings = get_settings()

    with session_scope() as session:
        posts = session.scalars(
            select(MyPost).order_by(MyPost.created_at.desc()).limit(100)
        ).all()
        existing_topics = session.scalars(select(Topic)).all()
        existing_count = len(existing_topics)

        if not posts:
            log.info("no posts to extract topics from")
            return []

        if existing_count > 0:
            # Check how many posts postdate the last extraction
            last_extracted = max(t.extracted_at for t in existing_topics)
            new_posts = [p for p in posts if p.created_at > last_extracted]
            if len(new_posts) < min_new_posts:
                log.info(
                    "only %d new posts since last topic extraction; skipping",
                    len(new_posts),
                )
                return list(existing_topics)

        post_payload = [
            {"id": p.thread_id, "text": (p.text or "")[:500]} for p in posts if p.text
        ]

    if not post_payload:
        return []

    client = Anthropic(api_key=settings.anthropic_api_key)
    user_msg = (
        f"Here are {len(post_payload)} recent posts from a creator. Extract their topics.\n\n"
        f"{SCHEMA_INSTRUCTION}\n\n"
        f"POSTS:\n{json.dumps(post_payload, ensure_ascii=False)}"
    )

    resp = client.messages.create(
        model=settings.claude_topic_model,
        max_tokens=2048,
        system=SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    data = _safe_json(text)
    if not data or "topics" not in data:
        log.warning("topic extraction produced no parseable JSON: %s", text[:400])
        return []

    now = datetime.now(timezone.utc)
    saved: list[Topic] = []
    with session_scope() as session:
        # Wipe prior post_topic links for clean re-linking
        for row in session.scalars(select(PostTopic)).all():
            session.delete(row)

        for t in data["topics"]:
            label = (t.get("label") or "").strip()
            if not label:
                continue
            topic = session.scalar(select(Topic).where(Topic.label == label))
            if topic is None:
                topic = Topic(label=label, description=t.get("description", ""))
                session.add(topic)
                session.flush()
            else:
                topic.description = t.get("description", topic.description)
                topic.extracted_at = now
            for pid in t.get("post_ids", []) or []:
                session.add(PostTopic(post_thread_id=pid, topic_id=topic.id, confidence=1.0))
            saved.append(topic)

    log.info("extracted %d topics", len(saved))
    return saved


def _safe_json(text: str) -> dict | None:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to salvage the first {...} block
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return None
        return None
