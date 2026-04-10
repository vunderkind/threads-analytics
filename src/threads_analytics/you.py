"""The 'You' feature — anti-homogenization guardrail.

The risk with any scientific optimization loop is that you grind yourself into a
generic algorithm-friendly template. This module exists to identify and preserve
what's uniquely YOU so the experiment engine doesn't sand off the signal that
makes you worth following in the first place.

Claude is asked to look at the user's recent posts and extract: voice traits,
topic crossovers, stylistic signatures, which posts sound most like them, what
to protect no matter what, and what homogenization patterns to beware of.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from anthropic import Anthropic
from sqlalchemy import desc, select

from .config import get_settings
from .db import session_scope
from .models import MyPost, MyReply, Profile, Run, YouProfile

log = logging.getLogger(__name__)


SYSTEM = (
    "You are an editor who protects writers from losing their voice to audience "
    "optimization. You look at a creator's posts and extract what is uniquely "
    "THEIRS — the stylistic quirks, unusual topic crossovers, particular voice "
    "registers — so those can be preserved no matter what the algorithm rewards. "
    "You are writing for the creator themselves, not an academic. Be specific, "
    "cite evidence, and stay within hard word limits. No paragraphs of prose. "
    "If they sound like a thousand other AI-commentary accounts, say so bluntly."
)


SCHEMA_INSTRUCTION = """Respond with ONLY a JSON object, no prose, no fences.

Shape (HARD word limits — do not exceed):
{
  "coreIdentity": "2 sentences, max 45 words total. Describe this person in their own frame, not the algorithm's. Specific and concrete.",
  "stylisticSignatures": [
    {"signature": "short label, 2-5 words", "evidence": "1 sentence ≤12 words citing where it shows up"}
  ],
  "postsThatSoundMostLikeYou": [
    {"post_id": "id from input", "text": "first 180 chars", "why": "1 sentence ≤15 words"}
  ],
  "protectList": [
    "6 items, each 1 sentence ≤20 words. Things to NEVER optimize away. Specific."
  ],
  "doubleDownList": [
    "5 items, each 1 sentence ≤20 words. Uniquely theirs AND working."
  ],
  "homogenizationRisks": [
    {"risk": "≤12 words", "if_you_do_this_you_lose": "≤15 words"}
  ]
}

Produce 4-6 stylistic signatures, exactly 3 postsThatSoundMostLikeYou, exactly 6 protect items, exactly 5 double-down items, 3-4 homogenization risks. Stop mid-thought if you hit a word limit. Distinctive voice traits and unique topic crossovers are NOT needed — absorb them into coreIdentity or signatures."""


def generate_you_profile(run: Run, post_limit: int = 50) -> int | None:
    """Generate and persist the 'You' profile for this run."""
    settings = get_settings()

    with session_scope() as session:
        profile = session.scalar(select(Profile).limit(1))
        profile_payload = (
            {
                "username": profile.username,
                "biography": profile.biography,
            }
            if profile
            else None
        )

        posts = session.scalars(
            select(MyPost).order_by(desc(MyPost.created_at)).limit(post_limit)
        ).all()
        post_payload = [
            {
                "id": p.thread_id,
                "text": (p.text or "")[:500],
                "media_type": p.media_type,
            }
            for p in posts
            if p.text
        ]

        replies = session.scalars(
            select(MyReply).order_by(desc(MyReply.created_at)).limit(15)
        ).all()
        reply_payload = [{"text": (r.text or "")[:400]} for r in replies]

    if not post_payload:
        log.info("no posts to build You profile")
        return None

    text_body = (
        f"{SCHEMA_INSTRUCTION}\n\n"
        "DATA:\n"
        + json.dumps(
            {
                "profile": profile_payload,
                "posts": post_payload,
                "replies": reply_payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    client = Anthropic(api_key=settings.anthropic_api_key)
    resp = client.messages.create(
        model=settings.claude_recommender_model,
        max_tokens=3500,
        system=SYSTEM,
        messages=[{"role": "user", "content": text_body}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    data = _safe_json(text)
    if not data:
        log.warning("you profile produced no parseable JSON: %s", text[:400])
        return None

    with session_scope() as session:
        existing = session.get(YouProfile, run.id)
        if existing is None:
            existing = YouProfile(run_id=run.id)
            session.add(existing)
        existing.core_identity = data.get("coreIdentity", "")
        existing.distinctive_voice_traits = data.get("distinctiveVoiceTraits") or []
        existing.unique_topic_crossovers = data.get("uniqueTopicCrossovers") or []
        existing.stylistic_signatures = data.get("stylisticSignatures") or []
        existing.posts_that_sound_most_like_you = data.get("postsThatSoundMostLikeYou") or []
        existing.protect_list = data.get("protectList") or []
        existing.double_down_list = data.get("doubleDownList") or []
        existing.homogenization_risks = data.get("homogenizationRisks") or []
        existing.raw_json = data
        existing.created_at = datetime.now(timezone.utc)

    log.info("persisted 'You' profile for run %d", run.id)
    return run.id


def _safe_json(text: str) -> dict | None:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return None
        return None
