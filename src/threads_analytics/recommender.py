"""Claude-powered recommendation synthesis.

Inputs: the user's PatternReport, affinity creators' PatternReport, extracted topics,
and the outcomes of prior recommendations. Output: 5-8 ranked, evidence-backed
recommendations persisted to the `recommendations` table.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from anthropic import Anthropic
from sqlalchemy import select

from .analyzer import PatternReport
from .config import get_settings
from .db import session_scope
from .models import (
    AffinityCreator,
    MyAccountInsight,
    Recommendation,
    RecommendationOutcome,
    Run,
    Topic,
)

log = logging.getLogger(__name__)


SYSTEM = (
    "You are a Threads growth strategist. You receive: (1) a stats report for the "
    "user's own posts, (2) an equivalent stats report for affinity creators "
    "successful in the same topic space (MAY BE EMPTY if the app is pending "
    "Meta App Review for keyword search access), (3) the user's topics, (4) the "
    "outcomes of prior recommendations on the user's follower count and engagement. "
    "Your job is to produce 5-8 specific, ranked, actionable recommendations "
    "that will plausibly grow the user's followers. Every recommendation MUST cite "
    "concrete numbers from the user's own data where possible — not generic advice. "
    "If affinity data is empty or sparse, lean on your strong general knowledge of "
    "what works on Threads for the user's specific topics (be specific: say 'for "
    "creators posting about X, the pattern that works is Y'), but ALWAYS ground "
    "each recommendation in something observable about the user's own posts. "
    "If prior recommendations had negative or zero outcomes, acknowledge it and "
    "adjust. If they had positive outcomes, double down on what worked."
)


SCHEMA_INSTRUCTION = (
    "Respond with ONLY a JSON object, no prose, no code fences. Shape:\n"
    "{\n"
    '  "recommendations": [\n'
    "    {\n"
    '      "rank": 1,\n'
    '      "category": "timing" | "format" | "hook" | "topic" | "cadence" | "engagement",\n'
    '      "title": "<=100 chars, imperative",\n'
    '      "body": "2-4 sentences explaining the specific action",\n'
    '      "evidence": {"your_data": "...", "affinity_data": "...", "prior_outcome": "..."}\n'
    "    }\n"
    "  ]\n"
    "}\n"
    "Order by rank (1 = highest priority)."
)


def synthesize_recommendations(run: Run, my: PatternReport, affinity: PatternReport) -> list[int]:
    """Call Claude to generate recommendations, persist them, return the new IDs."""
    settings = get_settings()

    with session_scope() as session:
        topics = session.scalars(select(Topic)).all()
        topic_list = [{"label": t.label, "description": t.description} for t in topics]

        # Top 10 affinity creators by engagement score for the prompt
        creators = session.scalars(
            select(AffinityCreator).order_by(AffinityCreator.engagement_score.desc()).limit(10)
        ).all()
        creator_list = [
            {"handle": c.handle, "engagement_score": round(c.engagement_score, 2)}
            for c in creators
        ]

        # Prior recommendations + outcomes
        prior = session.scalars(
            select(Recommendation)
            .where(Recommendation.run_id != run.id)
            .order_by(Recommendation.created_at.desc())
            .limit(12)
        ).all()
        prior_payload: list[dict[str, Any]] = []
        for r in prior:
            outcomes = session.scalars(
                select(RecommendationOutcome).where(
                    RecommendationOutcome.recommendation_id == r.id
                )
            ).all()
            prior_payload.append(
                {
                    "title": r.title,
                    "category": r.category,
                    "status": r.status,
                    "outcomes": [
                        {
                            "follower_delta": o.follower_delta,
                            "engagement_delta": round(o.engagement_delta, 2),
                        }
                        for o in outcomes
                    ],
                }
            )

        # Current follower count for context
        account = session.scalar(
            select(MyAccountInsight).order_by(MyAccountInsight.fetched_at.desc()).limit(1)
        )
        follower_count = account.follower_count if account else None

    payload = {
        "handle": settings.threads_handle,
        "current_follower_count": follower_count,
        "your_patterns": my.to_dict(),
        "affinity_patterns": affinity.to_dict(),
        "your_topics": topic_list,
        "top_affinity_creators": creator_list,
        "prior_recommendations_and_outcomes": prior_payload,
    }

    client = Anthropic(api_key=settings.anthropic_api_key)
    user_msg = (
        f"Here is the full data bundle. Produce growth recommendations.\n\n"
        f"{SCHEMA_INSTRUCTION}\n\n"
        f"DATA:\n{json.dumps(payload, default=str, ensure_ascii=False)}"
    )
    resp = client.messages.create(
        model=settings.claude_recommender_model,
        max_tokens=4096,
        system=SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    data = _safe_json(text)
    if not data or "recommendations" not in data:
        log.warning("recommender produced no parseable JSON: %s", text[:400])
        return []

    now = datetime.now(timezone.utc)
    new_ids: list[int] = []
    with session_scope() as session:
        for item in data["recommendations"]:
            rec = Recommendation(
                run_id=run.id,
                rank=int(item.get("rank") or 0),
                category=(item.get("category") or "general")[:64],
                title=(item.get("title") or "")[:256],
                body=item.get("body") or "",
                evidence_json=item.get("evidence") or {},
                status="pending",
                created_at=now,
            )
            session.add(rec)
            session.flush()
            new_ids.append(rec.id)

    log.info("persisted %d recommendations for run %d", len(new_ids), run.id)
    return new_ids


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
