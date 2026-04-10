"""Public Perception v3 — research-grounded profile perception.

Framework:
  - Thin-slice judgment (Ambady & Rosenthal): brief exposures to a profile
    produce meaningful first impressions. Extraversion and Conscientiousness
    are judged accurately from thin slices; Openness/Agreeableness/Neuroticism
    take more exposure.
  - Brunswik lens model: targets externalize traits via distal cues (profile
    picture, bio, posts). Perceivers use those cues — validly or invalidly —
    to infer the underlying trait.
  - Big Five (OCEAN): the standard trait framework.

The output is structured around these constructs so a creator can see exactly
which cues their profile externalizes, which traits read clearly, and where
the risk of misreading is highest.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from anthropic import Anthropic
from sqlalchemy import desc, select

from .config import get_settings
from .db import session_scope
from .models import MyPost, MyReply, Profile, PublicPerception, Run

log = logging.getLogger(__name__)


SYSTEM = (
    "You are an honest stranger doing a 5-second 'thin slice' first-impression "
    "read of a Threads profile — grounded in the research literature on profile "
    "perception (Ambady thin-slice judgment, Brunswik lens model, Big Five). "
    "Your output is for the creator themselves, who wants to manage how they are "
    "perceived for growth.\n\n"
    "HARD RULES:\n"
    "1. Every Big Five rating MUST cite the specific distal cues from the "
    "profile (bio fragments, post excerpts, image descriptions) that you used "
    "to make the inference. No rating without cue evidence.\n"
    "2. Be honest about which traits are judge-able from a thin slice and "
    "which are not. Extraversion and Conscientiousness are reliably judged at "
    "5 seconds; Openness / Agreeableness / Neuroticism are less reliable. Say so.\n"
    "3. Use plain English. A non-technical creator must understand every "
    "field. Never exceed the word limits in the schema.\n"
    "4. Flag misread risks — places where the cues could be interpreted in a "
    "way that differs from who the creator actually is.\n"
    "5. Give exactly ONE highest-leverage profile fix: change X cue, expect Y "
    "shift in perception."
)


SCHEMA_INSTRUCTION = """Respond with ONLY a JSON object, no prose, no fences.

Shape:
{
  "thinSliceJudgment": "1 sentence, max 25 words. The snap first-impression that a stranger forms in the first 5 seconds.",
  "bigFive": {
    "extraversion":      { "rating": "high|medium|low", "confidence": "high|medium|low", "cues": ["≤4 cue descriptions, each ≤15 words, citing concrete things from the profile"] },
    "conscientiousness": { "rating": "high|medium|low", "confidence": "high|medium|low", "cues": [...] },
    "openness":          { "rating": "high|medium|low", "confidence": "high|medium|low", "cues": [...] },
    "agreeableness":     { "rating": "high|medium|low", "confidence": "high|medium|low", "cues": [...] },
    "neuroticism":       { "rating": "high|medium|low", "confidence": "high|medium|low", "cues": [...] }
  },
  "cueClarity": {
    "clearTraits": ["trait names where the cues are strong and read clearly"],
    "ambiguousTraits": ["trait names where the cues are mixed or contradictory"],
    "explanation": "1-2 sentences explaining which traits read clearly and which don't. Max 40 words."
  },
  "misreadRisks": [
    {"cue": "concrete thing a stranger notices (≤15 words)", "wrongInference": "what they might wrongly infer (≤20 words)"}
  ],
  "profileSignalQuality": {
    "rating": "strong|mixed|weak",
    "summary": "1 sentence, max 30 words, on whether the externalized cues align with a growth-friendly perception."
  },
  "highestLeverageFix": {
    "cueToChange": "specific element of the profile (e.g. 'bio first sentence', 'pinned post', 'profile picture')",
    "whatToChangeItTo": "concrete suggestion, max 25 words",
    "expectedShift": "what trait perception would improve, max 25 words"
  },
  "followTriggers": ["4 bullets, each max 15 words. Concrete cues that would make a stranger click follow."],
  "bounceReasons": ["4 bullets, each max 15 words. Concrete cues that would make a stranger bounce."]
}

DO NOT exceed word limits. If you run out of words, stop mid-thought."""


def generate_public_perception(run: Run) -> int | None:
    settings = get_settings()

    with session_scope() as session:
        profile = session.scalar(select(Profile).limit(1))
        profile_payload = None
        if profile is not None:
            profile_payload = {
                "username": profile.username,
                "biography": profile.biography,
                "profile_picture_url": profile.profile_picture_url,
            }

        posts = session.scalars(
            select(MyPost).order_by(desc(MyPost.created_at)).limit(10)
        ).all()
        post_payload = [
            {
                "text": (p.text or "")[:500],
                "media_type": p.media_type,
            }
            for p in posts
        ]

        image_posts = session.scalars(
            select(MyPost)
            .where(MyPost.media_url.is_not(None))
            .order_by(desc(MyPost.created_at))
            .limit(5)
        ).all()
        image_urls = [p.media_url for p in image_posts if p.media_url]

        replies = session.scalars(
            select(MyReply).order_by(desc(MyReply.created_at)).limit(5)
        ).all()
        reply_payload = [{"text": (r.text or "")[:400]} for r in replies]

    text_body = (
        f"{SCHEMA_INSTRUCTION}\n\n"
        "DATA:\n"
        + json.dumps(
            {
                "profile": profile_payload,
                "recent_posts": post_payload,
                "recent_replies": reply_payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    content: list[dict] = [{"type": "text", "text": text_body}]
    for url in image_urls:
        content.append({"type": "image", "source": {"type": "url", "url": url}})

    client = Anthropic(api_key=settings.anthropic_api_key)
    try:
        resp = client.messages.create(
            model=settings.claude_recommender_model,
            max_tokens=2500,
            system=SYSTEM,
            messages=[{"role": "user", "content": content}],
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("perception vision call failed, retrying text-only: %s", exc)
        resp = client.messages.create(
            model=settings.claude_recommender_model,
            max_tokens=2500,
            system=SYSTEM,
            messages=[{"role": "user", "content": text_body}],
        )

    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    data = _safe_json(text)
    if not data:
        log.warning("perception produced no parseable JSON: %s", text[:400])
        return None

    # Persist. We reuse the old columns to store v3 values that map reasonably,
    # and stash the full structured payload in raw_json for the template.
    with session_scope() as session:
        existing = session.get(PublicPerception, run.id)
        if existing is None:
            existing = PublicPerception(run_id=run.id)
            session.add(existing)
        # Map v3 → legacy fields for backward compat
        existing.one_sentence_cold = data.get("thinSliceJudgment", "")
        existing.first_impression = (
            (data.get("profileSignalQuality") or {}).get("summary") or ""
        )
        existing.positioning_clarity = (
            (data.get("cueClarity") or {}).get("explanation") or ""
        )
        existing.stickiness = ""
        existing.conversation_readiness = ""
        existing.follow_triggers = data.get("followTriggers") or []
        existing.bounce_reasons = data.get("bounceReasons") or []
        # Growth blockers → derived from the single highest-leverage fix
        fix = data.get("highestLeverageFix") or {}
        if fix:
            existing.growth_blockers = [
                f"Change {fix.get('cueToChange', '')}: {fix.get('whatToChangeItTo', '')} → {fix.get('expectedShift', '')}"
            ]
        existing.raw_json = data
        existing.created_at = datetime.now(timezone.utc)

    log.info("persisted v3 public perception for run %d", run.id)
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
