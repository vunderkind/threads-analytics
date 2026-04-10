"""Algorithm Inference v2 — research-grounded.

Uses the documented Threads/X/Bluesky ranking signals as the frame:
- Reply velocity (first 30-60 min) — the dominant distribution driver
- Conversation depth — replies >> likes by ~27x on X's heavy ranker
- Self-reply behavior — author-reply-to-reply carries +75 weight on X
- Zero-reply penalty — the quality-prior suppression loop
- Format diversity — video/image dwell time bonus
- Posting cadence — dilution vs reliability tradeoff

Each signal gets a rating (penalized / neutral / boosted), a piece of cited
numerical evidence from the user's data, and a predicted impact. Plus a single
"highest ROI lever" that directly maps to the research.
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime, timezone

from anthropic import Anthropic
from sqlalchemy import desc, func, select

from .config import get_settings
from .db import session_scope
from .models import (
    AlgorithmInference,
    MyAccountInsight,
    MyPost,
    MyPostInsight,
    Profile,
    Run,
)

log = logging.getLogger(__name__)


RESEARCH_CONTEXT = """
EPISTEMIC STANDARDS — the whole response must follow these.

Threads's ranking system is NOT open-sourced. You cannot know its exact internals.
You can ONLY reason from:

(A) DOCUMENTED FACTS — may be cited with source:
  - X (Twitter) open-sourced its "heavy ranker" weights (2023): reply with author
    follow-up that user re-engages with = +75; plain reply = +13.5; like = +0.5;
    retweet = +1.0. Source: twitter/the-algorithm on GitHub. This means on X,
    replies ≈ 27× likes, conversation chains ≈ 150× likes.
  - Meta has publicly stated that replies are the strongest signal on Threads and
    that conversation-driving content is favored. Source: Meta / Threads blog posts.
  - Bluesky's Discover documents that first-hour engagement drives surfacing.

(B) PLAUSIBLE INFERENCES — may be stated, but MUST hedge:
  - Threads likely uses reply-weighting architecturally similar to X (shared Meta
    heritage, similar UX primitives), but the actual multipliers are unpublished.
  - A "zero-reply penalty loop" is a plausible account-level dynamic consistent
    with quality-signal ranking systems, but Meta has not documented it as a
    specific mechanism.
  - "First 30-60 min engagement velocity" is X-documented; the exact Threads
    window is inferred from Meta's public statements.

(C) SPECIFIC-ACCOUNT CLAIMS — always hypotheses:
  - You cannot directly observe how the Threads ranker is scoring this account.
    You can only observe outputs (views, likes, replies) and offer plausible
    mechanisms consistent with (A) and (B).

HARD RULES:
1. Never assert a specific numeric weight about Threads. Only cite X numbers with
   "on X's open-source ranker" as the attribution.
2. Never say "the algorithm is penalizing you" — say "consistent with a penalty
   loop" or "the pattern is consistent with the ranker assigning a lower quality
   prior". Always hedge.
3. In the evidence field, cite actual numbers from the user's data. Those ARE facts.
4. In the inferredImpact field, hedge about what WOULD happen — use "likely",
   "plausibly", "would be consistent with".
5. If a signal is neutral or the data doesn't support a claim, say "neutral" and
   explain that the data doesn't show enough to classify it. Do not fabricate.
"""


SYSTEM = (
    "You are a rigorous social media ranking analyst. You are given statistical "
    "evidence about a single Threads account and must produce a signal-by-signal "
    "diagnosis that NEVER overreaches beyond what the evidence supports. Your "
    "standard is scientific rigor, not persuasive certainty. If you do not know, "
    "say so. Every claim about the Threads ranker (as opposed to X, which is "
    "open-sourced) must be hedged. Cite numbers from the user's data as facts; "
    "cite X open-source weights as facts (with the source); everything else is "
    "an inference and must be labeled as such.\n\n"
    f"{RESEARCH_CONTEXT}"
)


SCHEMA_INSTRUCTION = (
    "Respond with ONLY a JSON object, no prose, no fences. Shape:\n"
    "{\n"
    '  "narrativeDiagnosis": "3-5 sentences telling the story of how this account is being ranked right now. Mention the penalty loop if it applies.",\n'
    '  "replyVelocitySignal":      {"rating": "penalized|neutral|boosted", "evidence": "specific numbers", "inferredImpact": "1-2 sentences"},\n'
    '  "conversationDepthSignal":  {"rating": "penalized|neutral|boosted", "evidence": "...", "inferredImpact": "..."},\n'
    '  "selfReplySignal":          {"rating": "penalized|neutral|boosted", "evidence": "...", "inferredImpact": "..."},\n'
    '  "zeroReplyPenaltySignal":   {"rating": "penalized|neutral|boosted", "evidence": "...", "inferredImpact": "..."},\n'
    '  "formatDiversitySignal":    {"rating": "penalized|neutral|boosted", "evidence": "...", "inferredImpact": "..."},\n'
    '  "postingCadenceSignal":     {"rating": "penalized|neutral|boosted", "evidence": "...", "inferredImpact": "..."},\n'
    '  "inferredSignalWeights": {\n'
    '    "reply_velocity": 0.0-1.0,\n'
    '    "conversation_depth": 0.0-1.0,\n'
    '    "self_reply": 0.0-1.0,\n'
    '    "zero_reply_penalty": 0.0-1.0,\n'
    '    "format_diversity": 0.0-1.0,\n'
    '    "posting_cadence": 0.0-1.0\n'
    "  },\n"
    '  "highestRoiLever": {\n'
    '    "title": "<=12 words imperative",\n'
    '    "mechanism": "which research-cited signal this moves and why",\n'
    '    "expectedImpact": "what you\'d expect to see in 30 days",\n'
    '    "citesResearch": "specific citation like \'X heavy ranker +75 weight on author-reply-to-reply\'"\n'
    "  }\n"
    "}"
)


def generate_algorithm_inference(run: Run) -> int | None:
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

        acc = session.scalar(
            select(MyAccountInsight).order_by(desc(MyAccountInsight.fetched_at)).limit(1)
        )
        account_payload = None
        if acc is not None:
            account_payload = {
                "follower_count": acc.follower_count,
                "views_30d": acc.views,
                "likes_30d": acc.likes,
                "replies_30d": acc.replies,
                "reposts_30d": acc.reposts,
                "quotes_30d": acc.quotes,
                "reply_to_like_ratio_30d": (
                    round(acc.replies / acc.likes, 4) if acc.likes else None
                ),
            }

        posts = session.scalars(
            select(MyPost).order_by(desc(MyPost.created_at)).limit(100)
        ).all()
        latest: dict[str, MyPostInsight] = {}
        for ins in session.scalars(
            select(MyPostInsight).order_by(desc(MyPostInsight.fetched_at))
        ).all():
            latest.setdefault(ins.thread_id, ins)

        rows = []
        for p in posts:
            ins = latest.get(p.thread_id)
            if ins is None:
                continue
            rows.append(
                {
                    "hour": p.created_at.hour if p.created_at else None,
                    "length": len(p.text or ""),
                    "media_type": p.media_type or "TEXT_POST",
                    "views": ins.views,
                    "likes": ins.likes,
                    "replies": ins.replies,
                    "reposts": ins.reposts,
                }
            )

        total_posts = session.scalar(select(func.count()).select_from(MyPost)) or 0

    if not rows:
        return None

    likes = [r["likes"] for r in rows]
    replies = [r["replies"] for r in rows]
    views = [r["views"] for r in rows]
    stats = {
        "posts_analyzed": len(rows),
        "total_posts_in_db": total_posts,
        "median_likes": statistics.median(likes),
        "mean_likes": statistics.fmean(likes),
        "max_likes": max(likes),
        "median_replies": statistics.median(replies),
        "mean_replies": statistics.fmean(replies),
        "max_replies": max(replies),
        "zero_reply_fraction": sum(1 for x in replies if x == 0) / len(replies),
        "zero_like_fraction": sum(1 for x in likes if x == 0) / len(likes),
        "median_views": statistics.median(views),
        "reply_to_like_post_ratio": (
            statistics.fmean(replies) / statistics.fmean(likes) if statistics.fmean(likes) else None
        ),
        "reach_rate_percent": (
            round(statistics.median(views) / account_payload["follower_count"] * 100, 2)
            if account_payload and account_payload["follower_count"]
            else None
        ),
        "media_distribution": _distribution([r["media_type"] for r in rows]),
        "hour_distribution": _distribution([r["hour"] for r in rows if r["hour"] is not None]),
    }

    text_body = (
        f"{SCHEMA_INSTRUCTION}\n\n"
        "DATA:\n"
        + json.dumps(
            {
                "profile": profile_payload,
                "account_30d": account_payload,
                "post_stats": stats,
                "sample_rows_head": rows[:30],
            },
            ensure_ascii=False,
            indent=2,
            default=str,
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
        log.warning("algorithm inference v2 produced no parseable JSON: %s", text[:400])
        return None

    with session_scope() as session:
        existing = session.get(AlgorithmInference, run.id)
        if existing is None:
            existing = AlgorithmInference(run_id=run.id)
            session.add(existing)
        existing.narrative_diagnosis = data.get("narrativeDiagnosis", "")
        existing.reply_velocity_signal = data.get("replyVelocitySignal")
        existing.conversation_depth_signal = data.get("conversationDepthSignal")
        existing.self_reply_signal = data.get("selfReplySignal")
        existing.zero_reply_penalty_signal = data.get("zeroReplyPenaltySignal")
        existing.format_diversity_signal = data.get("formatDiversitySignal")
        existing.posting_cadence_signal = data.get("postingCadenceSignal")
        existing.inferred_signal_weights = data.get("inferredSignalWeights")
        existing.highest_roi_lever = data.get("highestRoiLever")
        existing.raw_json = data
        existing.created_at = datetime.now(timezone.utc)
        # Keep legacy v1 fields clear
        existing.summary = data.get("narrativeDiagnosis", "")

    log.info("persisted algorithm inference v2 for run %d", run.id)
    return run.id


def _distribution(items: list) -> dict:
    out: dict = {}
    for item in items:
        out[item] = out.get(item, 0) + 1
    return out


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
