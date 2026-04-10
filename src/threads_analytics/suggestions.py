"""Claude-powered experiment suggestion engine.

Replaces the v1 recommender. Takes the full picture of the account — current
ground-truth metrics, baseline deltas, perception, algorithm inference, topics,
and the user's personal experiment track record — and asks Claude Opus to
propose 5–8 concrete experiment specs that are directly persistable as
Experiment rows with status='proposed'.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from anthropic import Anthropic
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from .config import get_settings
from .experiments import create_experiment, personal_category_performance
from .metrics import (
    METRIC_META,
    METRIC_ORDER,
    GroundTruthPanel,
    compute_ground_truth,
)
from .models import (
    AlgorithmInference,
    Experiment,
    PublicPerception,
    Topic,
    YouProfile,
)
from .predicates import PER_POST_CATEGORIES, PER_WINDOW_CATEGORIES

log = logging.getLogger(__name__)


VALID_CATEGORIES = sorted(PER_POST_CATEGORIES | PER_WINDOW_CATEGORIES)


SYSTEM = (
    "You are a rigorous growth experimentation coach for a single Threads account. "
    "Your job is to propose 5-8 scientific experiments that the user can run.\n\n"
    "EPISTEMIC STANDARD: Every experiment must be framed as a HYPOTHESIS TO TEST, "
    "never a prescription. 'Posting between 7-9pm will improve reach rate' is a "
    "hypothesis — phrased as one. 'This will work' is an assertion — forbidden. "
    "Use language like 'if X, then we expect Y' or 'we hypothesize that'. The "
    "whole point of the system is that the user runs the experiment and the "
    "verdict engine says whether the hypothesis held.\n\n"
    "You receive: the user's current scientific metrics with baselines and deltas, "
    "the honest outsider view of their profile, Claude's inference of how the "
    "algorithm sees them, their extracted topics, their personal experiment track "
    "record, AND their 'You' profile. Every proposal must be: (a) a concrete "
    "intervention the user can unambiguously execute, (b) tied to a specific "
    "currently-underperforming metric, (c) testable within 7-14 days, "
    "(d) falsifiable, (e) RESPECTFUL of the protect list in the You profile, "
    "(f) framed as a hypothesis, not a prescription. Weight proposals toward "
    "categories where the user has historically won. Do not propose experiments "
    "in categories they have repeatedly lost. When you cite a mechanism, distinguish "
    "X's open-sourced ranker weights (facts) from Threads-specific claims "
    "(inferences — must hedge)."
)


SCHEMA_INSTRUCTION = (
    "Respond with ONLY a JSON object, no prose, no code fences. Shape:\n"
    "{\n"
    '  "experiments": [\n'
    "    {\n"
    '      "title": "imperative, <=80 chars",\n'
    '      "hypothesis": "1-2 sentences stating the expected effect with a direction and magnitude",\n'
    '      "category": "one of TIMING | LENGTH | MEDIA | HOOK | TOPIC | CADENCE | ENGAGEMENT | CUSTOM",\n'
    '      "predicate_spec": "JSON object matching the category predicate shape (see examples)",\n'
    '      "primary_metric": "one of reach_rate | reply_rate_per_view | reply_to_like_ratio | zero_reply_fraction | top_decile_reach_multiple | follower_velocity",\n'
    '      "target_delta_pct": 0.25,\n'
    '      "variant_window_days": 14,\n'
    '      "reasoning": "1-2 sentences linking this to the specific metric weakness or track-record insight"\n'
    "    }\n"
    "  ]\n"
    "}\n\n"
    "predicate_spec shapes by category:\n"
    "  TIMING: {\"hours\": [19, 20, 21]}  (hours in UTC)\n"
    "  LENGTH: {\"min_len\": 80, \"max_len\": 200}\n"
    "  MEDIA:  {\"media_types\": [\"IMAGE\", \"CAROUSEL_ALBUM\"]}\n"
    "  HOOK:   {\"prefixes\": [\"you vs me\", \"hot take:\", \"nobody talks about\"]}\n"
    "  TOPIC:  {\"topic_label\": \"<exact label from the topics list provided>\"}\n"
    "  CADENCE: {\"min\": 1, \"max\": 2}  (posts per day)\n"
    "  ENGAGEMENT: {\"behavior\": \"reply_to_own_commenters\", \"target_response_minutes\": 10}\n"
    "  CUSTOM: {\"instruction\": \"plain-English instruction for the user to tag posts manually\"}\n"
)


def generate_suggestions(session: Session, n: int = 6) -> list[int]:
    """Call Claude Opus to generate experiment proposals and persist as rows.

    Returns the list of newly created Experiment IDs (all with status='proposed')."""
    settings = get_settings()

    panel = compute_ground_truth(session)
    perception = session.scalar(
        select(PublicPerception).order_by(desc(PublicPerception.created_at)).limit(1)
    )
    algo = session.scalar(
        select(AlgorithmInference).order_by(desc(AlgorithmInference.created_at)).limit(1)
    )
    topics = session.scalars(select(Topic)).all()
    track_record = personal_category_performance(session)
    you_profile = session.scalar(
        select(YouProfile).order_by(desc(YouProfile.created_at)).limit(1)
    )

    # Also fetch existing proposals the user hasn't run, so Claude can avoid duplicates
    open_proposals = session.scalars(
        select(Experiment).where(Experiment.status == "proposed").limit(20)
    ).all()
    open_titles = [e.title for e in open_proposals]

    metrics_payload = {
        name: {
            "label": METRIC_META[name]["label"],
            "direction": METRIC_META[name]["direction"],
            "current": panel.metrics[name].value,
            "baseline": panel.baselines[name].value,
            "delta_pct": panel.deltas[name],
        }
        for name in METRIC_ORDER
    }

    data = {
        "current_metrics": metrics_payload,
        "verdict_headline": panel.verdict_headline,
        "topics": [{"label": t.label, "description": t.description} for t in topics],
        "perception": {
            "first_glance": perception.first_glance if perception else None,
            "overall": perception.overall_impression if perception else None,
            "who_will_dislike": perception.who_will_dislike if perception else None,
        }
        if perception
        else None,
        "algorithm_inference": {
            "summary": algo.summary if algo else None,
            "top_penalties": [p.get("title") for p in (algo.penalties or [])][:5] if algo else [],
            "top_boosts": [b.get("title") for b in (algo.boosts or [])][:3] if algo else [],
        }
        if algo
        else None,
        "track_record": {
            cat: {
                "total": cs.total,
                "wins": cs.wins,
                "losses": cs.losses,
                "nulls": cs.nulls,
                "insufficient": cs.insufficient,
                "win_rate": cs.win_rate(),
                "avg_win_effect_pct": cs.avg_win_effect_pct,
            }
            for cat, cs in track_record.items()
        },
        "open_proposal_titles_to_avoid_duplicating": open_titles,
        "you_profile": (
            {
                "core_identity": you_profile.core_identity,
                "protect_list": you_profile.protect_list,
                "double_down_list": you_profile.double_down_list,
                "homogenization_risks": you_profile.homogenization_risks,
            }
            if you_profile
            else None
        ),
    }

    user_msg = (
        f"Propose {n} experiments for this user.\n\n"
        f"{SCHEMA_INSTRUCTION}\n\n"
        f"DATA:\n{json.dumps(data, default=str, ensure_ascii=False, indent=2)}"
    )

    client = Anthropic(api_key=settings.anthropic_api_key)
    resp = client.messages.create(
        model=settings.claude_recommender_model,
        max_tokens=4096,
        system=SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    parsed = _safe_json(text)
    if not parsed or "experiments" not in parsed:
        log.warning("suggestions produced no parseable JSON: %s", text[:400])
        return []

    new_ids: list[int] = []
    for item in parsed["experiments"]:
        try:
            title = (item.get("title") or "").strip()[:256]
            if not title:
                continue
            category = (item.get("category") or "").strip().upper()
            if category not in VALID_CATEGORIES:
                log.warning("skipping proposal with bad category: %r", category)
                continue
            metric = item.get("primary_metric")
            if metric not in METRIC_ORDER:
                log.warning("skipping proposal with bad metric: %r", metric)
                continue
            exp = create_experiment(
                session,
                title=title,
                hypothesis=item.get("hypothesis", ""),
                category=category,
                predicate_spec=item.get("predicate_spec") or {},
                primary_metric=metric,
                source="suggested_by_claude",
                target_delta_pct=_maybe_float(item.get("target_delta_pct")),
                variant_window_days=int(item.get("variant_window_days", 14) or 14),
                status="proposed",
                notes=item.get("reasoning"),
            )
            new_ids.append(exp.id)
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to persist suggestion: %s", exc)

    log.info("generated %d experiment proposals", len(new_ids))
    return new_ids


def _maybe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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
