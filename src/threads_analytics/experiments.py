"""Experiment CRUD and lifecycle operations.

proposed → active → completed | abandoned

Also exposes the "personal track record" aggregation used by the suggestion
engine and the Experiments dashboard page.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from .metrics import METRIC_META, METRIC_ORDER
from .models import (
    Experiment,
    ExperimentPostClassification,
    ExperimentVerdict,
)
from .predicates import PER_POST_CATEGORIES, PER_WINDOW_CATEGORIES
from .verdict import evaluate_experiment, persist_verdict

log = logging.getLogger(__name__)

DEFAULT_BASELINE_WINDOW_DAYS = 14
DEFAULT_VARIANT_WINDOW_DAYS = 14


@dataclass
class CategoryStats:
    category: str
    total: int = 0
    wins: int = 0
    losses: int = 0
    nulls: int = 0
    insufficient: int = 0
    avg_win_effect_pct: float | None = None
    last_verdict: str | None = None

    def win_rate(self) -> float | None:
        decided = self.wins + self.losses
        if decided == 0:
            return None
        return self.wins / decided


# ---------- creation ----------


def create_experiment(
    session: Session,
    *,
    title: str,
    hypothesis: str,
    category: str,
    predicate_spec: dict[str, Any] | None,
    primary_metric: str,
    source: str = "user_defined",
    target_delta_pct: float | None = None,
    variant_window_days: int = DEFAULT_VARIANT_WINDOW_DAYS,
    baseline_window_days: int = DEFAULT_BASELINE_WINDOW_DAYS,
    status: str = "proposed",
    notes: str | None = None,
) -> Experiment:
    if category not in PER_POST_CATEGORIES and category not in PER_WINDOW_CATEGORIES:
        raise ValueError(f"unknown category: {category}")
    if primary_metric not in METRIC_ORDER:
        raise ValueError(f"unknown metric: {primary_metric}")

    exp = Experiment(
        title=title,
        hypothesis=hypothesis,
        category=category,
        predicate_spec=predicate_spec or {},
        primary_metric=primary_metric,
        target_delta_pct=target_delta_pct,
        status=status,
        source=source,
        notes=notes,
        created_at=datetime.now(timezone.utc),
    )
    # Optimistically pre-set the intended window shapes in metadata even though
    # the actual start/end don't snap until the experiment is started.
    exp.secondary_metrics = {
        "planned_baseline_window_days": baseline_window_days,
        "planned_variant_window_days": variant_window_days,
    }
    session.add(exp)
    session.flush()
    return exp


def start_experiment(session: Session, exp: Experiment) -> Experiment:
    """Transition a proposed experiment to active, snapping baseline/variant windows."""
    if exp.status != "proposed":
        return exp
    now = datetime.now(timezone.utc)
    variant_days = (exp.secondary_metrics or {}).get(
        "planned_variant_window_days", DEFAULT_VARIANT_WINDOW_DAYS
    )
    baseline_days = (exp.secondary_metrics or {}).get(
        "planned_baseline_window_days", DEFAULT_BASELINE_WINDOW_DAYS
    )
    exp.baseline_end = now
    exp.baseline_start = now - timedelta(days=baseline_days)
    exp.variant_start = now
    exp.variant_end = now + timedelta(days=variant_days)
    exp.status = "active"
    exp.started_at = now
    return exp


def end_experiment(
    session: Session,
    exp: Experiment,
    final_status: str = "completed",
) -> Experiment:
    """Mark an experiment finished, compute the verdict, and persist it."""
    now = datetime.now(timezone.utc)
    if exp.status == "active":
        if exp.variant_end is None or exp.variant_end > now:
            exp.variant_end = now
    exp.ended_at = now
    exp.status = final_status

    if final_status == "completed":
        result = evaluate_experiment(session, exp)
        persist_verdict(session, exp, result)
    return exp


def evaluate_now(session: Session, exp: Experiment) -> None:
    """Run the verdict engine against the current data without ending the experiment."""
    result = evaluate_experiment(session, exp)
    persist_verdict(session, exp, result)


def abandon_experiment(session: Session, exp: Experiment) -> Experiment:
    exp.status = "abandoned"
    exp.ended_at = datetime.now(timezone.utc)
    return exp


# ---------- background maintenance ----------


def auto_evaluate_due(session: Session) -> list[int]:
    """Find active experiments whose variant_end has passed and evaluate them.

    Returns the list of experiment IDs that transitioned to completed.
    """
    now = datetime.now(timezone.utc)
    due = session.scalars(
        select(Experiment).where(
            Experiment.status == "active",
            Experiment.variant_end.is_not(None),
            Experiment.variant_end <= now,
        )
    ).all()
    ids: list[int] = []
    for exp in due:
        try:
            end_experiment(session, exp, final_status="completed")
            ids.append(exp.id)
        except Exception as exc:  # noqa: BLE001
            log.warning("auto-evaluate failed for experiment %d: %s", exp.id, exc)
    return ids


def classify_active_experiments(session: Session) -> int:
    """For every active experiment, refresh the per-post classifications so the
    dashboard always shows the latest variant/control counts."""
    active = session.scalars(
        select(Experiment).where(Experiment.status == "active")
    ).all()
    total = 0
    for exp in active:
        try:
            result = evaluate_experiment(session, exp)
            persist_verdict(session, exp, result)
            total += result.variant_n + result.control_n
        except Exception as exc:  # noqa: BLE001
            log.warning("classify failed for experiment %d: %s", exp.id, exc)
    return total


# ---------- reads ----------


def list_experiments(
    session: Session, status: str | None = None, limit: int = 100
) -> list[Experiment]:
    q = select(Experiment).order_by(desc(Experiment.created_at)).limit(limit)
    if status:
        q = q.where(Experiment.status == status)
    return list(session.scalars(q).all())


def personal_category_performance(session: Session) -> dict[str, CategoryStats]:
    """Aggregate verdict history per category for the track record widget."""
    exps = session.scalars(
        select(Experiment).where(Experiment.status.in_(["completed", "abandoned"]))
    ).all()
    out: dict[str, CategoryStats] = {}
    for exp in exps:
        bucket = out.setdefault(exp.category, CategoryStats(category=exp.category))
        bucket.total += 1
        v = session.get(ExperimentVerdict, exp.id)
        if v is None:
            bucket.insufficient += 1
            continue
        bucket.last_verdict = v.verdict
        if v.verdict == "win":
            bucket.wins += 1
            if v.effect_size_pct is not None:
                # running average
                prior = bucket.avg_win_effect_pct or 0
                bucket.avg_win_effect_pct = (
                    prior + (v.effect_size_pct - prior) / bucket.wins
                )
        elif v.verdict == "loss":
            bucket.losses += 1
        elif v.verdict == "null":
            bucket.nulls += 1
        else:
            bucket.insufficient += 1
    return out
