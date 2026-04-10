"""Measure how prior recommendations correlated with follower / engagement changes.

For each non-dismissed recommendation from earlier runs, compare the account
snapshot at the run that issued it to the current run, and persist the delta
into recommendation_outcomes. These outcomes are fed back into the next
recommender pass so the service "learns from experience."
"""

from __future__ import annotations

import logging

from sqlalchemy import select

from .db import session_scope
from .models import MyAccountInsight, Recommendation, RecommendationOutcome, Run

log = logging.getLogger(__name__)


def _engagement_total(snap: MyAccountInsight | None) -> float:
    if snap is None:
        return 0.0
    return float(
        (snap.likes or 0)
        + (snap.replies or 0)
        + (snap.reposts or 0)
        + (snap.quotes or 0)
    )


def measure_outcomes(current_run: Run) -> int:
    """Score every pending/applied recommendation from prior runs against the current snapshot.

    Returns the number of outcome rows written.
    """
    with session_scope() as session:
        current_snap = session.get(MyAccountInsight, current_run.id)
        if current_snap is None:
            log.info("no account insight snapshot on current run; skipping outcome measurement")
            return 0

        prior_recs = session.scalars(
            select(Recommendation).where(
                (Recommendation.run_id != current_run.id)
                & (Recommendation.status != "dismissed")
            )
        ).all()

        written = 0
        for rec in prior_recs:
            # Already measured against this run?
            already = session.scalar(
                select(RecommendationOutcome).where(
                    (RecommendationOutcome.recommendation_id == rec.id)
                    & (RecommendationOutcome.checked_at_run_id == current_run.id)
                )
            )
            if already is not None:
                continue

            origin_snap = session.get(MyAccountInsight, rec.run_id)
            if origin_snap is None:
                continue

            follower_delta = (current_snap.follower_count or 0) - (
                origin_snap.follower_count or 0
            )
            engagement_delta = _engagement_total(current_snap) - _engagement_total(origin_snap)

            session.add(
                RecommendationOutcome(
                    recommendation_id=rec.id,
                    checked_at_run_id=current_run.id,
                    follower_delta=follower_delta,
                    engagement_delta=engagement_delta,
                    notes=None,
                )
            )
            written += 1

    log.info("wrote %d recommendation outcomes", written)
    return written
