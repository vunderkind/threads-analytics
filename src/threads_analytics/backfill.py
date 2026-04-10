"""Backfill historical Ground Truth snapshots from existing post data.

The normal pipeline produces ONE account-insight snapshot per run, keyed to
'now'. That means the Ground Truth sparklines have only a handful of points
and no real history. But we DO have hundreds of posts with `created_at`
timestamps, so we can reconstruct what the metrics WOULD HAVE BEEN at any
point in the last ~N days, using only the posts published in the window up
to that date.

This module creates one synthetic Run + MyAccountInsight per historical bucket,
so the next render of `/` shows a real trend line.

Caveats:
 - Follower count is not historically tracked by us — we use the current
   follower count for every backfilled snapshot. Reach rate and follower
   velocity are therefore not historically accurate during backfill, but
   reply-rate / reply-to-like / zero-reply / top-decile ARE accurate because
   they only use post-level data.
 - Backfilled runs are tagged status='backfilled' in their notes so they can
   be identified and deleted if needed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from .db import init_db, session_scope
from .metrics import (
    METRIC_FOLLOWER_VELOCITY,
    METRIC_ORDER,
    compute_metric,
)
from .models import MyAccountInsight, MyPost, MyPostInsight, Run

log = logging.getLogger(__name__)


def backfill_history(
    bucket_days: int = 1,
    max_days_back: int = 180,
    window_days: int = 14,
) -> dict:
    """Walk backwards in time and create a snapshot per bucket.

    For each historical "now" at T, we compute the same six Ground Truth
    metrics over the window [T - window_days, T] and write a synthetic
    account-insight row. The follower count is copied from the most recent
    real snapshot (it's the only one we have).
    """
    init_db()

    with session_scope() as session:
        # 1. Find the oldest post we have — that bounds how far back we can go.
        oldest = session.scalar(
            select(MyPost).order_by(MyPost.created_at.asc()).limit(1)
        )
        if oldest is None:
            return {"backfilled_runs": 0, "reason": "no posts in db"}
        oldest_post_date = oldest.created_at.replace(tzinfo=timezone.utc) if oldest.created_at.tzinfo is None else oldest.created_at

        # 2. Get the most-recent real follower count to carry through.
        real_acc = session.scalar(
            select(MyAccountInsight).order_by(MyAccountInsight.fetched_at.desc()).limit(1)
        )
        if real_acc is None:
            return {"backfilled_runs": 0, "reason": "no account insight to anchor follower count"}
        current_followers = real_acc.follower_count

        now = datetime.now(timezone.utc)
        # Lower bound: the oldest post we have (naive-aware safe) OR the cap
        oldest_naive = oldest.created_at
        if oldest_naive.tzinfo is None:
            oldest_naive = oldest_naive.replace(tzinfo=timezone.utc)
        earliest_bucket = max(
            oldest_naive,  # can't compute metrics on a window with zero posts
            now - timedelta(days=max_days_back),
        )
        bucket_ends: list[datetime] = []
        t = now - timedelta(days=bucket_days)  # skip "now" — the real pipeline handles today
        while t > earliest_bucket:
            bucket_ends.append(t)
            t -= timedelta(days=bucket_days)
        bucket_ends.reverse()  # oldest first

    created_run_ids: list[int] = []
    skipped_existing = 0

    for bucket_end in bucket_ends:
        bucket_start = bucket_end - timedelta(days=window_days)
        with session_scope() as session:
            # Skip if we already have a backfilled run at this bucket_end (idempotent)
            existing = session.scalar(
                select(Run).where(
                    Run.status == "backfilled",
                    Run.started_at >= bucket_end - timedelta(hours=6),
                    Run.started_at <= bucket_end + timedelta(hours=6),
                )
            )
            if existing is not None:
                skipped_existing += 1
                continue

            run = Run(
                started_at=bucket_end,
                finished_at=bucket_end,
                status="backfilled",
                notes=f"backfill bucket [{bucket_start.date()}, {bucket_end.date()}]",
            )
            session.add(run)
            session.flush()
            run_id = run.id

            # Compute metrics against this historical window.
            metric_values: dict[str, float | None] = {}
            for name in METRIC_ORDER:
                if name == METRIC_FOLLOWER_VELOCITY:
                    # We don't have historical follower counts — skip
                    metric_values[name] = None
                    continue
                mv = compute_metric(session, name, bucket_start, bucket_end)
                metric_values[name] = mv.value

            # Compute aggregate window engagement for the account insight row
            rows = session.scalars(
                select(MyPost).where(
                    MyPost.created_at >= bucket_start,
                    MyPost.created_at < bucket_end,
                )
            ).all()
            post_ids = [p.thread_id for p in rows]
            if post_ids:
                latest_ins_map: dict[str, MyPostInsight] = {}
                for ins in session.scalars(
                    select(MyPostInsight)
                    .where(MyPostInsight.thread_id.in_(post_ids))
                    .order_by(MyPostInsight.fetched_at.desc())
                ).all():
                    latest_ins_map.setdefault(ins.thread_id, ins)
                likes_sum = sum(ins.likes for ins in latest_ins_map.values())
                replies_sum = sum(ins.replies for ins in latest_ins_map.values())
                views_sum = sum(ins.views for ins in latest_ins_map.values())
                reposts_sum = sum(ins.reposts for ins in latest_ins_map.values())
                quotes_sum = sum(ins.quotes for ins in latest_ins_map.values())
            else:
                likes_sum = replies_sum = views_sum = reposts_sum = quotes_sum = 0

            session.add(
                MyAccountInsight(
                    run_id=run_id,
                    follower_count=current_followers,  # best-effort: carry current
                    views=views_sum,
                    likes=likes_sum,
                    replies=replies_sum,
                    reposts=reposts_sum,
                    quotes=quotes_sum,
                    demographics_json=None,
                    fetched_at=bucket_end,
                )
            )
            created_run_ids.append(run_id)

    return {
        "backfilled_runs": len(created_run_ids),
        "skipped_existing": skipped_existing,
        "bucket_days": bucket_days,
        "max_days_back": max_days_back,
        "window_days": window_days,
        "run_ids": created_run_ids,
    }
