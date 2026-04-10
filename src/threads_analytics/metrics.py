"""The six scientific ground-truth metrics for a Threads account.

All metrics are computable from what we already ingest:
    my_posts · my_post_insights · my_account_insights

Each metric supports a `since`/`until` window, so the same code computes
both the current value and the baseline value (e.g. the 14 days before
the current 14-day window).

The metric names line up with the enum used in Experiment.primary_metric
and Experiment.secondary_metrics.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import MyAccountInsight, MyPost, MyPostInsight


METRIC_REACH_RATE = "reach_rate"
METRIC_REPLY_RATE_PER_VIEW = "reply_rate_per_view"
METRIC_REPLY_TO_LIKE_RATIO = "reply_to_like_ratio"
METRIC_ZERO_REPLY_FRACTION = "zero_reply_fraction"
METRIC_TOP_DECILE_MULTIPLE = "top_decile_reach_multiple"
METRIC_FOLLOWER_VELOCITY = "follower_velocity"

# The canonical ordering used by the Ground Truth dashboard.
METRIC_ORDER = [
    METRIC_REACH_RATE,
    METRIC_REPLY_RATE_PER_VIEW,
    METRIC_REPLY_TO_LIKE_RATIO,
    METRIC_ZERO_REPLY_FRACTION,
    METRIC_TOP_DECILE_MULTIPLE,
    METRIC_FOLLOWER_VELOCITY,
]

# Metadata shown on each metric card. "direction" = which way is improvement.
METRIC_META = {
    METRIC_REACH_RATE: {
        "label": "Reach rate",
        "description": "Median views per post ÷ follower count. The fraction of followers the algo is showing each post to.",
        "unit": "%",
        "direction": "up",  # higher is better
        "format": "pct",
    },
    METRIC_REPLY_RATE_PER_VIEW: {
        "label": "Reply rate per view",
        "description": "Total replies ÷ total views. The strongest conversation signal the algorithm scores you on.",
        "unit": "%",
        "direction": "up",
        "format": "pct",
    },
    METRIC_REPLY_TO_LIKE_RATIO: {
        "label": "Reply : like ratio",
        "description": "Replies ÷ likes. 18%+ is strong. The algo rewards conversation over passive approval.",
        "unit": "%",
        "direction": "up",
        "format": "pct",
    },
    METRIC_ZERO_REPLY_FRACTION: {
        "label": "Zero-reply posts",
        "description": "Share of posts that got zero replies. High = the algo is training a low-quality prior on your account.",
        "unit": "%",
        "direction": "down",  # lower is better
        "format": "pct",
    },
    METRIC_TOP_DECILE_MULTIPLE: {
        "label": "Top-decile reach multiple",
        "description": "Views at the 90th percentile ÷ median views. How far your breakouts travel above your baseline.",
        "unit": "×",
        "direction": "up",
        "format": "multiple",
    },
    METRIC_FOLLOWER_VELOCITY: {
        "label": "Follower velocity",
        "description": "Follower gain per day, 7-day rolling average. Net growth from the account snapshots.",
        "unit": "/day",
        "direction": "up",
        "format": "raw",
    },
}


@dataclass
class MetricValue:
    name: str
    value: float | None
    window_start: datetime | None
    window_end: datetime | None
    n_posts: int = 0
    detail: dict = field(default_factory=dict)


@dataclass
class MetricSeriesPoint:
    when: datetime
    value: float | None


@dataclass
class GroundTruthPanel:
    """What the Ground Truth overview renders."""

    computed_at: datetime
    window_days: int
    metrics: dict[str, MetricValue]
    baselines: dict[str, MetricValue]
    deltas: dict[str, float | None]  # relative change, None if either side missing
    verdict_headline: str
    trend: dict[str, list[MetricSeriesPoint]]  # per-metric 30-day series

    def to_dict(self) -> dict:
        def _mv(m: MetricValue) -> dict:
            return {
                "name": m.name,
                "value": m.value,
                "window_start": m.window_start.isoformat() if m.window_start else None,
                "window_end": m.window_end.isoformat() if m.window_end else None,
                "n_posts": m.n_posts,
                "detail": m.detail,
            }

        return {
            "computed_at": self.computed_at.isoformat(),
            "window_days": self.window_days,
            "metrics": {k: _mv(v) for k, v in self.metrics.items()},
            "baselines": {k: _mv(v) for k, v in self.baselines.items()},
            "deltas": self.deltas,
            "verdict_headline": self.verdict_headline,
            "trend": {
                k: [{"when": p.when.isoformat(), "value": p.value} for p in pts]
                for k, pts in self.trend.items()
            },
        }


# ---------- Core computations ----------


def _rows_for_window(
    session: Session, since: datetime, until: datetime
) -> list[dict]:
    """Return simplified post rows (with their latest insights) created in the window."""
    posts = session.scalars(
        select(MyPost).where(MyPost.created_at >= since, MyPost.created_at < until)
    ).all()
    if not posts:
        return []

    # Latest insight per post (by fetched_at desc)
    latest: dict[str, MyPostInsight] = {}
    all_ins = session.scalars(
        select(MyPostInsight).order_by(MyPostInsight.fetched_at.desc())
    ).all()
    for ins in all_ins:
        latest.setdefault(ins.thread_id, ins)

    rows = []
    for p in posts:
        ins = latest.get(p.thread_id)
        if ins is None:
            continue
        rows.append(
            {
                "thread_id": p.thread_id,
                "created_at": p.created_at,
                "views": ins.views,
                "likes": ins.likes,
                "replies": ins.replies,
                "reposts": ins.reposts,
                "quotes": ins.quotes,
            }
        )
    return rows


def _latest_follower_count(session: Session, at_or_before: datetime) -> int | None:
    snap = session.scalar(
        select(MyAccountInsight)
        .where(MyAccountInsight.fetched_at <= at_or_before)
        .order_by(MyAccountInsight.fetched_at.desc())
        .limit(1)
    )
    return snap.follower_count if snap else None


def compute_metric(
    session: Session,
    name: str,
    since: datetime,
    until: datetime,
) -> MetricValue:
    """Compute a single metric for an arbitrary window."""
    rows = _rows_for_window(session, since, until)

    if name == METRIC_REACH_RATE:
        followers = _latest_follower_count(session, until)
        if not rows or not followers or followers == 0:
            return MetricValue(name, None, since, until, len(rows))
        reach_rates = [r["views"] / followers for r in rows]
        return MetricValue(
            name,
            float(statistics.median(reach_rates)),
            since,
            until,
            len(rows),
            detail={"follower_count": followers, "median_views": statistics.median([r["views"] for r in rows])},
        )

    if name == METRIC_REPLY_RATE_PER_VIEW:
        total_views = sum(r["views"] for r in rows)
        total_replies = sum(r["replies"] for r in rows)
        if total_views == 0:
            return MetricValue(name, None, since, until, len(rows))
        return MetricValue(
            name,
            total_replies / total_views,
            since,
            until,
            len(rows),
            detail={"total_views": total_views, "total_replies": total_replies},
        )

    if name == METRIC_REPLY_TO_LIKE_RATIO:
        total_likes = sum(r["likes"] for r in rows)
        total_replies = sum(r["replies"] for r in rows)
        if total_likes == 0:
            return MetricValue(name, None, since, until, len(rows))
        return MetricValue(
            name,
            total_replies / total_likes,
            since,
            until,
            len(rows),
            detail={"total_likes": total_likes, "total_replies": total_replies},
        )

    if name == METRIC_ZERO_REPLY_FRACTION:
        if not rows:
            return MetricValue(name, None, since, until, 0)
        zeros = sum(1 for r in rows if r["replies"] == 0)
        return MetricValue(
            name,
            zeros / len(rows),
            since,
            until,
            len(rows),
            detail={"zero_count": zeros, "total": len(rows)},
        )

    if name == METRIC_TOP_DECILE_MULTIPLE:
        views = sorted(r["views"] for r in rows)
        if len(views) < 5:
            return MetricValue(name, None, since, until, len(views))
        median = statistics.median(views)
        if median == 0:
            return MetricValue(name, None, since, until, len(views))
        p90_idx = min(int(len(views) * 0.9), len(views) - 1)
        p90 = views[p90_idx]
        return MetricValue(
            name,
            p90 / median,
            since,
            until,
            len(views),
            detail={"p90": p90, "median": median},
        )

    if name == METRIC_FOLLOWER_VELOCITY:
        # 7-day rolling: (followers at `until`) - (followers 7 days before `until`) / 7
        end_count = _latest_follower_count(session, until)
        start_count = _latest_follower_count(session, until - timedelta(days=7))
        if end_count is None or start_count is None:
            return MetricValue(name, None, since, until, len(rows))
        return MetricValue(
            name,
            (end_count - start_count) / 7.0,
            since,
            until,
            len(rows),
            detail={"end": end_count, "start": start_count},
        )

    raise ValueError(f"unknown metric: {name}")


def compute_ground_truth(
    session: Session, window_days: int = 14
) -> GroundTruthPanel:
    """Compute the full Ground Truth panel: all six metrics + baselines + trend sparklines."""
    now = datetime.now(timezone.utc)
    current_start = now - timedelta(days=window_days)
    baseline_end = current_start
    baseline_start = baseline_end - timedelta(days=window_days)

    metrics: dict[str, MetricValue] = {}
    baselines: dict[str, MetricValue] = {}
    deltas: dict[str, float | None] = {}

    for name in METRIC_ORDER:
        current = compute_metric(session, name, current_start, now)
        base = compute_metric(session, name, baseline_start, baseline_end)
        metrics[name] = current
        baselines[name] = base
        deltas[name] = _relative_delta(base.value, current.value)

    # 30-day sparkline: 6 buckets of 5 days each
    trend: dict[str, list[MetricSeriesPoint]] = {name: [] for name in METRIC_ORDER}
    bucket_days = 5
    bucket_count = 6
    for i in range(bucket_count):
        end = now - timedelta(days=bucket_days * (bucket_count - 1 - i))
        start = end - timedelta(days=bucket_days)
        for name in METRIC_ORDER:
            mv = compute_metric(session, name, start, end)
            trend[name].append(MetricSeriesPoint(when=end, value=mv.value))

    verdict_headline = _build_verdict_headline(metrics, baselines, deltas)

    return GroundTruthPanel(
        computed_at=now,
        window_days=window_days,
        metrics=metrics,
        baselines=baselines,
        deltas=deltas,
        verdict_headline=verdict_headline,
        trend=trend,
    )


def _relative_delta(base: float | None, current: float | None) -> float | None:
    if base is None or current is None:
        return None
    if base == 0:
        return None
    return (current - base) / abs(base)


def _build_verdict_headline(
    metrics: dict[str, MetricValue],
    baselines: dict[str, MetricValue],
    deltas: dict[str, float | None],
) -> str:
    """One honest sentence summarizing the direction of travel."""
    improved = 0
    regressed = 0
    flat = 0
    for name in METRIC_ORDER:
        d = deltas.get(name)
        if d is None:
            continue
        direction = METRIC_META[name]["direction"]  # "up" = higher is better
        if abs(d) < 0.03:  # <3% relative change = noise
            flat += 1
            continue
        improving = (d > 0 and direction == "up") or (d < 0 and direction == "down")
        if improving:
            improved += 1
        else:
            regressed += 1

    total = improved + regressed + flat
    if total == 0:
        return "Not enough data yet — run the pipeline a few times over several days to populate a baseline."

    # Pick the most dramatic regression to name-check, if any.
    worst: tuple[str, float] | None = None
    for name in METRIC_ORDER:
        d = deltas.get(name)
        if d is None:
            continue
        direction = METRIC_META[name]["direction"]
        is_bad = (d < 0 and direction == "up") or (d > 0 and direction == "down")
        if is_bad:
            if worst is None or abs(d) > abs(worst[1]):
                worst = (name, d)

    if regressed == 0 and improved > 0:
        return f"Moving in the right direction: {improved} of {total} signals improved, none regressed."
    if improved == 0 and regressed > 0:
        name, d = worst  # type: ignore[misc]
        meta = METRIC_META[name]
        return f"Moving the wrong way on {regressed} of {total} signals. Biggest drop: {meta['label']} moved {d:+.0%}."
    if worst is not None:
        name, d = worst
        meta = METRIC_META[name]
        return (
            f"Mixed: {improved} improved, {regressed} regressed, {flat} flat. "
            f"Biggest drop: {meta['label']} {d:+.0%}."
        )
    return f"Mixed results: {improved} improved, {flat} flat."
