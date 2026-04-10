"""Pattern analysis — pure stats, no LLM calls.

Produces a `PatternReport` that captures when you post, what length works, which
media types perform, and what hook phrases show up in your top-decile posts. The
recommender consumes this report directly.
"""

from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Iterable

from sqlalchemy import select

from .db import session_scope
from .models import AffinityCreator, AffinityPost, MyPost, MyPostInsight


@dataclass
class PatternReport:
    scope: str  # "me" or "affinity_top20"
    post_count: int = 0
    median_likes: float = 0.0
    median_views: float = 0.0
    median_replies: float = 0.0
    best_hours_local: list[int] = field(default_factory=list)
    hour_distribution: dict[int, int] = field(default_factory=dict)
    length_buckets: dict[str, dict[str, float]] = field(default_factory=dict)
    media_type_performance: dict[str, dict[str, float]] = field(default_factory=dict)
    top_hook_phrases: list[str] = field(default_factory=list)
    top_posts_sample: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _length_bucket(n: int) -> str:
    if n < 80:
        return "short (<80)"
    if n < 200:
        return "medium (80-200)"
    if n < 400:
        return "long (200-400)"
    return "very_long (400+)"


def _hour(dt: datetime) -> int:
    return dt.hour


def _top_decile(values: list[float]) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(len(values) * 0.9)
    idx = min(idx, len(values) - 1)
    return values[idx]


def _extract_hooks(texts: Iterable[str], top_k: int = 5) -> list[str]:
    """Return the most common first-3-word phrases from the given texts."""
    counter: Counter[str] = Counter()
    for t in texts:
        words = (t or "").strip().split()
        if len(words) >= 3:
            phrase = " ".join(words[:3]).lower().strip(",.!?:;")
            counter[phrase] += 1
    return [p for p, _ in counter.most_common(top_k)]


def analyze_my_patterns() -> PatternReport:
    """Compute a PatternReport for the user's own posts, using latest insight snapshot."""
    with session_scope() as session:
        posts = session.scalars(select(MyPost)).all()
        if not posts:
            return PatternReport(scope="me")

        # Most-recent insight per post
        latest_by_post: dict[str, MyPostInsight] = {}
        all_insights = session.scalars(
            select(MyPostInsight).order_by(MyPostInsight.fetched_at.desc())
        ).all()
        for ins in all_insights:
            latest_by_post.setdefault(ins.thread_id, ins)

        rows = []
        for p in posts:
            ins = latest_by_post.get(p.thread_id)
            if ins is None:
                continue
            rows.append(
                {
                    "text": p.text or "",
                    "created_at": p.created_at,
                    "media_type": p.media_type or "TEXT",
                    "views": ins.views,
                    "likes": ins.likes,
                    "replies": ins.replies,
                }
            )

    return _compute_report(rows, scope="me")


def analyze_affinity_patterns(top_n: int = 20) -> PatternReport:
    with session_scope() as session:
        creators = session.scalars(
            select(AffinityCreator)
            .order_by(AffinityCreator.engagement_score.desc())
            .limit(top_n)
        ).all()
        creator_ids = [c.id for c in creators]
        if not creator_ids:
            return PatternReport(scope="affinity_top20")
        posts = session.scalars(
            select(AffinityPost).where(AffinityPost.creator_id.in_(creator_ids))
        ).all()
        rows = [
            {
                "text": p.text or "",
                "created_at": p.created_at,
                "media_type": "TEXT",
                "views": 0,
                "likes": p.likes,
                "replies": p.replies,
            }
            for p in posts
        ]
    return _compute_report(rows, scope="affinity_top20")


def _compute_report(rows: list[dict], scope: str) -> PatternReport:
    if not rows:
        return PatternReport(scope=scope)

    likes = [r["likes"] for r in rows]
    views = [r["views"] for r in rows]
    replies = [r["replies"] for r in rows]

    hour_counts: Counter[int] = Counter()
    hour_likes: defaultdict[int, list[int]] = defaultdict(list)
    for r in rows:
        h = _hour(r["created_at"])
        hour_counts[h] += 1
        hour_likes[h].append(r["likes"])

    # "Best" hours = top 3 hours by median likes (min 2 posts)
    hour_median = [
        (h, statistics.median(v)) for h, v in hour_likes.items() if len(v) >= 2
    ]
    hour_median.sort(key=lambda x: x[1], reverse=True)
    best_hours = [h for h, _ in hour_median[:3]]

    # Length buckets
    buckets: defaultdict[str, list[int]] = defaultdict(list)
    for r in rows:
        buckets[_length_bucket(len(r["text"]))].append(r["likes"])
    length_buckets = {
        b: {
            "count": len(v),
            "median_likes": float(statistics.median(v)) if v else 0.0,
            "mean_likes": float(statistics.fmean(v)) if v else 0.0,
        }
        for b, v in buckets.items()
    }

    # Media types
    media: defaultdict[str, list[int]] = defaultdict(list)
    for r in rows:
        media[r["media_type"]].append(r["likes"])
    media_type_performance = {
        m: {
            "count": len(v),
            "median_likes": float(statistics.median(v)) if v else 0.0,
        }
        for m, v in media.items()
    }

    # Hook phrases from top-decile posts
    threshold = _top_decile(likes)
    top_texts = [r["text"] for r in rows if r["likes"] >= threshold and r["text"]]
    hooks = _extract_hooks(top_texts)

    # Sample of top 5 posts for the recommender to see concretely
    top_sorted = sorted(rows, key=lambda r: r["likes"], reverse=True)[:5]
    top_sample = [
        {
            "text": (r["text"] or "")[:240],
            "likes": r["likes"],
            "replies": r["replies"],
            "hour": _hour(r["created_at"]),
            "length": len(r["text"] or ""),
        }
        for r in top_sorted
    ]

    return PatternReport(
        scope=scope,
        post_count=len(rows),
        median_likes=float(statistics.median(likes)) if likes else 0.0,
        median_views=float(statistics.median(views)) if views else 0.0,
        median_replies=float(statistics.median(replies)) if replies else 0.0,
        best_hours_local=best_hours,
        hour_distribution=dict(hour_counts),
        length_buckets=length_buckets,
        media_type_performance=media_type_performance,
        top_hook_phrases=hooks,
        top_posts_sample=top_sample,
    )
