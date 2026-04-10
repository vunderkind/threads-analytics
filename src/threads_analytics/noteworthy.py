"""Noteworthy post detection + Claude commentary.

Instead of showing every post, we surface the posts that stood out as
outliers — positive breakouts, conversation starters, reach anomalies,
and flops worth learning from — and pass them to Claude (with the
ranking research context) to explain WHY each one broke out.

The output replaces the vanilla /posts table.
"""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from anthropic import Anthropic
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from .config import get_settings
from .db import session_scope
from .models import MyPost, MyPostInsight, NoteworthyPost, Run

log = logging.getLogger(__name__)

RESEARCH_CONTEXT = """
EPISTEMIC STANDARDS — read carefully before writing anything.

You will be asked to explain why a post behaved the way it did on Threads.
Threads's ranking system is NOT open-sourced. We do not know its exact internals.
What we DO have:

(A) DOCUMENTED FACTS — you may cite these as facts, always with the source:
  - X (formerly Twitter) open-sourced its "heavy ranker" in 2023. The documented
    weights (out of ~100 total): reply that the author replies to and the user
    re-engages with = +75; plain reply = +13.5; like = +0.5; retweet = +1.0.
    This means on X, a reply is ~27× a like and a conversation chain is ~150× a like.
    Source: twitter/the-algorithm on GitHub.
  - Meta has publicly stated replies are the strongest signal on Threads and that
    conversation-driving content is favored. Source: Meta blog / public statements.
  - Bluesky's Discover feed documents that first-hour engagement drives surfacing.

(B) PLAUSIBLE INFERENCES — you may state these, but MUST hedge:
  - Threads likely uses similar reply-weighting to X (same team, same general
    architecture), but the exact multipliers for Threads are unpublished.
  - "Zero-reply penalty loops" are a plausible account-level dynamic but have not
    been documented by Meta as a specific mechanism.
  - "Engagement velocity in first 30-60 min" is documented for X but is inferred
    for Threads from observed behavior + Meta's public statements.

(C) SPECIFIC-POST EXPLANATIONS — always frame as hypotheses:
  - Why one specific post broke out or flopped is not observable — we can only
    offer plausible mechanisms consistent with (A) and (B). Never assert
    causation. Always use words like "likely", "plausible", "consistent with",
    "one plausible explanation is". Never say "the algorithm penalized this post"
    — say "a plausible explanation is that the ranker's first-hour signal..."

HARD RULES:
1. NEVER cite a specific numeric weight as fact unless it's from (A).
2. NEVER attribute causation to "the algorithm" for a specific post. Say "likely",
   "consistent with", "the pattern suggests".
3. Distinguish X-documented (fact) from Threads-inferred (hypothesis) in your
   commentary. Do not conflate them.
4. If you don't know, say "this is not determinable from the available data."
"""


@dataclass
class Candidate:
    post_id: str
    category: str
    metric: str
    value: float
    ratio_vs_median: float | None
    why: str
    post_text: str
    media_type: str
    likes: int
    replies: int
    views: int
    reposts: int
    created_at: datetime | None


@dataclass
class PostBenchmarks:
    """User's own distribution, used to ground Claude's commentary."""
    median_likes: float
    median_replies: float
    median_views: float
    mean_likes: float
    mean_replies: float
    mean_views: float
    p90_likes: float
    p10_likes: float
    max_likes: int
    best_post: dict | None       # the user's single highest-engagement post
    worst_post: dict | None      # the user's worst (non-zero-view) recent post
    mid_post: dict | None        # a median-performing post
    n_posts: int


def compute_benchmarks(session: Session) -> PostBenchmarks | None:
    """Compute the user's own distribution: median/mean/percentiles + best/worst/mid exemplar posts.

    These are the internal yardsticks Claude uses when commenting on a noteworthy
    post — "3× your median views, consistent with your best AI-commentary posts" etc.
    """
    posts = session.scalars(select(MyPost)).all()
    if not posts:
        return None
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
                "post": p,
                "ins": ins,
                "text": (p.text or "")[:280],
                "views": ins.views,
                "likes": ins.likes,
                "replies": ins.replies,
                "created_at": p.created_at,
            }
        )
    if not rows:
        return None

    likes_sorted = sorted(r["likes"] for r in rows)
    replies_sorted = sorted(r["replies"] for r in rows)
    views_sorted = sorted(r["views"] for r in rows)

    def p(arr: list[int], pct: float) -> float:
        if not arr:
            return 0.0
        idx = min(int(len(arr) * pct), len(arr) - 1)
        return float(arr[idx])

    median_likes = statistics.median(likes_sorted) if likes_sorted else 0
    median_replies = statistics.median(replies_sorted) if replies_sorted else 0
    median_views = statistics.median(views_sorted) if views_sorted else 0

    # Composite engagement score: likes + 3 * replies (replies weighted higher,
    # echoing Meta's public statements about Threads ranking).
    def engagement_score(r: dict) -> float:
        return r["likes"] + 3 * r["replies"]

    sorted_by_engagement = sorted(rows, key=engagement_score, reverse=True)
    best = sorted_by_engagement[0] if sorted_by_engagement else None
    # Worst = lowest non-zero view, if any; otherwise the last by engagement
    worst_candidates = [r for r in sorted_by_engagement if r["views"] > 0]
    worst = worst_candidates[-1] if worst_candidates else None
    # Mid = the post closest to the median composite score
    scored = [(engagement_score(r), r) for r in rows]
    scored.sort(key=lambda x: x[0])
    mid = scored[len(scored) // 2][1] if scored else None

    def _exemplar(r: dict | None) -> dict | None:
        if r is None:
            return None
        return {
            "post_id": r["post"].thread_id,
            "text": r["text"],
            "likes": r["likes"],
            "replies": r["replies"],
            "views": r["views"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }

    return PostBenchmarks(
        median_likes=float(median_likes),
        median_replies=float(median_replies),
        median_views=float(median_views),
        mean_likes=float(statistics.fmean([r["likes"] for r in rows])),
        mean_replies=float(statistics.fmean([r["replies"] for r in rows])),
        mean_views=float(statistics.fmean([r["views"] for r in rows])),
        p90_likes=p(likes_sorted, 0.9),
        p10_likes=p(likes_sorted, 0.1),
        max_likes=max(r["likes"] for r in rows),
        best_post=_exemplar(best),
        worst_post=_exemplar(worst),
        mid_post=_exemplar(mid),
        n_posts=len(rows),
    )


def find_noteworthy_candidates(session: Session, limit_per_category: int = 3) -> list[Candidate]:
    """Find posts that stand out as outliers in various dimensions.

    Returns a deduplicated list of candidates, each marked with the dimension
    that made them noteworthy. A single post can only appear once — if it's an
    outlier in multiple dimensions, we keep the most dramatic one.
    """
    posts = session.scalars(select(MyPost)).all()
    if not posts:
        return []

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
                "post": p,
                "ins": ins,
                "views": ins.views,
                "likes": ins.likes,
                "replies": ins.replies,
                "reposts": ins.reposts,
            }
        )
    if not rows:
        return []

    median_views = statistics.median(r["views"] for r in rows) or 1
    median_likes = statistics.median(r["likes"] for r in rows) or 1
    median_replies = statistics.median(r["replies"] for r in rows) or 1
    mean_views = statistics.fmean(r["views"] for r in rows) or 1

    by_id: dict[str, Candidate] = {}

    def _add(cand: Candidate) -> None:
        existing = by_id.get(cand.post_id)
        # Keep the most dramatic ratio
        if existing is None or (
            cand.ratio_vs_median is not None
            and (existing.ratio_vs_median is None or cand.ratio_vs_median > existing.ratio_vs_median)
        ):
            by_id[cand.post_id] = cand

    def _mk(r: dict, category: str, metric: str, value: float, ratio: float | None, why: str) -> Candidate:
        p = r["post"]
        return Candidate(
            post_id=p.thread_id,
            category=category,
            metric=metric,
            value=float(value),
            ratio_vs_median=float(ratio) if ratio is not None else None,
            why=why,
            post_text=(p.text or "")[:400],
            media_type=p.media_type or "TEXT_POST",
            likes=r["likes"],
            replies=r["replies"],
            views=r["views"],
            reposts=r["reposts"],
            created_at=p.created_at,
        )

    # 1. Reach outliers — views > 3x median
    reach_sorted = sorted(rows, key=lambda r: r["views"], reverse=True)
    for r in reach_sorted[:limit_per_category]:
        if r["views"] >= 3 * median_views and r["views"] >= 200:
            ratio = r["views"] / median_views
            _add(
                _mk(
                    r,
                    "reach_outlier",
                    "views",
                    r["views"],
                    ratio,
                    f"{r['views']} views vs median {median_views:.0f} ({ratio:.1f}×)",
                )
            )

    # 2. Conversation starters — replies > 2x median AND ≥ 3 replies
    for r in sorted(rows, key=lambda r: r["replies"], reverse=True)[:limit_per_category]:
        if r["replies"] >= 3 and r["replies"] >= 2 * median_replies:
            ratio = r["replies"] / median_replies if median_replies else None
            _add(
                _mk(
                    r,
                    "conversation_starter",
                    "replies",
                    r["replies"],
                    ratio,
                    f"{r['replies']} replies vs median {median_replies:.0f}",
                )
            )

    # 3. Breakouts — likes >= 10x median
    for r in sorted(rows, key=lambda r: r["likes"], reverse=True)[:limit_per_category]:
        if r["likes"] >= 10 * median_likes and r["likes"] >= 20:
            ratio = r["likes"] / median_likes
            _add(
                _mk(
                    r,
                    "breakout",
                    "likes",
                    r["likes"],
                    ratio,
                    f"{r['likes']} likes vs median {median_likes:.0f} ({ratio:.0f}×)",
                )
            )

    # 4. Flops worth learning from — very recent posts with 0 engagement
    recent_rows = sorted(
        [r for r in rows if r["post"].created_at],
        key=lambda r: r["post"].created_at,
        reverse=True,
    )[:25]
    flops = [r for r in recent_rows if r["likes"] == 0 and r["replies"] == 0]
    for r in flops[:2]:
        _add(
            _mk(
                r,
                "flop",
                "engagement",
                0,
                None,
                f"0 likes, 0 replies — recent zero-engagement post",
            )
        )

    # 5. High reach, low engagement anomaly (served but didn't land)
    for r in rows:
        if r["views"] >= 2 * median_views and r["likes"] <= median_likes and r["replies"] == 0:
            _add(
                _mk(
                    r,
                    "served_but_fell_flat",
                    "views",
                    r["views"],
                    r["views"] / median_views,
                    f"{r['views']} views but 0 replies and {r['likes']} likes — algorithm tested but audience didn't engage",
                )
            )

    return sorted(by_id.values(), key=lambda c: (c.ratio_vs_median or 0), reverse=True)[:10]


def generate_noteworthy_commentary(run: Run) -> list[int]:
    """Find noteworthy posts and run Claude over them for algo-aware commentary.

    Computes the user's own benchmarks (median, best, worst, mid posts) and
    feeds them to Claude as the internal yardstick — so the commentary
    references the user's actual distribution, not generic Threads norms.
    """
    settings = get_settings()

    with session_scope() as session:
        candidates = find_noteworthy_candidates(session)
        benchmarks = compute_benchmarks(session)

    if not candidates:
        log.info("no noteworthy post candidates")
        return []

    # Clear previous noteworthy rows so the page shows only fresh analysis
    with session_scope() as session:
        for existing in session.scalars(select(NoteworthyPost)).all():
            session.delete(existing)

    cand_payload = [
        {
            "post_id": c.post_id,
            "category": c.category,
            "why_noteworthy": c.why,
            "text": c.post_text,
            "media_type": c.media_type,
            "metric": c.metric,
            "value": c.value,
            "ratio_vs_median": c.ratio_vs_median,
            "likes": c.likes,
            "replies": c.replies,
            "views": c.views,
            "reposts": c.reposts,
            "created_hour_utc": c.created_at.hour if c.created_at else None,
        }
        for c in candidates
    ]

    system = (
        "You are a rigorous ranking analyst writing for a non-technical creator "
        "who wants to manage their online growth. You are given:\n"
        "  (A) NOTEWORTHY POSTS — outliers from their own recent posting.\n"
        "  (B) INTERNAL BENCHMARKS — the user's own median/mean/best/worst/mid posts.\n\n"
        "Your job is to explain each noteworthy post by comparing it TO THE USER'S "
        "OWN DISTRIBUTION, not to generic Threads norms. Say things like: "
        "'3× your median views, but 0× your median replies — unusual pattern.' "
        "'Outperformed your mid-tier post by 4× on likes.' 'Under-performed your "
        "worst recent post on engagement rate.'\n\n"
        "For each post write:\n"
        "  - commentary: 2-3 plain-English sentences a creator can understand. "
        "Avoid jargon. Compare to the user's own benchmarks with specific ratios.\n"
        "  - algo_hypothesis: 1-2 sentences on the plausible ranker mechanism. "
        "Must be hedged ('likely', 'consistent with', 'plausible'). Never claim "
        "causation. Cite X-documented weights only when relevant.\n\n"
        f"{RESEARCH_CONTEXT}"
    )

    schema = (
        "Respond with ONLY a JSON object, no prose, no fences. Shape:\n"
        "{\n"
        '  "analyses": [\n'
        "    {\n"
        '      "post_id": "<the exact post_id from the input>",\n'
        '      "commentary": "2-3 plain-English sentences grounded in the user\'s own benchmarks. Compare with specific ratios vs median/best/mid. Accessible to a non-technical reader.",\n'
        '      "algo_hypothesis": "1-2 sentences offering a plausible ranker mechanism CONSISTENT WITH the documented research. Must use hedged language (\'likely\', \'plausible\', \'consistent with\'). Never assert causation."\n'
        "    }\n"
        "  ]\n"
        "}"
    )

    benchmarks_payload = None
    if benchmarks:
        benchmarks_payload = {
            "n_posts": benchmarks.n_posts,
            "your_median_views": benchmarks.median_views,
            "your_median_likes": benchmarks.median_likes,
            "your_median_replies": benchmarks.median_replies,
            "your_mean_views": round(benchmarks.mean_views, 1),
            "your_mean_likes": round(benchmarks.mean_likes, 1),
            "your_p90_likes": benchmarks.p90_likes,
            "your_p10_likes": benchmarks.p10_likes,
            "your_max_likes": benchmarks.max_likes,
            "your_best_post": benchmarks.best_post,
            "your_worst_recent_post": benchmarks.worst_post,
            "your_median_performing_post": benchmarks.mid_post,
        }

    user_msg = (
        f"{schema}\n\n"
        f"INTERNAL BENCHMARKS (the user's own distribution):\n{json.dumps(benchmarks_payload, ensure_ascii=False, indent=2, default=str)}\n\n"
        f"NOTEWORTHY POSTS TO ANALYZE:\n{json.dumps(cand_payload, ensure_ascii=False, indent=2)}"
    )

    client = Anthropic(api_key=settings.anthropic_api_key)
    resp = client.messages.create(
        model=settings.claude_recommender_model,
        max_tokens=4000,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "text", None))
    data = _safe_json(text)
    if not data or "analyses" not in data:
        log.warning("noteworthy commentary produced no parseable JSON: %s", text[:400])
        return []

    analyses_by_id = {a.get("post_id"): a for a in data.get("analyses", [])}
    saved: list[int] = []
    with session_scope() as session:
        for c in candidates:
            analysis = analyses_by_id.get(c.post_id, {})
            np_row = NoteworthyPost(
                run_id=run.id,
                post_thread_id=c.post_id,
                category=c.category,
                remarkable_metric=c.metric,
                remarkable_value=c.value,
                ratio_vs_median=c.ratio_vs_median,
                claude_commentary=analysis.get("commentary", ""),
                algo_hypothesis=analysis.get("algo_hypothesis", ""),
                created_at=datetime.now(timezone.utc),
            )
            session.add(np_row)
            session.flush()
            saved.append(np_row.id)

    log.info("persisted %d noteworthy posts for run %d", len(saved), run.id)
    return saved


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
