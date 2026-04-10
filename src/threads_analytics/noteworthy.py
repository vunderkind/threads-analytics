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


CATEGORY_META: dict[str, dict] = {
    "breakout": {
        "label": "breakout",
        "lesson": "What went right here? Study this and repeat the pattern.",
    },
    "conversation_catalyst": {
        "label": "conversation catalyst",
        "lesson": "Something about this post invited dialogue. Find it.",
    },
    "quiet_winner": {
        "label": "quiet winner",
        "lesson": "Algorithm didn't push it, but the few who saw it loved it. Worth re-surfacing.",
    },
    "served_but_flat": {
        "label": "served but flat",
        "lesson": "Algorithm tested this in a wider pool but the audience rejected it. Topic was interesting enough to distribute — framing or hook was wrong.",
    },
    "reply_magnet": {
        "label": "reply magnet",
        "lesson": "Replies dominated over likes. Conversation-first framing worked — lean into that voice.",
    },
    "format_win": {
        "label": "format win",
        "lesson": "This media format dramatically outperformed your average. Use it more.",
    },
    "unexpected_short_hit": {
        "label": "short & punchy",
        "lesson": "A very short post that broke out. Brevity + a sharp idea beat effort. Study the concept.",
    },
    "high_effort_flop": {
        "label": "high-effort flop",
        "lesson": "A long post with no engagement. Effort didn't correlate with reach — the hook, length, or topic missed. Don't equate work with value.",
    },
}


def find_noteworthy_candidates(session: Session, limit_per_category: int = 3) -> list[Candidate]:
    """Surface posts that carry a distinct learning lesson.

    Each category maps to a specific insight the creator can extract and
    apply. A post only appears in one category — we assign it to the
    category where its lesson is strongest, using a simple priority ordering
    so 'quiet_winner' beats 'breakout' when both apply (quiet winners are
    more surprising), etc.
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
        text_len = len(p.text or "")
        rows.append(
            {
                "post": p,
                "ins": ins,
                "views": ins.views,
                "likes": ins.likes,
                "replies": ins.replies,
                "reposts": ins.reposts,
                "media_type": p.media_type or "TEXT_POST",
                "text_len": text_len,
            }
        )
    if not rows:
        return []

    # Global benchmarks
    median_views = max(statistics.median(r["views"] for r in rows), 1)
    median_likes = max(statistics.median(r["likes"] for r in rows), 1)
    median_replies = max(statistics.median(r["replies"] for r in rows), 1)
    median_text_len = max(statistics.median(r["text_len"] for r in rows if r["text_len"]), 1)

    total_likes = sum(r["likes"] for r in rows)
    total_replies = sum(r["replies"] for r in rows)
    account_reply_to_like_ratio = (total_replies / total_likes) if total_likes else 0

    # Format benchmarks: median likes within each media type
    format_median_likes: dict[str, float] = {}
    for media_type in {r["media_type"] for r in rows}:
        subset = [r["likes"] for r in rows if r["media_type"] == media_type]
        if subset:
            format_median_likes[media_type] = max(statistics.median(subset), 1)

    by_id: dict[str, Candidate] = {}

    # Category priorities: lower rank = higher priority. A post ends up in
    # whichever of its matching categories has the best rank.
    priority_rank = {
        "quiet_winner": 1,      # most surprising insight
        "high_effort_flop": 2,  # clearest learning
        "format_win": 3,
        "unexpected_short_hit": 4,
        "reply_magnet": 5,
        "conversation_catalyst": 6,
        "served_but_flat": 7,
        "breakout": 8,          # most intuitive — save for last
    }

    def _add(cand: Candidate) -> None:
        existing = by_id.get(cand.post_id)
        if existing is None:
            by_id[cand.post_id] = cand
            return
        if priority_rank[cand.category] < priority_rank[existing.category]:
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

    # --- 1. Breakouts: likes OR views ≥ 5× median
    for r in sorted(rows, key=lambda r: r["likes"], reverse=True)[:limit_per_category]:
        if r["likes"] >= 5 * median_likes and r["likes"] >= 10:
            ratio = r["likes"] / median_likes
            _add(_mk(r, "breakout", "likes", r["likes"], ratio,
                     f"{r['likes']} likes vs median {median_likes:.0f} ({ratio:.1f}×)"))
    for r in sorted(rows, key=lambda r: r["views"], reverse=True)[:limit_per_category]:
        if r["views"] >= 5 * median_views and r["views"] >= 200:
            ratio = r["views"] / median_views
            _add(_mk(r, "breakout", "views", r["views"], ratio,
                     f"{r['views']} views vs median {median_views:.0f} ({ratio:.1f}×)"))

    # --- 2. Conversation catalyst: replies ≥ 3× median AND ≥3 replies
    for r in sorted(rows, key=lambda r: r["replies"], reverse=True)[:limit_per_category]:
        if r["replies"] >= 3 and r["replies"] >= 3 * median_replies:
            ratio = r["replies"] / median_replies
            _add(_mk(r, "conversation_catalyst", "replies", r["replies"], ratio,
                     f"{r['replies']} replies vs median {median_replies:.0f} ({ratio:.1f}×)"))

    # --- 3. Quiet winners: views ≤ median BUT like-per-view rate ≥ 3× account average
    account_like_per_view = (
        sum(r["likes"] for r in rows) / sum(r["views"] for r in rows)
        if sum(r["views"] for r in rows) else 0
    )
    for r in rows:
        if r["views"] == 0:
            continue
        lpv = r["likes"] / r["views"]
        if (
            r["views"] <= median_views
            and account_like_per_view > 0
            and lpv >= 3 * account_like_per_view
            and r["likes"] >= 3
        ):
            ratio = lpv / account_like_per_view
            _add(_mk(r, "quiet_winner", "like_per_view", lpv, ratio,
                     f"{r['likes']} likes on only {r['views']} views — {ratio:.1f}× your like-per-view rate"))

    # --- 4. Served but flat: views ≥ 2× median AND engagement rate ≤ 0.3× account
    account_engagement_per_view = (
        (sum(r["likes"] for r in rows) + sum(r["replies"] for r in rows))
        / sum(r["views"] for r in rows)
        if sum(r["views"] for r in rows) else 0
    )
    for r in rows:
        if r["views"] == 0 or account_engagement_per_view == 0:
            continue
        epv = (r["likes"] + r["replies"]) / r["views"]
        if r["views"] >= 2 * median_views and epv <= 0.3 * account_engagement_per_view:
            ratio = r["views"] / median_views
            _add(_mk(r, "served_but_flat", "views", r["views"], ratio,
                     f"{r['views']} views ({ratio:.1f}× median) but {r['likes']} likes, {r['replies']} replies — audience saw it and passed"))

    # --- 5. Reply magnet: reply-to-like ratio ≥ 2× account's own ratio AND ≥2 replies
    if account_reply_to_like_ratio > 0:
        for r in rows:
            if r["likes"] == 0 or r["replies"] < 2:
                continue
            rtl = r["replies"] / r["likes"]
            if rtl >= 2 * account_reply_to_like_ratio:
                ratio = rtl / account_reply_to_like_ratio
                _add(_mk(r, "reply_magnet", "reply_to_like", rtl, ratio,
                         f"{r['replies']} replies on {r['likes']} likes — {ratio:.1f}× your usual reply-to-like ratio"))

    # --- 6. Format win: a non-dominant media type massively outperformed its own subset
    for r in rows:
        mt = r["media_type"]
        fm = format_median_likes.get(mt, 0)
        if fm <= 0:
            continue
        if mt != "TEXT_POST" and r["likes"] >= 3 * fm and r["likes"] >= 2 * median_likes:
            ratio = r["likes"] / median_likes
            _add(_mk(r, "format_win", "likes", r["likes"], ratio,
                     f"{mt} post with {r['likes']} likes — {ratio:.1f}× your median (and this format is usually underused)"))

    # --- 7. Unexpected short hit: text length ≤ median/2 AND likes ≥ 3× median
    for r in rows:
        if r["text_len"] and r["text_len"] <= median_text_len / 2 and r["likes"] >= 3 * median_likes:
            ratio = r["likes"] / median_likes
            _add(_mk(r, "unexpected_short_hit", "likes", r["likes"], ratio,
                     f"{r['text_len']} chars, {r['likes']} likes — brevity won ({ratio:.1f}× median likes)"))

    # --- 8. High-effort flop: text length ≥ 2× median AND 0 likes + 0 replies
    for r in rows:
        if (
            r["text_len"] >= 2 * median_text_len
            and r["likes"] == 0
            and r["replies"] == 0
            and r["views"] >= median_views  # algo did serve it, it just fell flat
        ):
            _add(_mk(r, "high_effort_flop", "length_vs_engagement", r["text_len"], None,
                     f"{r['text_len']} chars of effort, {r['views']} views, 0 likes + 0 replies"))

    # Sort: bring the highest-priority categories first, and within a category
    # by ratio descending.
    return sorted(
        by_id.values(),
        key=lambda c: (priority_rank[c.category], -(c.ratio_vs_median or 0)),
    )[:10]


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
        "who wants to LEARN from their own posting patterns. Every noteworthy "
        "post is an opportunity for a specific lesson, and the lesson depends "
        "on the category the post falls into.\n\n"
        "You are given:\n"
        "  (A) NOTEWORTHY POSTS — outliers from the creator's own recent posting, "
        "each pre-classified into a CATEGORY that names the lesson type.\n"
        "  (B) CATEGORY LESSONS — the learning frame for each category.\n"
        "  (C) INTERNAL BENCHMARKS — the creator's own median/mean/best/worst/mid posts.\n\n"
        "For EACH post, write exactly two things:\n"
        "  - lesson: 2-3 plain sentences explaining what this specific post "
        "teaches the creator, framed around the category's learning angle. "
        "Compare to the creator's OWN distribution with specific ratios from "
        "the benchmarks. Tell them what to DO with this insight. Avoid jargon.\n"
        "  - algo_hypothesis: 1-2 sentences on the plausible ranker mechanism. "
        "Must be hedged ('likely', 'consistent with', 'plausible'). Never claim "
        "causation. Only cite X-documented ranker weights where relevant.\n\n"
        f"{RESEARCH_CONTEXT}"
    )

    schema = (
        "Respond with ONLY a JSON object, no prose, no fences. Shape:\n"
        "{\n"
        '  "analyses": [\n'
        "    {\n"
        '      "post_id": "<the exact post_id from the input>",\n'
        '      "commentary": "2-3 plain sentences. The LESSON this post teaches, framed by its category. Compare to the user\'s own benchmarks with specific ratios. End with an actionable takeaway.",\n'
        '      "algo_hypothesis": "1-2 sentences offering a plausible ranker mechanism CONSISTENT WITH the documented research. Must use hedged language. Never assert causation."\n'
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
        f"CATEGORY LESSONS (the learning frame for each noteworthy category):\n"
        f"{json.dumps(CATEGORY_META, ensure_ascii=False, indent=2)}\n\n"
        f"INTERNAL BENCHMARKS (the creator's own distribution):\n{json.dumps(benchmarks_payload, ensure_ascii=False, indent=2, default=str)}\n\n"
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
