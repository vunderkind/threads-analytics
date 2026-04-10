"""Discover and rank affinity creators via Threads keyword search.

Rate-limit discipline lives here: we count queries per run and refuse to issue new
searches if the projected 7-day usage would exceed the configured rolling budget.
"""

from __future__ import annotations

import logging
import statistics
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from .config import get_settings
from .db import session_scope
from .models import AffinityCreator, AffinityPost, Run, Topic
from .threads_client import ThreadsClient

log = logging.getLogger(__name__)

STALE_AFTER = timedelta(days=3)


def discover_affinity_creators(run: Run, client: ThreadsClient) -> dict:
    """Run keyword search for stale/new topics, upsert creators and their posts.

    Returns a summary dict with counts + query budget used.
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    seven_days_ago = now - timedelta(days=7)

    # Count queries used in the last rolling window across prior runs.
    with session_scope() as session:
        rolling_used = (
            session.execute(
                select(func.coalesce(func.sum(Run.keyword_search_queries_used), 0)).where(
                    Run.started_at >= seven_days_ago
                )
            ).scalar_one()
        ) or 0

        stale_topics = session.scalars(
            select(Topic).where(
                (Topic.last_searched_at.is_(None)) | (Topic.last_searched_at < now - STALE_AFTER)
            )
        ).all()
        # Detach for use outside the session
        topic_rows = [(t.id, t.label) for t in stale_topics]

    remaining_rolling = max(0, settings.keyword_search_rolling_budget - rolling_used)
    per_run_budget = min(settings.keyword_search_budget_per_run, remaining_rolling)

    if per_run_budget <= 0:
        log.warning(
            "keyword search skipped: rolling budget exhausted (%d used in last 7d)", rolling_used
        )
        return {
            "topics_searched": 0,
            "queries_used": 0,
            "rolling_used_before": rolling_used,
            "skipped_reason": "rolling_budget_exhausted",
        }

    queries_used = 0
    creators_touched = 0
    posts_touched = 0
    permission_denied = False

    for topic_id, label in topic_rows:
        if queries_used >= per_run_budget:
            break
        if permission_denied:
            break
        try:
            results = client.keyword_search(label, search_type="TOP", limit=25)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            # Detect the "app has not been approved for keyword_search"
            # response and stop retrying immediately.
            if "does not have permission" in msg or "THApiException" in msg:
                log.warning(
                    "keyword_search permission denied by Meta \u2014 likely missing "
                    "threads_keyword_search scope or app review. Skipping affinity "
                    "discovery for this run; recommender will work from own data only."
                )
                permission_denied = True
                break
            log.warning("keyword_search failed for %r: %s", label, exc)
            continue
        queries_used += 1 + sum(1 for _ in results if _.insight)  # insight fetches count too
        # Note: client.rate_limit_state already counts; we keep a local counter for the return.

        with session_scope() as session:
            topic = session.get(Topic, topic_id)
            if topic is None:
                continue
            topic.last_searched_at = now
            for r in results:
                handle = r.author_handle or "unknown"
                creator = session.scalar(
                    select(AffinityCreator).where(AffinityCreator.handle == handle)
                )
                if creator is None:
                    creator = AffinityCreator(
                        handle=handle,
                        user_id=r.author_user_id,
                        discovered_via_topic_id=topic_id,
                    )
                    session.add(creator)
                    session.flush()
                creator.last_refreshed_at = now
                creators_touched += 1

                existing_post = session.get(AffinityPost, r.post.id)
                if existing_post is None:
                    session.add(
                        AffinityPost(
                            thread_id=r.post.id,
                            creator_id=creator.id,
                            text=r.post.text or "",
                            likes=r.insight.likes,
                            replies=r.insight.replies,
                            reposts=r.insight.reposts,
                            quotes=r.insight.quotes,
                            created_at=r.post.created_at,
                        )
                    )
                    posts_touched += 1
                else:
                    existing_post.likes = r.insight.likes
                    existing_post.replies = r.insight.replies
                    existing_post.reposts = r.insight.reposts
                    existing_post.quotes = r.insight.quotes

    # Rescore all creators.
    with session_scope() as session:
        creators = session.scalars(select(AffinityCreator)).all()
        for c in creators:
            posts = session.scalars(
                select(AffinityPost).where(AffinityPost.creator_id == c.id)
            ).all()
            if not posts:
                c.engagement_score = 0.0
                continue
            likes = [p.likes for p in posts]
            median_likes = statistics.median(likes) if likes else 0
            post_count = len(posts)
            # composite score: rewards consistent high engagement + frequency
            c.engagement_score = float(median_likes) * (1.0 + min(post_count, 20) / 10.0)

    # Write back the query count onto the run row.
    with session_scope() as session:
        run_row = session.get(Run, run.id)
        if run_row is not None:
            run_row.keyword_search_queries_used = (
                run_row.keyword_search_queries_used or 0
            ) + queries_used

    return {
        "topics_searched": len([t for t in topic_rows if queries_used > 0]),
        "queries_used": queries_used,
        "rolling_used_before": rolling_used,
        "creators_touched": creators_touched,
        "posts_touched": posts_touched,
    }
