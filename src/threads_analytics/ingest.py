"""Pull your own posts + insights from Threads and snapshot them into the DB."""

from __future__ import annotations

import logging

from sqlalchemy import select

from datetime import datetime, timezone

from .db import session_scope
from .models import MyAccountInsight, MyPost, MyPostInsight, MyReply, Profile, Run
from .threads_client import ThreadsClient

log = logging.getLogger(__name__)


def ingest_own_data(run: Run, client: ThreadsClient, post_limit: int = 1000) -> dict:
    """Fetch own posts and per-post + account insights, persist under the given run.

    Returns a small summary dict for logging / the run notes field.
    """
    # Profile (bio, picture, username). Upserted into the profiles table.
    try:
        profile_data = client.get_me()
        with session_scope() as session:
            user_id = str(profile_data.get("id") or "")
            prof = session.get(Profile, user_id) if user_id else None
            if prof is None and user_id:
                prof = Profile(user_id=user_id)
                session.add(prof)
            if prof is not None:
                prof.username = profile_data.get("username") or prof.username or ""
                prof.biography = profile_data.get("threads_biography") or prof.biography
                prof.profile_picture_url = (
                    profile_data.get("threads_profile_picture_url")
                    or prof.profile_picture_url
                )
                prof.updated_at = datetime.now(timezone.utc)
    except Exception as exc:  # noqa: BLE001
        log.warning("profile fetch failed: %s", exc)

    posts = client.list_my_posts(limit=post_limit)
    log.info("Fetched %d own posts", len(posts))

    new_post_ids: list[str] = []
    with session_scope() as session:
        for p in posts:
            existing = session.get(MyPost, p.id)
            if existing is None:
                session.add(
                    MyPost(
                        thread_id=p.id,
                        text=p.text,
                        media_type=p.media_type,
                        media_url=p.media_url,
                        permalink=p.permalink,
                        created_at=p.created_at,
                        first_seen_run_id=run.id,
                    )
                )
                new_post_ids.append(p.id)
            else:
                # Text/permalink shouldn't change but refresh defensively.
                existing.text = p.text or existing.text
                existing.permalink = p.permalink or existing.permalink
                existing.media_type = p.media_type or existing.media_type
                if p.media_url:
                    existing.media_url = p.media_url

    # Threads API rate limits are impression-based (4800 × impressions / 24h),
    # which for a single personal account is effectively unbounded. Refresh
    # insights on every post we know about. Wall-clock is the only real
    # bottleneck and at ~0.4s/call, 1000 posts = ~7 minutes for a full run.
    insight_rows = 0
    with session_scope() as session:
        all_posts = session.scalars(
            select(MyPost).order_by(MyPost.created_at.desc())
        ).all()
        for post in all_posts:
            try:
                ins = client.get_post_insights(post.thread_id)
            except Exception as exc:  # noqa: BLE001
                log.warning("insight fetch failed for %s: %s", post.thread_id, exc)
                continue
            session.add(
                MyPostInsight(
                    thread_id=post.thread_id,
                    run_id=run.id,
                    views=ins.views,
                    likes=ins.likes,
                    replies=ins.replies,
                    reposts=ins.reposts,
                    quotes=ins.quotes,
                )
            )
            insight_rows += 1

    # Account-level snapshot. Always persist a row (even if zeros) so
    # downstream queries (learning loop, dashboard trend) have something to
    # anchor on. Individual metric failures are already logged as warnings
    # inside get_account_insights.
    try:
        account = client.get_account_insights()
    except Exception as exc:  # noqa: BLE001
        log.warning("account insights fetch failed entirely: %s", exc)
        from .threads_client import ThreadsAccountInsight

        account = ThreadsAccountInsight()

    with session_scope() as session:
        session.add(
            MyAccountInsight(
                run_id=run.id,
                follower_count=account.follower_count,
                views=account.views,
                likes=account.likes,
                replies=account.replies,
                reposts=account.reposts,
                quotes=account.quotes,
                demographics_json=account.demographics,
            )
        )

    # Replies made by the user (best-effort)
    replies = []
    try:
        replies = client.list_my_replies(limit=25)
    except Exception as exc:  # noqa: BLE001
        log.warning("replies fetch failed: %s", exc)

    new_reply_ids: list[str] = []
    if replies:
        with session_scope() as session:
            for r in replies:
                if session.get(MyReply, r.id) is None:
                    session.add(
                        MyReply(
                            thread_id=r.id,
                            text=r.text,
                            media_type=r.media_type,
                            permalink=r.permalink,
                            created_at=r.created_at,
                            root_post_id=r.root_post_id,
                            first_seen_run_id=run.id,
                        )
                    )
                    new_reply_ids.append(r.id)

    return {
        "posts_fetched": len(posts),
        "new_posts": len(new_post_ids),
        "insight_rows": insight_rows,
        "replies_fetched": len(replies),
        "new_replies": len(new_reply_ids),
        "follower_count": account.follower_count if account else None,
    }
