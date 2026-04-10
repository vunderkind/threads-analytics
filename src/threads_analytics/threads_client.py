"""Thin wrapper around the Threads Graph API.

All HTTP shape, pagination, and rate-limit accounting for the Threads API lives here,
so the rest of the codebase deals in plain dataclasses.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable

import httpx

from .config import get_settings

log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.threads.net/v1.0"

POST_FIELDS = (
    "id,media_product_type,media_type,media_url,text,permalink,timestamp,username,is_quote_post"
)
REPLY_FIELDS = (
    "id,media_product_type,media_type,text,permalink,timestamp,username,root_post"
)
POST_INSIGHT_METRICS = "views,likes,replies,reposts,quotes"
# Account insights are split across two metric groups by Meta:
#   - "lifetime" metrics (no period parameter): followers_count, follower_demographics
#   - "period" metrics (require since/until): views, likes, replies, reposts, quotes
# Attempting to fetch both groups in one call returns an error.
ACCOUNT_LIFETIME_METRICS = "followers_count"
ACCOUNT_DEMOGRAPHICS_METRIC = "follower_demographics"
ACCOUNT_PERIOD_METRICS = "views,likes,replies,reposts,quotes"


@dataclass
class ThreadsPost:
    id: str
    text: str
    media_type: str | None
    permalink: str | None
    created_at: datetime
    username: str | None = None
    media_url: str | None = None


@dataclass
class ThreadsReply:
    id: str
    text: str
    media_type: str | None
    permalink: str | None
    created_at: datetime
    root_post_id: str | None = None


@dataclass
class ThreadsPostInsight:
    thread_id: str
    views: int = 0
    likes: int = 0
    replies: int = 0
    reposts: int = 0
    quotes: int = 0


@dataclass
class ThreadsAccountInsight:
    follower_count: int = 0
    views: int = 0
    likes: int = 0
    replies: int = 0
    reposts: int = 0
    quotes: int = 0
    demographics: dict[str, Any] | None = None


@dataclass
class SearchResult:
    post: ThreadsPost
    insight: ThreadsPostInsight
    author_handle: str | None
    author_user_id: str | None


@dataclass
class RateLimitState:
    queries_this_call: int = 0
    warnings: list[str] = field(default_factory=list)


def _parse_ts(s: str | None) -> datetime:
    if not s:
        return datetime.utcnow()
    # Threads returns ISO8601 with offset, e.g. "2024-08-01T12:34:56+0000"
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")


class ThreadsClient:
    def __init__(self, access_token: str | None = None, user_id: str | None = None):
        settings = get_settings()
        self.access_token = access_token or settings.threads_access_token
        self.user_id = user_id or settings.threads_user_id
        self._client = httpx.Client(timeout=30.0)
        self.rate_limit_state = RateLimitState()

        if not self.access_token:
            raise RuntimeError(
                "THREADS_ACCESS_TOKEN is not set. Run scripts/setup_token.py first."
            )

    # ---------- HTTP ----------

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = dict(params or {})
        params["access_token"] = self.access_token
        url = f"{GRAPH_BASE}{path}"
        resp = self._client.get(url, params=params)
        if resp.status_code >= 400:
            log.error("Threads API error %s: %s", resp.status_code, resp.text)
            resp.raise_for_status()
        return resp.json()

    # ---------- Own account ----------

    def get_me(self) -> dict[str, Any]:
        return self._get(
            f"/{self.user_id or 'me'}",
            params={"fields": "id,username,threads_profile_picture_url,threads_biography"},
        )

    def list_my_posts(self, limit: int = 100) -> list[ThreadsPost]:
        """Fetch recent own posts, paginated via Meta's `paging.next` URLs
        until `limit` is reached or Meta stops returning pages.

        IMPORTANT: on subsequent pages we call `self._client.get(next_url)`
        DIRECTLY without any params. Passing `params={}` to httpx breaks the
        already-encoded next_url (httpx re-merges and strips the query string
        in a way that silently returns an empty page).
        """
        out: list[ThreadsPost] = []

        def _parse_item(item: dict) -> ThreadsPost:
            return ThreadsPost(
                id=item["id"],
                text=item.get("text", "") or "",
                media_type=item.get("media_type"),
                media_url=item.get("media_url"),
                permalink=item.get("permalink"),
                created_at=_parse_ts(item.get("timestamp")),
                username=item.get("username"),
            )

        # First page — go through _get so we build the URL + inject access_token
        first_data = self._get(
            f"/{self.user_id or 'me'}/threads",
            params={"fields": POST_FIELDS, "limit": min(limit, 100)},
        )
        for item in first_data.get("data", []):
            out.append(_parse_item(item))
            if len(out) >= limit:
                return out
        next_url = first_data.get("paging", {}).get("next")

        # Subsequent pages — raw httpx call on the full next_url, NO params arg
        while next_url and len(out) < limit:
            resp = self._client.get(next_url)
            if resp.status_code >= 400:
                log.error("Threads API error %s on pagination: %s", resp.status_code, resp.text)
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                out.append(_parse_item(item))
                if len(out) >= limit:
                    return out
            next_url = data.get("paging", {}).get("next")
        return out

    def list_my_replies(self, limit: int = 25) -> list[ThreadsReply]:
        """Fetch the user's own replies (their Replies tab). Best-effort — returns
        an empty list if the endpoint isn't available or errors out."""
        out: list[ThreadsReply] = []
        params: dict[str, Any] = {"fields": REPLY_FIELDS, "limit": min(limit, 25)}
        url = f"/{self.user_id or 'me'}/replies"
        try:
            data = self._get(url, params=params)
        except httpx.HTTPError as exc:
            log.warning("list_my_replies failed (endpoint unavailable?): %s", exc)
            return out
        for item in data.get("data", []):
            root_post = item.get("root_post") or {}
            root_id = root_post.get("id") if isinstance(root_post, dict) else None
            out.append(
                ThreadsReply(
                    id=item["id"],
                    text=item.get("text", "") or "",
                    media_type=item.get("media_type"),
                    permalink=item.get("permalink"),
                    created_at=_parse_ts(item.get("timestamp")),
                    root_post_id=root_id,
                )
            )
            if len(out) >= limit:
                break
        return out

    def get_post_insights(self, thread_id: str) -> ThreadsPostInsight:
        data = self._get(f"/{thread_id}/insights", params={"metric": POST_INSIGHT_METRICS})
        metrics = _metrics_to_dict(data.get("data", []))
        return ThreadsPostInsight(
            thread_id=thread_id,
            views=int(metrics.get("views", 0)),
            likes=int(metrics.get("likes", 0)),
            replies=int(metrics.get("replies", 0)),
            reposts=int(metrics.get("reposts", 0)),
            quotes=int(metrics.get("quotes", 0)),
        )

    def get_account_insights(self) -> ThreadsAccountInsight:
        """Fetch account-level insights.

        Meta's Threads insights API requires separate calls for lifetime and
        period metrics. We call each group separately and swallow failures on
        the non-critical ones so a partial result is still returned.
        """
        import time
        from datetime import datetime, timedelta, timezone

        base = f"/{self.user_id or 'me'}/threads_insights"
        result = ThreadsAccountInsight()

        # Lifetime: followers_count
        try:
            data = self._get(base, params={"metric": ACCOUNT_LIFETIME_METRICS})
            metrics = _metrics_to_dict(data.get("data", []))
            result.follower_count = int(metrics.get("followers_count", 0) or 0)
        except httpx.HTTPError as exc:
            log.warning("followers_count fetch failed: %s", exc)

        # Lifetime: follower_demographics (separate call; sometimes requires
        # a breakdown parameter to return data)
        for breakdown in ("country", "city", "age", "gender"):
            try:
                data = self._get(
                    base,
                    params={
                        "metric": ACCOUNT_DEMOGRAPHICS_METRIC,
                        "breakdown": breakdown,
                    },
                )
                metrics = _metrics_to_dict(data.get("data", []))
                demo = metrics.get("follower_demographics")
                if demo:
                    if result.demographics is None:
                        result.demographics = {}
                    result.demographics[breakdown] = demo
                # A single successful breakdown is enough for v1
                break
            except httpx.HTTPError:
                continue

        # Period metrics: views/likes/replies/reposts/quotes over the last 30 days
        try:
            now = datetime.now(timezone.utc)
            since = int((now - timedelta(days=30)).timestamp())
            until = int(now.timestamp())
            data = self._get(
                base,
                params={
                    "metric": ACCOUNT_PERIOD_METRICS,
                    "since": since,
                    "until": until,
                },
            )
            metrics = _metrics_to_dict(data.get("data", []))
            result.views = int(metrics.get("views", 0) or 0)
            result.likes = int(metrics.get("likes", 0) or 0)
            result.replies = int(metrics.get("replies", 0) or 0)
            result.reposts = int(metrics.get("reposts", 0) or 0)
            result.quotes = int(metrics.get("quotes", 0) or 0)
        except httpx.HTTPError as exc:
            log.warning("period account metrics fetch failed: %s", exc)

        return result

    # ---------- Keyword search (affinity discovery) ----------

    def keyword_search(
        self,
        query: str,
        search_type: str = "TOP",
        limit: int = 25,
    ) -> list[SearchResult]:
        """Public keyword search. Rate-limited to 500 queries / 7 rolling days."""
        self.rate_limit_state.queries_this_call += 1
        params = {
            "q": query,
            "search_type": search_type,
            "fields": POST_FIELDS,
            "limit": min(limit, 25),
        }
        data = self._get("/keyword_search", params=params)
        results: list[SearchResult] = []
        for item in data.get("data", []):
            post = ThreadsPost(
                id=item["id"],
                text=item.get("text", "") or "",
                media_type=item.get("media_type"),
                permalink=item.get("permalink"),
                created_at=_parse_ts(item.get("timestamp")),
                username=item.get("username"),
            )
            # Public engagement counts come via /{id}/insights but are public for non-private
            # accounts. We best-effort fetch them and swallow failures.
            insight = ThreadsPostInsight(thread_id=post.id)
            try:
                insight = self.get_post_insights(post.id)
            except httpx.HTTPError:
                pass
            results.append(
                SearchResult(
                    post=post,
                    insight=insight,
                    author_handle=item.get("username"),
                    author_user_id=None,
                )
            )
        return results

    # ---------- Token ----------

    def refresh_long_lived_token(self) -> str:
        """Swap the current long-lived token for a fresh one (valid 60 days)."""
        data = self._get(
            "/refresh_access_token",
            params={"grant_type": "th_refresh_token"},
        )
        new_token = data.get("access_token")
        if not new_token:
            raise RuntimeError(f"Unexpected refresh response: {data}")
        self.access_token = new_token
        return new_token

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "ThreadsClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def _metrics_to_dict(metric_list: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Flatten the Graph API's {name, values:[{value}]} shape into name->value."""
    out: dict[str, Any] = {}
    for m in metric_list:
        name = m.get("name")
        values = m.get("values") or m.get("total_value") or []
        if isinstance(values, dict):
            out[name] = values.get("value")
        elif isinstance(values, list) and values:
            first = values[0]
            if isinstance(first, dict):
                out[name] = first.get("value")
            else:
                out[name] = first
    return out
