"""End-to-end pipeline test with a fake Threads client but REAL Anthropic calls.

This proves every module works together and that Claude produces useful topic
extraction + recommendations against realistic fake data. The ONLY thing this
does not exercise is actual HTTP calls to the Threads Graph API — because those
require an OAuth token that only the user can generate via a browser flow.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select


THREADS_TOKEN_REQUIRED_MARK = pytest.mark.skipif(
    not os.environ.get("ANTHROPIC_API_KEY")
    and not Path(
        Path(__file__).resolve().parents[1] / ".env"
    ).exists(),
    reason="requires .env with ANTHROPIC_API_KEY",
)


# ---------- Fake Threads client ----------


def _fake_post(id_: str, text: str, hours_ago: int, media_type: str = "TEXT"):
    from threads_analytics.threads_client import ThreadsPost

    return ThreadsPost(
        id=id_,
        text=text,
        media_type=media_type,
        permalink=f"https://threads.net/@testuser/post/{id_}",
        created_at=datetime.now(timezone.utc) - timedelta(hours=hours_ago),
        username="testuser",
    )


def _fake_insight(id_: str, likes: int, views: int, replies: int, reposts: int = 0, quotes: int = 0):
    from threads_analytics.threads_client import ThreadsPostInsight

    return ThreadsPostInsight(
        thread_id=id_,
        views=views,
        likes=likes,
        replies=replies,
        reposts=reposts,
        quotes=quotes,
    )


class FakeThreadsClient:
    """Drop-in replacement for ThreadsClient that returns canned data."""

    def __init__(self, follower_count: int = 1200):
        self.follower_count = follower_count
        self.user_id = "fake_user_id"
        self.access_token = "fake_token"
        self.rate_limit_state = type("R", (), {"queries_this_call": 0})()
        self._post_data = self._build_posts()

    def _build_posts(self):
        # 15 posts spanning a few topics with deliberate engagement patterns
        # so the analyzer has something real to surface.
        topics_posts = [
            # Evening AI posts (high engagement)
            ("p1", "building AI agents is mostly about context management — here's what I learned", 5, 120, 2400, 20),
            ("p2", "building AI agents means thinking about tool use carefully", 29, 150, 3000, 30),
            ("p3", "building AI agents the mistake everyone makes is ignoring eval loops", 53, 180, 3500, 35),
            ("p4", "building AI agents I spent a week on prompting and it was worth it", 77, 95, 2100, 15),
            # Morning Nigerian tech posts (medium engagement)
            ("p5", "the Nigerian tech scene is underrated — here's why", 14, 40, 900, 8),
            ("p6", "the Nigerian tech scene produces resilient founders because of the conditions", 38, 55, 1100, 12),
            ("p7", "the Nigerian tech scene has shipped more in 5 years than most expect", 62, 35, 800, 6),
            # Random life posts (low engagement)
            ("p8", "good morning Lagos", 9, 5, 200, 1),
            ("p9", "coffee is life", 33, 3, 150, 0),
            ("p10", "random thought about the weather", 57, 2, 120, 0),
            # Long-form posts (high engagement)
            ("p11", "building AI agents requires discipline around evals. let me explain what I mean in detail. the first thing most teams skip is having a robust test harness. without it you are flying blind and every change feels scary. the second thing is treating your system prompt like code — version it, review it, measure it. the third thing is instrumenting everything.", 4, 200, 4200, 45),
            ("p12", "the Nigerian tech scene deserves a deeper look. I've watched founders here do more with less than anyone else I know. the scrappiness translates into resilience when they go global. here's what I mean specifically.", 28, 80, 1800, 14),
            # Tool-related posts
            ("p13", "tools I use for shipping fast: Claude, Cursor, linear", 52, 60, 1400, 10),
            ("p14", "my shipping stack in 2026", 76, 45, 1200, 8),
            ("p15", "why I moved from notion to linear", 100, 30, 900, 5),
        ]
        out = []
        for i, (pid, text, hours_ago, likes, views, replies) in enumerate(topics_posts):
            post = _fake_post(pid, text, hours_ago)
            insight = _fake_insight(pid, likes=likes, views=views, replies=replies, reposts=likes // 10)
            out.append((post, insight))
        return out

    def list_my_posts(self, limit: int = 100):
        return [p for p, _ in self._post_data[:limit]]

    def get_post_insights(self, thread_id: str):
        for p, i in self._post_data:
            if p.id == thread_id:
                return i
        from threads_analytics.threads_client import ThreadsPostInsight

        return ThreadsPostInsight(thread_id=thread_id)

    def get_account_insights(self):
        from threads_analytics.threads_client import ThreadsAccountInsight

        return ThreadsAccountInsight(
            follower_count=self.follower_count,
            views=40000,
            likes=1200,
            replies=240,
            reposts=120,
            quotes=30,
            demographics={"country": {"NG": 0.55, "US": 0.25, "UK": 0.10}},
        )

    def keyword_search(self, query: str, search_type: str = "TOP", limit: int = 25):
        """Return a handful of fake affinity creator posts per topic query."""
        from threads_analytics.threads_client import (
            SearchResult,
            ThreadsPost,
            ThreadsPostInsight,
        )

        # Three fake creators, each with varying engagement per topic
        creators = [
            ("ai_builder_pro", [500, 600, 550, 700]),
            ("lagos_techie", [300, 280, 350, 400]),
            ("shipping_daily", [150, 180, 170, 200]),
        ]
        results = []
        idx = 0
        for handle, likes_list in creators:
            for k, likes in enumerate(likes_list):
                idx += 1
                post_id = f"fake_{handle}_{query[:6]}_{k}"
                post = ThreadsPost(
                    id=post_id,
                    text=f"{handle} post about {query} — specific opinion #{k}",
                    media_type="TEXT",
                    permalink=f"https://threads.net/@{handle}/post/{post_id}",
                    created_at=datetime.now(timezone.utc) - timedelta(days=k + 1),
                    username=handle,
                )
                insight = ThreadsPostInsight(
                    thread_id=post_id,
                    views=likes * 20,
                    likes=likes,
                    replies=likes // 5,
                    reposts=likes // 10,
                    quotes=likes // 20,
                )
                results.append(
                    SearchResult(
                        post=post,
                        insight=insight,
                        author_handle=handle,
                        author_user_id=None,
                    )
                )
        return results[:limit]

    def refresh_long_lived_token(self):
        return "fake_refreshed_token"

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None


# ---------- The actual test ----------


@pytest.fixture()
def isolated_db(monkeypatch, tmp_path):
    """Point the database at a temporary sqlite file for this test."""
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    # Force reimport of the db module to pick up the new URL
    import importlib
    from threads_analytics import config, db

    config.get_settings.cache_clear()
    db._engine = None
    db._SessionLocal = None
    importlib.reload(db)
    yield db_path


def test_full_pipeline_end_to_end_with_fake_threads(isolated_db, monkeypatch):
    """Run a complete ingest→topics→affinity→analyze→recommend→learn cycle twice."""
    from threads_analytics import pipeline as pipeline_mod
    from threads_analytics import threads_client as tc_mod

    # Patch the ThreadsClient used by the pipeline to our fake
    monkeypatch.setattr(pipeline_mod, "ThreadsClient", FakeThreadsClient)

    # --- First run ---
    summary1 = pipeline_mod.run_full_cycle()
    assert "error" not in summary1, f"first run errored: {summary1.get('error')}"
    assert summary1["ingest"]["posts_fetched"] == 15
    assert summary1["ingest"]["new_posts"] == 15
    assert summary1["ingest"]["follower_count"] == 1200
    assert summary1["my_post_count"] == 15
    assert summary1["affinity_post_count"] > 0
    assert len(summary1["topics"]) >= 3
    assert len(summary1["new_recommendation_ids"]) >= 3

    # Verify DB state
    from threads_analytics.db import session_scope
    from threads_analytics.models import (
        AffinityCreator,
        MyAccountInsight,
        MyPost,
        Recommendation,
        Run,
        Topic,
    )

    with session_scope() as session:
        assert session.scalar(select(Run).where(Run.status == "complete")) is not None
        assert session.query(MyPost).count() == 15
        assert session.query(MyAccountInsight).count() == 1
        assert session.query(Topic).count() >= 3
        assert session.query(AffinityCreator).count() >= 2
        recs = session.scalars(select(Recommendation)).all()
        assert len(recs) >= 3
        for r in recs:
            assert r.title
            assert r.body
            assert r.category
            assert r.status == "pending"

    # --- Second run with a bumped follower count, to exercise the learning loop ---
    def fake_client_more_followers():
        return FakeThreadsClient(follower_count=1350)

    monkeypatch.setattr(pipeline_mod, "ThreadsClient", fake_client_more_followers)

    summary2 = pipeline_mod.run_full_cycle()
    assert "error" not in summary2, f"second run errored: {summary2.get('error')}"
    assert summary2["outcomes_written"] >= 3, "learning loop should have measured prior recs"

    from threads_analytics.models import RecommendationOutcome

    with session_scope() as session:
        outcomes = session.scalars(select(RecommendationOutcome)).all()
        assert len(outcomes) >= 3
        # The follower delta from run 1 → run 2 should be +150 exactly
        positive_deltas = [o for o in outcomes if o.follower_delta == 150]
        assert len(positive_deltas) >= 3, (
            f"expected +150 follower deltas on prior recs, got "
            f"{[o.follower_delta for o in outcomes]}"
        )
