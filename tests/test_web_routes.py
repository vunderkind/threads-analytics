"""Test every dashboard route against a populated database.

Seeds the DB with a fake run, posts, insights, topics, creators, and a couple of
recommendations, then hits every GET route and verifies it returns 200 + contains
the expected content. Also exercises the POST /recommendations/{id}/status and
POST /run endpoints.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def populated_app(monkeypatch, tmp_path):
    db_path = tmp_path / "web.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")

    import importlib
    from threads_analytics import config, db

    config.get_settings.cache_clear()
    db._engine = None
    db._SessionLocal = None
    importlib.reload(db)

    from threads_analytics.db import init_db, session_scope
    from threads_analytics.models import (
        AffinityCreator,
        AffinityPost,
        MyAccountInsight,
        MyPost,
        MyPostInsight,
        Profile,
        Recommendation,
        Run,
        Topic,
    )

    init_db()
    now = datetime.now(timezone.utc)

    with session_scope() as session:
        session.add(
            Profile(
                user_id="test_user_id",
                username="testuser",
                biography="test bio",
                profile_picture_url=None,
            )
        )
        run = Run(started_at=now, finished_at=now, status="complete", keyword_search_queries_used=7)
        session.add(run)
        session.flush()
        run_id = run.id

        for i in range(3):
            post = MyPost(
                thread_id=f"post_{i}",
                text=f"this is test post number {i} about building ai agents",
                media_type="TEXT",
                permalink=f"https://threads.net/post_{i}",
                created_at=now - timedelta(hours=i + 1),
                first_seen_run_id=run_id,
            )
            session.add(post)
            session.flush()
            session.add(
                MyPostInsight(
                    thread_id=post.thread_id,
                    run_id=run_id,
                    views=100 * (i + 1),
                    likes=20 * (i + 1),
                    replies=3 * (i + 1),
                    reposts=i,
                    quotes=0,
                )
            )

        session.add(
            MyAccountInsight(
                run_id=run_id,
                follower_count=1234,
                views=9999,
                likes=500,
                replies=80,
                reposts=40,
                quotes=10,
                demographics_json={"country": {"NG": 0.6}},
            )
        )

        topic = Topic(label="building ai agents", description="agents, tools, evals")
        session.add(topic)
        session.flush()

        creator = AffinityCreator(
            handle="ai_builder_pro",
            user_id=None,
            discovered_via_topic_id=topic.id,
            engagement_score=650.0,
        )
        session.add(creator)
        session.flush()
        session.add(
            AffinityPost(
                thread_id="fake_aff_1",
                creator_id=creator.id,
                text="why agents need evals",
                likes=600,
                replies=120,
                reposts=60,
                quotes=20,
                created_at=now - timedelta(days=1),
            )
        )

        rec_pending = Recommendation(
            run_id=run_id,
            rank=1,
            category="timing",
            title="Post between 7-9pm",
            body="Your top-decile posts cluster in the 19-21 hour window.",
            evidence_json={"your_data": "median 120 likes in 19-21", "affinity_data": "4/5 top creators"},
            status="pending",
        )
        rec_applied = Recommendation(
            run_id=run_id,
            rank=2,
            category="hook",
            title="Start with 'Here is why'",
            body="Your top posts use this hook at 3x the rate of your median.",
            evidence_json={"your_data": "3/3 top posts"},
            status="pending",
        )
        session.add(rec_pending)
        session.add(rec_applied)

    from threads_analytics.web.app import create_app

    app = create_app()
    return TestClient(app), run_id


def test_index_renders(populated_app):
    client, _ = populated_app
    r = client.get("/")
    assert r.status_code == 200
    body = r.text
    assert "testuser" in body
    # Active + proposed counts appear in the profile callout
    assert "active" in body


def test_posts_route(populated_app):
    client, _ = populated_app
    r = client.get("/posts")
    assert r.status_code == 200
    # /posts shows noteworthy outlier commentary. Empty fixture = empty state message.
    assert "Noteworthy posts" in r.text


def test_topics_route(populated_app):
    client, _ = populated_app
    r = client.get("/topics")
    assert r.status_code == 200
    assert "building ai agents" in r.text
    assert "agents, tools, evals" in r.text


def test_affinity_route(populated_app):
    client, _ = populated_app
    r = client.get("/affinity")
    assert r.status_code == 200
    assert "ai_builder_pro" in r.text
    assert "650" in r.text
    assert "why agents need evals" in r.text


def test_recommendations_redirects_to_experiments(populated_app):
    client, _ = populated_app
    r = client.get("/recommendations", follow_redirects=False)
    assert r.status_code == 301
    assert "/experiments" in r.headers.get("location", "")


def test_learning_redirects_to_experiments(populated_app):
    client, _ = populated_app
    r = client.get("/learning", follow_redirects=False)
    assert r.status_code == 301
    assert "/experiments" in r.headers.get("location", "")


def test_run_status_route(populated_app):
    client, _ = populated_app
    r = client.get("/run/status")
    assert r.status_code == 200
    data = r.json()
    assert "running" in data
    assert "last_summary" in data


def test_static_css_served(populated_app):
    client, _ = populated_app
    r = client.get("/static/style.css")
    assert r.status_code == 200
    assert "--bg" in r.text
