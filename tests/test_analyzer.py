"""Pure-stats tests for the analyzer — no DB, no API."""

from __future__ import annotations

from datetime import datetime, timezone

from threads_analytics.analyzer import _compute_report, _extract_hooks, _length_bucket


def _row(text: str, hour: int, likes: int, media: str = "TEXT", replies: int = 0, views: int = 0):
    return {
        "text": text,
        "created_at": datetime(2026, 4, 1, hour, 0, tzinfo=timezone.utc),
        "media_type": media,
        "likes": likes,
        "replies": replies,
        "views": views,
    }


def test_length_bucketing():
    assert _length_bucket(10) == "short (<80)"
    assert _length_bucket(150) == "medium (80-200)"
    assert _length_bucket(300) == "long (200-400)"
    assert _length_bucket(600) == "very_long (400+)"


def test_extract_hooks_most_common_first_three_words():
    texts = [
        "Here is why building AI agents matters",
        "Here is why the Nigerian tech scene is special",
        "You should ship daily",
        "Here is why shipping daily works",
    ]
    hooks = _extract_hooks(texts, top_k=2)
    assert hooks[0] == "here is why"


def test_compute_report_identifies_best_hours():
    rows = [
        _row("Morning post one", 8, likes=5),
        _row("Morning post two", 8, likes=7),
        _row("Evening post one about AI", 20, likes=100),
        _row("Evening post two about AI", 20, likes=120),
        _row("Late night ramble", 2, likes=1),
        _row("Another late night", 2, likes=2),
    ]
    report = _compute_report(rows, scope="me")
    assert report.post_count == 6
    assert 20 in report.best_hours_local
    assert report.best_hours_local.index(20) == 0
    assert report.median_likes > 0


def test_compute_report_handles_empty_rows():
    report = _compute_report([], scope="me")
    assert report.post_count == 0
    assert report.best_hours_local == []


def test_length_buckets_compute_median_likes():
    rows = [
        _row("short post", 10, likes=2),
        _row("another short", 10, likes=4),
        _row("a" * 150, 10, likes=20),
        _row("b" * 150, 10, likes=30),
    ]
    report = _compute_report(rows, scope="me")
    short = report.length_buckets["short (<80)"]
    medium = report.length_buckets["medium (80-200)"]
    assert short["count"] == 2
    assert medium["count"] == 2
    assert medium["median_likes"] > short["median_likes"]
