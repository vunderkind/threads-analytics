"""Pure-logic tests for the predicate classifiers — no DB, no API."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace


def _mock_post(text: str = "", hour: int = 12, media_type: str = "TEXT_POST", thread_id: str = "p1"):
    return SimpleNamespace(
        thread_id=thread_id,
        text=text,
        media_type=media_type,
        created_at=datetime(2026, 4, 10, hour, 0, tzinfo=timezone.utc),
    )


def test_timing_hits_window():
    from threads_analytics.predicates import _classify_timing

    cls = _classify_timing(_mock_post(hour=20), {"hours": [19, 20, 21]})
    assert cls.bucket == "variant"
    assert "hour 20" in cls.reason


def test_timing_misses_window():
    from threads_analytics.predicates import _classify_timing

    cls = _classify_timing(_mock_post(hour=3), {"hours": [19, 20, 21]})
    assert cls.bucket == "control"


def test_length_in_range():
    from threads_analytics.predicates import _classify_length

    cls = _classify_length(_mock_post(text="a" * 120), {"min_len": 80, "max_len": 200})
    assert cls.bucket == "variant"
    cls2 = _classify_length(_mock_post(text="a" * 40), {"min_len": 80, "max_len": 200})
    assert cls2.bucket == "control"


def test_media_allowed():
    from threads_analytics.predicates import _classify_media

    cls = _classify_media(
        _mock_post(media_type="IMAGE"), {"media_types": ["IMAGE", "CAROUSEL_ALBUM"]}
    )
    assert cls.bucket == "variant"
    cls2 = _classify_media(
        _mock_post(media_type="TEXT_POST"), {"media_types": ["IMAGE"]}
    )
    assert cls2.bucket == "control"


def test_hook_prefix_match_case_insensitive():
    from threads_analytics.predicates import _classify_hook

    cls = _classify_hook(
        _mock_post(text="You vs me: the battle of the decade"),
        {"prefixes": ["you vs me"]},
    )
    assert cls.bucket == "variant"


def test_hook_regex_match():
    from threads_analytics.predicates import _classify_hook

    cls = _classify_hook(
        _mock_post(text="nobody talks about this detail in invincible"),
        {"regex": r"^nobody talks about"},
    )
    assert cls.bucket == "variant"


def test_hook_no_match():
    from threads_analytics.predicates import _classify_hook

    cls = _classify_hook(
        _mock_post(text="good morning lagos"),
        {"prefixes": ["you vs me"], "regex": r"^hot take"},
    )
    assert cls.bucket == "control"


def test_custom_manual_tagging():
    from threads_analytics.predicates import _classify_custom

    spec = {"variant_post_ids": ["p1", "p2"], "control_post_ids": ["p3"]}
    assert _classify_custom(_mock_post(thread_id="p1"), spec).bucket == "variant"
    assert _classify_custom(_mock_post(thread_id="p3"), spec).bucket == "control"
    # Not-tagged posts default to control with a neutral reason
    out = _classify_custom(_mock_post(thread_id="p99"), spec)
    assert out.bucket == "control"
    assert "not manually tagged" in out.reason
