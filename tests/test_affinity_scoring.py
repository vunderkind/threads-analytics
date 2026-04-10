"""Unit test for the engagement score formula used in affinity.py.

The formula is reimplemented here so we can exercise it without the DB.
"""

from __future__ import annotations

import statistics


def engagement_score(likes: list[int]) -> float:
    if not likes:
        return 0.0
    median = statistics.median(likes)
    count = len(likes)
    return float(median) * (1.0 + min(count, 20) / 10.0)


def test_empty_creators_score_zero():
    assert engagement_score([]) == 0.0


def test_higher_median_beats_lower_median_at_same_volume():
    low = engagement_score([5, 5, 5, 5])
    high = engagement_score([50, 50, 50, 50])
    assert high > low * 9  # roughly 10x, same multiplier


def test_more_posts_boost_score_up_to_cap():
    few = engagement_score([10] * 3)
    many = engagement_score([10] * 15)
    capped = engagement_score([10] * 40)
    assert many > few
    # Cap at 20 posts, so 40 should equal 20
    assert capped == engagement_score([10] * 20)
