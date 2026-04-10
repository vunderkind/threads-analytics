"""Statistical correctness tests for verdict.py.

Pure stats — no DB, no Claude.
"""

from __future__ import annotations

import random

from threads_analytics.verdict import (
    _bootstrap_median_diff_ci,
    _cliffs_delta,
    _stat_verdict,
)


def test_cliffs_delta_all_greater():
    a = [10, 20, 30]
    b = [1, 2, 3]
    # every a > every b  → delta = 1.0
    assert _cliffs_delta(a, b) == 1.0


def test_cliffs_delta_all_less():
    a = [1, 2, 3]
    b = [10, 20, 30]
    assert _cliffs_delta(a, b) == -1.0


def test_cliffs_delta_identical():
    a = [5, 5, 5]
    b = [5, 5, 5]
    assert _cliffs_delta(a, b) == 0.0


def test_bootstrap_ci_brackets_true_diff():
    # True median(variant) - median(control) ≈ 10
    random.seed(0)
    variant = [15 + random.random() for _ in range(30)]
    control = [5 + random.random() for _ in range(30)]
    low, high = _bootstrap_median_diff_ci(variant, control, n_resamples=500)
    assert low is not None and high is not None
    assert low < 10 < high, f"CI should bracket 10 but was [{low:.3f}, {high:.3f}]"


def test_bootstrap_ci_small_sample_returns_none():
    low, high = _bootstrap_median_diff_ci([1], [2])
    assert low is None and high is None


def test_stat_verdict_clear_win_on_reach_rate():
    """Variant substantially higher than control → WIN for an 'up' metric."""
    random.seed(1)
    variant = [0.08 + random.random() * 0.02 for _ in range(20)]
    control = [0.04 + random.random() * 0.02 for _ in range(20)]
    result = _stat_verdict(variant, control, metric_name="reach_rate")
    assert result.verdict == "win"
    assert result.effect_size_pct is not None and result.effect_size_pct > 0.3
    assert result.p_value is not None and result.p_value < 0.05
    assert result.variant_n == 20 and result.control_n == 20


def test_stat_verdict_null_when_identical():
    random.seed(2)
    variant = [0.05 + random.random() * 0.001 for _ in range(15)]
    control = [0.05 + random.random() * 0.001 for _ in range(15)]
    result = _stat_verdict(variant, control, metric_name="reach_rate")
    assert result.verdict == "null"


def test_stat_verdict_loss_when_variant_worse_on_up_metric():
    """Variant lower than control on an 'up' metric → LOSS."""
    random.seed(3)
    variant = [0.02 + random.random() * 0.005 for _ in range(20)]
    control = [0.08 + random.random() * 0.005 for _ in range(20)]
    result = _stat_verdict(variant, control, metric_name="reach_rate")
    assert result.verdict == "loss"
    assert result.effect_size_pct is not None and result.effect_size_pct < 0


def test_stat_verdict_loss_when_variant_worse_on_down_metric():
    """For zero_reply_fraction, LOWER is better. Variant higher → LOSS."""
    random.seed(4)
    variant = [0.9 + random.random() * 0.02 for _ in range(20)]
    control = [0.3 + random.random() * 0.02 for _ in range(20)]
    result = _stat_verdict(variant, control, metric_name="zero_reply_fraction")
    assert result.verdict == "loss"  # variant is worse (higher) on a down metric


def test_stat_verdict_win_when_variant_lower_on_down_metric():
    """For zero_reply_fraction, LOWER is better. Variant lower → WIN."""
    random.seed(5)
    variant = [0.1 + random.random() * 0.02 for _ in range(20)]
    control = [0.6 + random.random() * 0.02 for _ in range(20)]
    result = _stat_verdict(variant, control, metric_name="zero_reply_fraction")
    assert result.verdict == "win"
