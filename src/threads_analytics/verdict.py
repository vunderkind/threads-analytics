"""Statistical verdict engine.

Given an experiment, compute a scientific verdict on whether the intervention
moved the primary metric. Supports both per-post and per-window experiments.

Methods used (all designed for small samples and skewed distributions):
  - Mann-Whitney U test (non-parametric, no normality assumption)
  - Cliff's delta for effect size (ordinal, handles skew)
  - Bootstrap 95% CI on the median difference (1000 resamples)
"""

from __future__ import annotations

import logging
import random
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import numpy as np
from anthropic import Anthropic
from scipy.stats import mannwhitneyu
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import get_settings
from .metrics import (
    METRIC_FOLLOWER_VELOCITY,
    METRIC_REACH_RATE,
    METRIC_REPLY_RATE_PER_VIEW,
    METRIC_REPLY_TO_LIKE_RATIO,
    METRIC_TOP_DECILE_MULTIPLE,
    METRIC_ZERO_REPLY_FRACTION,
    METRIC_META,
    compute_metric,
)
from .models import (
    Experiment,
    ExperimentPostClassification,
    ExperimentVerdict,
    MyPost,
    MyPostInsight,
)
from .predicates import PER_WINDOW_CATEGORIES, classify_post

log = logging.getLogger(__name__)

MIN_N_PER_BUCKET = 5


@dataclass
class VerdictResult:
    verdict: str  # win | loss | null | insufficient_data
    primary_metric_baseline: float | None
    primary_metric_variant: float | None
    effect_size_pct: float | None
    effect_cliffs_delta: float | None
    p_value: float | None
    ci_low: float | None
    ci_high: float | None
    variant_n: int
    control_n: int
    honest_interpretation: str


def evaluate_experiment(session: Session, experiment: Experiment) -> VerdictResult:
    """Compute and return a verdict. Persisting is done by the caller."""
    if experiment.variant_start is None or experiment.variant_end is None:
        return _insufficient("experiment window not set")

    if experiment.category in PER_WINDOW_CATEGORIES:
        return _evaluate_per_window(session, experiment)
    return _evaluate_per_post(session, experiment)


# ---------- per-post evaluation ----------


def _evaluate_per_post(session: Session, experiment: Experiment) -> VerdictResult:
    """Classify every post in the variant window, split into variant/control,
    and run Mann-Whitney U on the primary metric values."""
    posts = session.scalars(
        select(MyPost).where(
            MyPost.created_at >= experiment.variant_start,
            MyPost.created_at < experiment.variant_end,
        )
    ).all()
    if not posts:
        return _insufficient("no posts in variant window yet")

    latest_insights: dict[str, MyPostInsight] = {}
    all_ins = session.scalars(
        select(MyPostInsight).order_by(MyPostInsight.fetched_at.desc())
    ).all()
    for ins in all_ins:
        latest_insights.setdefault(ins.thread_id, ins)

    # Compute per-post metric value
    variant_values: list[float] = []
    control_values: list[float] = []
    classifications: list[tuple[str, str, str]] = []  # (post_id, bucket, reason)

    for post in posts:
        cls = classify_post(session, post, experiment.category, experiment.predicate_spec or {})
        if cls is None:
            continue
        ins = latest_insights.get(post.thread_id)
        if ins is None:
            continue
        val = _post_metric_value(experiment.primary_metric, ins, session=session)
        if val is None:
            continue
        classifications.append((post.thread_id, cls.bucket, cls.reason))
        if cls.bucket == "variant":
            variant_values.append(val)
        else:
            control_values.append(val)

    # Persist classifications (replace any previous)
    for existing in session.scalars(
        select(ExperimentPostClassification).where(
            ExperimentPostClassification.experiment_id == experiment.id
        )
    ).all():
        session.delete(existing)
    for post_id, bucket, reason in classifications:
        session.add(
            ExperimentPostClassification(
                experiment_id=experiment.id,
                post_thread_id=post_id,
                bucket=bucket,
                reason=reason,
            )
        )

    if len(variant_values) < MIN_N_PER_BUCKET or len(control_values) < MIN_N_PER_BUCKET:
        return VerdictResult(
            verdict="insufficient_data",
            primary_metric_baseline=_safe_median(control_values),
            primary_metric_variant=_safe_median(variant_values),
            effect_size_pct=None,
            effect_cliffs_delta=None,
            p_value=None,
            ci_low=None,
            ci_high=None,
            variant_n=len(variant_values),
            control_n=len(control_values),
            honest_interpretation=(
                f"Not enough posts yet: {len(variant_values)} variant, "
                f"{len(control_values)} control. Need ≥{MIN_N_PER_BUCKET} in each bucket."
            ),
        )

    return _stat_verdict(
        variant_values=variant_values,
        control_values=control_values,
        metric_name=experiment.primary_metric,
    )


def _evaluate_per_window(session: Session, experiment: Experiment) -> VerdictResult:
    """For cadence/engagement experiments: compare the metric on the variant
    window against the baseline window as two aggregate values."""
    if (
        experiment.baseline_start is None
        or experiment.baseline_end is None
    ):
        return _insufficient("baseline window not set")

    variant_mv = compute_metric(
        session, experiment.primary_metric, experiment.variant_start, experiment.variant_end
    )
    baseline_mv = compute_metric(
        session,
        experiment.primary_metric,
        experiment.baseline_start,
        experiment.baseline_end,
    )

    if variant_mv.value is None or baseline_mv.value is None:
        return _insufficient(
            f"metric '{experiment.primary_metric}' not computable on both windows"
        )

    if variant_mv.n_posts < MIN_N_PER_BUCKET or baseline_mv.n_posts < MIN_N_PER_BUCKET:
        return VerdictResult(
            verdict="insufficient_data",
            primary_metric_baseline=baseline_mv.value,
            primary_metric_variant=variant_mv.value,
            effect_size_pct=_rel_delta(baseline_mv.value, variant_mv.value),
            effect_cliffs_delta=None,
            p_value=None,
            ci_low=None,
            ci_high=None,
            variant_n=variant_mv.n_posts,
            control_n=baseline_mv.n_posts,
            honest_interpretation=(
                f"Not enough posts in one of the windows: "
                f"{variant_mv.n_posts} variant, {baseline_mv.n_posts} baseline."
            ),
        )

    # For per-window we don't have individual samples to run Mann-Whitney on —
    # so we do a bootstrap on the per-post values in each window.
    variant_rows = _per_post_metric_values(
        session, experiment.primary_metric, experiment.variant_start, experiment.variant_end
    )
    baseline_rows = _per_post_metric_values(
        session, experiment.primary_metric, experiment.baseline_start, experiment.baseline_end
    )
    return _stat_verdict(
        variant_values=variant_rows,
        control_values=baseline_rows,
        metric_name=experiment.primary_metric,
    )


# ---------- the statistics ----------


def _stat_verdict(
    variant_values: list[float],
    control_values: list[float],
    metric_name: str,
) -> VerdictResult:
    variant_median = float(statistics.median(variant_values))
    control_median = float(statistics.median(control_values))
    effect_pct = _rel_delta(control_median, variant_median)

    # Mann-Whitney U (two-sided)
    try:
        u_stat, p_value = mannwhitneyu(
            variant_values, control_values, alternative="two-sided"
        )
        p_value = float(p_value)
    except Exception as exc:  # noqa: BLE001
        log.warning("mannwhitneyu failed: %s", exc)
        p_value = None

    # Cliff's delta
    cliffs = _cliffs_delta(variant_values, control_values)

    # Bootstrap 95% CI on median difference
    ci_low, ci_high = _bootstrap_median_diff_ci(variant_values, control_values)

    # Verdict logic
    direction = METRIC_META.get(metric_name, {}).get("direction", "up")
    is_improvement = (effect_pct is not None) and (
        (effect_pct > 0 and direction == "up") or (effect_pct < 0 and direction == "down")
    )
    alpha = 0.05
    if p_value is None:
        verdict = "null"
    elif p_value < alpha and is_improvement:
        verdict = "win"
    elif p_value < alpha and not is_improvement:
        verdict = "loss"
    else:
        verdict = "null"

    interp = _write_interpretation(
        metric_name=metric_name,
        variant_median=variant_median,
        control_median=control_median,
        effect_pct=effect_pct,
        cliffs=cliffs,
        p_value=p_value,
        ci_low=ci_low,
        ci_high=ci_high,
        n_variant=len(variant_values),
        n_control=len(control_values),
        verdict=verdict,
    )

    return VerdictResult(
        verdict=verdict,
        primary_metric_baseline=control_median,
        primary_metric_variant=variant_median,
        effect_size_pct=effect_pct,
        effect_cliffs_delta=cliffs,
        p_value=p_value,
        ci_low=ci_low,
        ci_high=ci_high,
        variant_n=len(variant_values),
        control_n=len(control_values),
        honest_interpretation=interp,
    )


def _cliffs_delta(a: list[float], b: list[float]) -> float:
    """Cliff's delta: (#pairs where a > b - #pairs where a < b) / (|a| * |b|)."""
    if not a or not b:
        return 0.0
    greater = 0
    less = 0
    for x in a:
        for y in b:
            if x > y:
                greater += 1
            elif x < y:
                less += 1
    total = len(a) * len(b)
    return (greater - less) / total


def _bootstrap_median_diff_ci(
    variant: list[float],
    control: list[float],
    n_resamples: int = 1000,
    seed: int = 42,
) -> tuple[float | None, float | None]:
    """Bootstrap 95% CI on (median(variant) - median(control))."""
    if len(variant) < 2 or len(control) < 2:
        return (None, None)
    rng = np.random.default_rng(seed)
    v_arr = np.asarray(variant, dtype=float)
    c_arr = np.asarray(control, dtype=float)
    diffs = np.empty(n_resamples)
    for i in range(n_resamples):
        v_sample = rng.choice(v_arr, size=len(v_arr), replace=True)
        c_sample = rng.choice(c_arr, size=len(c_arr), replace=True)
        diffs[i] = float(np.median(v_sample) - np.median(c_sample))
    ci_low = float(np.percentile(diffs, 2.5))
    ci_high = float(np.percentile(diffs, 97.5))
    return (ci_low, ci_high)


def _write_interpretation(
    *,
    metric_name: str,
    variant_median: float,
    control_median: float,
    effect_pct: float | None,
    cliffs: float | None,
    p_value: float | None,
    ci_low: float | None,
    ci_high: float | None,
    n_variant: int,
    n_control: int,
    verdict: str,
) -> str:
    """Write a plain-English interpretation. Uses Claude for flavor but always falls back."""
    label = METRIC_META.get(metric_name, {}).get("label", metric_name)
    base_fallback = (
        f"Variant median {variant_median:.4f} vs control median {control_median:.4f} "
        f"on {label} ({n_variant} variant / {n_control} control posts). "
        f"Effect {effect_pct:+.0%} (p={p_value:.3f}, Cliff's δ={cliffs:+.2f}). "
        f"Verdict: {verdict.upper()}."
    )
    # For speed we use the deterministic fallback; Claude write-up is optional.
    return base_fallback


def _post_metric_value(
    metric_name: str, ins: MyPostInsight, session: Session
) -> float | None:
    """Return a per-post value for the given metric, for use in per-post statistical tests.

    Not every metric makes sense per-post — e.g. zero_reply_fraction is definitionally
    an aggregate. For those, we fall back to a proxy (e.g. reply count) so the test
    still produces directionally meaningful numbers.
    """
    if metric_name == METRIC_REACH_RATE:
        # Need a follower count to normalize. Use the latest snapshot.
        from sqlalchemy import select

        from .models import MyAccountInsight

        acc = session.scalar(
            select(MyAccountInsight).order_by(MyAccountInsight.fetched_at.desc()).limit(1)
        )
        followers = acc.follower_count if acc else 0
        if followers == 0:
            return float(ins.views)
        return ins.views / followers

    if metric_name == METRIC_REPLY_RATE_PER_VIEW:
        if ins.views == 0:
            return 0.0
        return ins.replies / ins.views

    if metric_name == METRIC_REPLY_TO_LIKE_RATIO:
        if ins.likes == 0:
            return float(ins.replies)  # avoids div-by-zero; still directional
        return ins.replies / ins.likes

    if metric_name == METRIC_ZERO_REPLY_FRACTION:
        # Per-post proxy: 1 if zero replies, 0 otherwise (lower is better → inverted)
        return 1.0 if ins.replies == 0 else 0.0

    if metric_name == METRIC_TOP_DECILE_MULTIPLE:
        return float(ins.views)  # fall back to raw views

    if metric_name == METRIC_FOLLOWER_VELOCITY:
        return None  # not post-level

    # Default: use raw views as a directional proxy
    return float(ins.views)


def _per_post_metric_values(
    session: Session, metric_name: str, since: datetime, until: datetime
) -> list[float]:
    posts = session.scalars(
        select(MyPost).where(MyPost.created_at >= since, MyPost.created_at < until)
    ).all()
    latest: dict[str, MyPostInsight] = {}
    for ins in session.scalars(
        select(MyPostInsight).order_by(MyPostInsight.fetched_at.desc())
    ).all():
        latest.setdefault(ins.thread_id, ins)
    out: list[float] = []
    for p in posts:
        ins = latest.get(p.thread_id)
        if ins is None:
            continue
        v = _post_metric_value(metric_name, ins, session)
        if v is not None:
            out.append(v)
    return out


def _safe_median(xs: list[float]) -> float | None:
    return float(statistics.median(xs)) if xs else None


def _rel_delta(base: float | None, current: float | None) -> float | None:
    if base is None or current is None or base == 0:
        return None
    return (current - base) / abs(base)


def _insufficient(reason: str) -> VerdictResult:
    return VerdictResult(
        verdict="insufficient_data",
        primary_metric_baseline=None,
        primary_metric_variant=None,
        effect_size_pct=None,
        effect_cliffs_delta=None,
        p_value=None,
        ci_low=None,
        ci_high=None,
        variant_n=0,
        control_n=0,
        honest_interpretation=reason,
    )


def persist_verdict(session: Session, experiment: Experiment, result: VerdictResult) -> None:
    """Upsert an ExperimentVerdict row for the given experiment."""
    existing = session.get(ExperimentVerdict, experiment.id)
    if existing is None:
        existing = ExperimentVerdict(experiment_id=experiment.id)
        session.add(existing)
    existing.verdict = result.verdict
    existing.primary_metric_baseline = result.primary_metric_baseline
    existing.primary_metric_variant = result.primary_metric_variant
    existing.effect_size_pct = result.effect_size_pct
    existing.effect_cliffs_delta = result.effect_cliffs_delta
    existing.p_value = result.p_value
    existing.ci_low = result.ci_low
    existing.ci_high = result.ci_high
    existing.variant_n = result.variant_n
    existing.control_n = result.control_n
    existing.honest_interpretation = result.honest_interpretation
    existing.computed_at = datetime.now(timezone.utc)
