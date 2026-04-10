"""Per-category classifiers for experiment predicates.

Given an Experiment with a category and predicate_spec, produce a function that
maps a post row to "variant" / "control" / None (None = skip).

Per-post categories (TIMING, LENGTH, MEDIA, HOOK, TOPIC, CUSTOM) split the posts
published in the variant window into variant/control buckets.

Per-window categories (CADENCE, ENGAGEMENT) do not classify individual posts —
they compare the aggregate metric on the variant window to the aggregate metric
on the baseline window. For these, the classifier returns "variant" for every
post in the variant window and the verdict engine knows to compare windows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Literal

from sqlalchemy.orm import Session

from .models import Experiment, MyPost, PostTopic


Bucket = Literal["variant", "control"]


CATEGORY_TIMING = "TIMING"
CATEGORY_LENGTH = "LENGTH"
CATEGORY_MEDIA = "MEDIA"
CATEGORY_HOOK = "HOOK"
CATEGORY_TOPIC = "TOPIC"
CATEGORY_CADENCE = "CADENCE"
CATEGORY_ENGAGEMENT = "ENGAGEMENT"
CATEGORY_CUSTOM = "CUSTOM"

PER_POST_CATEGORIES = {
    CATEGORY_TIMING,
    CATEGORY_LENGTH,
    CATEGORY_MEDIA,
    CATEGORY_HOOK,
    CATEGORY_TOPIC,
    CATEGORY_CUSTOM,
}
PER_WINDOW_CATEGORIES = {CATEGORY_CADENCE, CATEGORY_ENGAGEMENT}


@dataclass
class Classification:
    bucket: Bucket
    reason: str


def classify_post(
    session: Session,
    post: MyPost,
    category: str,
    spec: dict[str, Any] | None,
) -> Classification | None:
    """Classify a single post under the given experiment category.

    Returns None when the category is window-level (no per-post classification)
    or when the post should be ignored (e.g. doesn't match the predicate shape).
    """
    spec = spec or {}

    if category == CATEGORY_TIMING:
        return _classify_timing(post, spec)
    if category == CATEGORY_LENGTH:
        return _classify_length(post, spec)
    if category == CATEGORY_MEDIA:
        return _classify_media(post, spec)
    if category == CATEGORY_HOOK:
        return _classify_hook(post, spec)
    if category == CATEGORY_TOPIC:
        return _classify_topic(session, post, spec)
    if category == CATEGORY_CUSTOM:
        return _classify_custom(post, spec)
    if category in PER_WINDOW_CATEGORIES:
        # For per-window categories, every post in the variant window counts as
        # "variant" — the verdict engine compares windows, not buckets.
        return Classification(bucket="variant", reason=f"{category} window-level experiment")
    return None


# ---------- per-category classifiers ----------


def _classify_timing(post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"hours": [7, 8, 19, 20, 21]} — hours (UTC) in which variant posts must land."""
    hours = spec.get("hours") or []
    if not post.created_at:
        return Classification("control", "post has no timestamp")
    h = post.created_at.hour
    if h in hours:
        return Classification("variant", f"posted at hour {h} UTC (in target window {hours})")
    return Classification("control", f"posted at hour {h} UTC (outside target window {hours})")


def _classify_length(post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"min_len": 80, "max_len": 200}"""
    min_len = spec.get("min_len", 0)
    max_len = spec.get("max_len", 10_000)
    n = len(post.text or "")
    if min_len <= n <= max_len:
        return Classification("variant", f"length {n} chars (in [{min_len},{max_len}])")
    return Classification("control", f"length {n} chars (outside [{min_len},{max_len}])")


def _classify_media(post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"media_types": ["IMAGE", "CAROUSEL_ALBUM"]}"""
    allowed = set(spec.get("media_types") or [])
    mt = post.media_type or "TEXT_POST"
    if mt in allowed:
        return Classification("variant", f"media_type={mt} in {sorted(allowed)}")
    return Classification("control", f"media_type={mt} not in {sorted(allowed)}")


def _classify_hook(post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"prefixes": ["you vs me", "hot take:", "nobody talks about"]} OR {"regex": "^(you|me):"}"""
    text = (post.text or "").lower().strip()
    prefixes = [p.lower() for p in spec.get("prefixes") or []]
    regex = spec.get("regex")
    matched = False
    why = ""
    for p in prefixes:
        if text.startswith(p):
            matched = True
            why = f"starts with {p!r}"
            break
    if not matched and regex:
        try:
            if re.search(regex, text, flags=re.IGNORECASE):
                matched = True
                why = f"matched regex {regex!r}"
        except re.error:
            pass
    if matched:
        return Classification("variant", why)
    return Classification("control", f"did not match any hook pattern")


def _classify_topic(session: Session, post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"topic_id": 3} or {"topic_label": "AI models & Anthropic commentary"}"""
    from sqlalchemy import select

    from .models import Topic

    topic_id = spec.get("topic_id")
    label = spec.get("topic_label")
    if topic_id is None and label:
        t = session.scalar(select(Topic).where(Topic.label == label))
        topic_id = t.id if t else None
    if topic_id is None:
        return Classification("control", "topic spec missing")
    exists = session.scalar(
        select(PostTopic).where(
            PostTopic.post_thread_id == post.thread_id,
            PostTopic.topic_id == topic_id,
        )
    )
    if exists is not None:
        return Classification("variant", f"linked to topic #{topic_id}")
    return Classification("control", f"not linked to topic #{topic_id}")


def _classify_custom(post: MyPost, spec: dict[str, Any]) -> Classification:
    """spec: {"variant_post_ids": ["id1", "id2"], "control_post_ids": ["id3"]}

    Manual tagging — used when the category is CUSTOM and the user tagged posts
    via the experiment detail UI.
    """
    variants = set(spec.get("variant_post_ids") or [])
    controls = set(spec.get("control_post_ids") or [])
    if post.thread_id in variants:
        return Classification("variant", "manually tagged as variant")
    if post.thread_id in controls:
        return Classification("control", "manually tagged as control")
    # Not tagged → skip by returning control with a neutral reason
    return Classification("control", "not manually tagged")
