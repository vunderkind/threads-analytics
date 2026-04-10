"""SQLAlchemy ORM models — the schema is the project's backbone."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Profile(Base):
    """Snapshot of the user's Threads profile — name, bio, picture.

    One row per user (keyed on user_id). Updated on every run.
    """

    __tablename__ = "profiles"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    username: Mapped[str] = mapped_column(String(128))
    biography: Mapped[str | None] = mapped_column(Text, nullable=True)
    profile_picture_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="running")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    keyword_search_queries_used: Mapped[int] = mapped_column(Integer, default=0)

    account_insights: Mapped[MyAccountInsight | None] = relationship(
        back_populates="run", uselist=False
    )
    recommendations: Mapped[list[Recommendation]] = relationship(back_populates="run")


class MyPost(Base):
    __tablename__ = "my_posts"

    thread_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    text: Mapped[str] = mapped_column(Text, default="")
    media_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    media_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    permalink: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime)
    first_seen_run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))

    insights: Mapped[list[MyPostInsight]] = relationship(back_populates="post")


class MyReply(Base):
    """A reply the user made on someone else's post (their Replies tab on Threads)."""

    __tablename__ = "my_replies"

    thread_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    text: Mapped[str] = mapped_column(Text, default="")
    media_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    permalink: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime)
    root_post_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_seen_run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))


class MyPostInsight(Base):
    __tablename__ = "my_post_insights"
    __table_args__ = (UniqueConstraint("thread_id", "run_id", name="uq_post_run"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(ForeignKey("my_posts.thread_id"))
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    views: Mapped[int] = mapped_column(Integer, default=0)
    likes: Mapped[int] = mapped_column(Integer, default=0)
    replies: Mapped[int] = mapped_column(Integer, default=0)
    reposts: Mapped[int] = mapped_column(Integer, default=0)
    quotes: Mapped[int] = mapped_column(Integer, default=0)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    post: Mapped[MyPost] = relationship(back_populates="insights")


class MyAccountInsight(Base):
    __tablename__ = "my_account_insights"

    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    follower_count: Mapped[int] = mapped_column(Integer, default=0)
    views: Mapped[int] = mapped_column(Integer, default=0)
    likes: Mapped[int] = mapped_column(Integer, default=0)
    replies: Mapped[int] = mapped_column(Integer, default=0)
    reposts: Mapped[int] = mapped_column(Integer, default=0)
    quotes: Mapped[int] = mapped_column(Integer, default=0)
    demographics_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    run: Mapped[Run] = relationship(back_populates="account_insights")


class Topic(Base):
    __tablename__ = "topics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(128), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    extracted_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    last_searched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class PostTopic(Base):
    __tablename__ = "post_topics"

    post_thread_id: Mapped[str] = mapped_column(ForeignKey("my_posts.thread_id"), primary_key=True)
    topic_id: Mapped[int] = mapped_column(ForeignKey("topics.id"), primary_key=True)
    confidence: Mapped[float] = mapped_column(Float, default=1.0)


class AffinityCreator(Base):
    __tablename__ = "affinity_creators"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    handle: Mapped[str] = mapped_column(String(128), unique=True)
    user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    discovered_via_topic_id: Mapped[int | None] = mapped_column(
        ForeignKey("topics.id"), nullable=True
    )
    engagement_score: Mapped[float] = mapped_column(Float, default=0.0)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    last_refreshed_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    posts: Mapped[list[AffinityPost]] = relationship(back_populates="creator")


class AffinityPost(Base):
    __tablename__ = "affinity_posts"

    thread_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    creator_id: Mapped[int] = mapped_column(ForeignKey("affinity_creators.id"))
    text: Mapped[str] = mapped_column(Text, default="")
    likes: Mapped[int] = mapped_column(Integer, default=0)
    replies: Mapped[int] = mapped_column(Integer, default=0)
    reposts: Mapped[int] = mapped_column(Integer, default=0)
    quotes: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    creator: Mapped[AffinityCreator] = relationship(back_populates="posts")


class Recommendation(Base):
    __tablename__ = "recommendations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    rank: Mapped[int] = mapped_column(Integer, default=0)
    category: Mapped[str] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(256))
    body: Mapped[str] = mapped_column(Text)
    evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="pending")  # pending|applied|dismissed
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    run: Mapped[Run] = relationship(back_populates="recommendations")
    outcomes: Mapped[list[RecommendationOutcome]] = relationship(back_populates="recommendation")


class PublicPerception(Base):
    """Claude-synthesized growth-focused outsider view.

    v2 reframe: every field is about WHETHER a stranger has a reason to follow
    and HOW to give them one. Old "firstGlance/profile/…/whoWillLike/dislike"
    fields are kept as nullable columns so old data still displays.
    """

    __tablename__ = "public_perceptions"

    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), primary_key=True)

    # v2 growth-focused fields
    one_sentence_cold: Mapped[str] = mapped_column(Text, default="")
    first_impression: Mapped[str] = mapped_column(Text, default="")
    positioning_clarity: Mapped[str] = mapped_column(Text, default="")
    stickiness: Mapped[str] = mapped_column(Text, default="")
    follow_triggers: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # list[str]
    bounce_reasons: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # list[str]
    conversation_readiness: Mapped[str] = mapped_column(Text, default="")
    growth_blockers: Mapped[dict | None] = mapped_column(JSON, nullable=True)  # list[str]

    # Legacy v1 fields (kept nullable so historical rows still render)
    first_glance: Mapped[str] = mapped_column(Text, default="")
    profile_impression: Mapped[str] = mapped_column(Text, default="")
    first_five_posts_impression: Mapped[str] = mapped_column(Text, default="")
    first_five_images_impression: Mapped[str] = mapped_column(Text, default="")
    first_five_replies_impression: Mapped[str] = mapped_column(Text, default="")
    overall_impression: Mapped[str] = mapped_column(Text, default="")
    who_will_like: Mapped[str] = mapped_column(Text, default="")
    who_will_dislike: Mapped[str] = mapped_column(Text, default="")

    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class AlgorithmInference(Base):
    """v2: Research-grounded inference of how Threads's ranker is treating this account.

    Schema mirrors the six ranking factors we know matter from Threads/X research:
    reply velocity, conversation depth, self-reply behavior (the +75 weight on
    author-reply-to-reply), zero-reply penalty, format diversity, posting cadence.

    Old v1 columns (summary/penalties/boosts/signal_profile/levers) are kept so
    historical rows still render.
    """

    __tablename__ = "algorithm_inferences"

    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), primary_key=True)

    # v1 fields (kept for backward compat)
    summary: Mapped[str] = mapped_column(Text, default="")
    penalties: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    boosts: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    signal_profile: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    levers: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # v2 research-grounded fields
    narrative_diagnosis: Mapped[str] = mapped_column(Text, default="")
    reply_velocity_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # {rating: penalized|neutral|boosted, evidence: str, inferred_impact: str}
    conversation_depth_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    self_reply_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    zero_reply_penalty_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    format_diversity_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    posting_cadence_signal: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    highest_roi_lever: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # {title, mechanism, expected_impact, cites_research: "X heavy ranker +75 weight…"}
    inferred_signal_weights: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # {reply_velocity: 0.0-1.0, conversation_depth: ..., ...} — Claude's guess

    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class YouProfile(Base):
    """Anti-homogenization guardrail.

    Claude extracts the signals that are uniquely this user's — voice, topics,
    stylistic quirks, crossover interests — so the experimentation loop doesn't
    grind them into generic reply-farm slop. Every run refreshes this.
    """

    __tablename__ = "you_profiles"

    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), primary_key=True)
    core_identity: Mapped[str] = mapped_column(Text, default="")
    # One-paragraph "this person in their own frame, not the algo's"
    distinctive_voice_traits: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of {trait, evidence, example}
    unique_topic_crossovers: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of {topic, why_unusual, example}
    stylistic_signatures: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of {signature, evidence}
    posts_that_sound_most_like_you: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of {post_id, text, why}
    protect_list: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of strings — things to NEVER optimize away
    double_down_list: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of strings — things to make MORE of (uniquely yours AND effective)
    homogenization_risks: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    # list of {risk, if_you_do_this_you_lose}
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class NoteworthyPost(Base):
    """Posts that stood out — outliers in reach/engagement/pattern — with Claude's
    algo-aware commentary on WHY they were remarkable."""

    __tablename__ = "noteworthy_posts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    post_thread_id: Mapped[str] = mapped_column(ForeignKey("my_posts.thread_id"))
    category: Mapped[str] = mapped_column(String(64))  # breakout | conversation_starter | reach_outlier | reply_velocity_win | flop | pattern_anomaly
    remarkable_metric: Mapped[str] = mapped_column(String(64))
    remarkable_value: Mapped[float] = mapped_column(Float)
    ratio_vs_median: Mapped[float | None] = mapped_column(Float, nullable=True)
    claude_commentary: Mapped[str] = mapped_column(Text, default="")
    algo_hypothesis: Mapped[str] = mapped_column(Text, default="")
    # The inferred mechanism: "got early reply velocity from audience X" etc.
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)


class Experiment(Base):
    """A scientific experiment: baseline → intervention → statistical verdict."""

    __tablename__ = "experiments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(256))
    hypothesis: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(32))  # TIMING|LENGTH|MEDIA|HOOK|TOPIC|CADENCE|ENGAGEMENT|CUSTOM
    predicate_spec: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    primary_metric: Mapped[str] = mapped_column(String(64))
    secondary_metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    baseline_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    baseline_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    variant_start: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    variant_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    target_delta_pct: Mapped[float | None] = mapped_column(Float, nullable=True)

    status: Mapped[str] = mapped_column(String(32), default="proposed")  # proposed|active|completed|abandoned
    source: Mapped[str] = mapped_column(String(32), default="user_defined")  # user_defined|suggested_by_claude
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    classifications: Mapped[list[ExperimentPostClassification]] = relationship(
        back_populates="experiment", cascade="all, delete-orphan"
    )
    verdict: Mapped[ExperimentVerdict | None] = relationship(
        back_populates="experiment", uselist=False, cascade="all, delete-orphan"
    )


class ExperimentPostClassification(Base):
    """Records how each post during an experiment's variant window was classified."""

    __tablename__ = "experiment_post_classifications"

    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiments.id"), primary_key=True)
    post_thread_id: Mapped[str] = mapped_column(
        ForeignKey("my_posts.thread_id"), primary_key=True
    )
    bucket: Mapped[str] = mapped_column(String(32))  # variant|control|outside_window
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    classified_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    experiment: Mapped[Experiment] = relationship(back_populates="classifications")


class ExperimentVerdict(Base):
    """Statistical verdict on an experiment — populated after variant_end or on demand."""

    __tablename__ = "experiment_verdicts"

    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiments.id"), primary_key=True)
    verdict: Mapped[str] = mapped_column(String(32))  # win|loss|null|insufficient_data
    primary_metric_baseline: Mapped[float | None] = mapped_column(Float, nullable=True)
    primary_metric_variant: Mapped[float | None] = mapped_column(Float, nullable=True)
    effect_size_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    effect_cliffs_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    ci_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    ci_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    variant_n: Mapped[int] = mapped_column(Integer, default=0)
    control_n: Mapped[int] = mapped_column(Integer, default=0)
    honest_interpretation: Mapped[str] = mapped_column(Text, default="")
    raw_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    experiment: Mapped[Experiment] = relationship(back_populates="verdict")


class RecommendationOutcome(Base):
    __tablename__ = "recommendation_outcomes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    recommendation_id: Mapped[int] = mapped_column(ForeignKey("recommendations.id"))
    checked_at_run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"))
    follower_delta: Mapped[int] = mapped_column(Integer, default=0)
    engagement_delta: Mapped[float] = mapped_column(Float, default=0.0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)

    recommendation: Mapped[Recommendation] = relationship(back_populates="outcomes")
