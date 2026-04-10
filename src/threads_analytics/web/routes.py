"""Dashboard routes (v2 — scientific experiment platform)."""

from __future__ import annotations

import threading
from datetime import datetime, timezone

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select

from ..config import get_settings
from ..db import session_scope
from ..experiments import (
    abandon_experiment,
    create_experiment,
    end_experiment,
    evaluate_now,
    list_experiments,
    personal_category_performance,
    start_experiment,
)
from ..metrics import METRIC_META, METRIC_ORDER, compute_ground_truth
from ..models import (
    AffinityCreator,
    AffinityPost,
    AlgorithmInference,
    Experiment,
    ExperimentPostClassification,
    ExperimentVerdict,
    MyAccountInsight,
    MyPost,
    MyPostInsight,
    NoteworthyPost,
    Profile,
    PublicPerception,
    Run,
    Topic,
    YouProfile,
)
from ..pipeline import run_full_cycle

_run_lock = threading.Lock()
_last_run_summary: dict = {}


# ---------- helpers ----------


def _profile_payload(session) -> dict | None:
    profile = session.scalar(select(Profile).limit(1))
    if profile is None:
        return None
    return {
        "username": profile.username,
        "biography": profile.biography,
        "profile_picture_url": profile.profile_picture_url,
        "updated_at": profile.updated_at,
    }


def _recent_runs(session, n: int = 5) -> list[dict]:
    runs = session.scalars(select(Run).order_by(desc(Run.started_at)).limit(n)).all()
    return [
        {
            "id": r.id,
            "started_at": r.started_at,
            "status": r.status,
            "queries_used": r.keyword_search_queries_used,
        }
        for r in runs
    ]


def _exp_summary(exp: Experiment, session) -> dict:
    v = session.get(ExperimentVerdict, exp.id) if exp.status in ("completed", "active") else None
    variant_n = control_n = 0
    if exp.id is not None:
        variant_n = (
            session.scalar(
                select(ExperimentPostClassification)
                .where(
                    ExperimentPostClassification.experiment_id == exp.id,
                    ExperimentPostClassification.bucket == "variant",
                )
                .limit(1)
            )
            is not None
            and session.query(ExperimentPostClassification)
            .filter(
                ExperimentPostClassification.experiment_id == exp.id,
                ExperimentPostClassification.bucket == "variant",
            )
            .count()
            or 0
        )
        control_n = (
            session.query(ExperimentPostClassification)
            .filter(
                ExperimentPostClassification.experiment_id == exp.id,
                ExperimentPostClassification.bucket == "control",
            )
            .count()
        )
    return {
        "id": exp.id,
        "title": exp.title,
        "hypothesis": exp.hypothesis,
        "category": exp.category,
        "status": exp.status,
        "source": exp.source,
        "primary_metric": exp.primary_metric,
        "primary_metric_label": METRIC_META.get(exp.primary_metric, {}).get(
            "label", exp.primary_metric
        ),
        "predicate_spec": exp.predicate_spec,
        "target_delta_pct": exp.target_delta_pct,
        "notes": exp.notes,
        "created_at": exp.created_at,
        "started_at": exp.started_at,
        "ended_at": exp.ended_at,
        "variant_start": exp.variant_start,
        "variant_end": exp.variant_end,
        "baseline_start": exp.baseline_start,
        "baseline_end": exp.baseline_end,
        "variant_n": variant_n,
        "control_n": control_n,
        "verdict": (
            {
                "verdict": v.verdict,
                "primary_metric_baseline": v.primary_metric_baseline,
                "primary_metric_variant": v.primary_metric_variant,
                "effect_size_pct": v.effect_size_pct,
                "effect_cliffs_delta": v.effect_cliffs_delta,
                "p_value": v.p_value,
                "ci_low": v.ci_low,
                "ci_high": v.ci_high,
                "variant_n": v.variant_n,
                "control_n": v.control_n,
                "honest_interpretation": v.honest_interpretation,
                "computed_at": v.computed_at,
            }
            if v
            else None
        ),
    }


def _format_metric_value(metric_name: str, value: float | None) -> str:
    if value is None:
        return "—"
    fmt = METRIC_META.get(metric_name, {}).get("format", "raw")
    if fmt == "pct":
        return f"{value * 100:.1f}%"
    if fmt == "multiple":
        return f"{value:.1f}×"
    return f"{value:.2f}"


def _format_delta(delta: float | None) -> dict:
    if delta is None:
        return {"label": "—", "class": "flat"}
    if abs(delta) < 0.03:
        return {"label": f"{delta:+.0%}", "class": "flat"}
    return {"label": f"{delta:+.0%}", "class": "pos" if delta > 0 else "neg"}


def _ground_truth_payload(session) -> dict:
    panel = compute_ground_truth(session)
    cards = []
    regressions: list[tuple[str, float]] = []
    improvements: list[tuple[str, float]] = []
    for name in METRIC_ORDER:
        mv = panel.metrics[name]
        base = panel.baselines[name]
        delta = panel.deltas[name]
        meta = METRIC_META[name]
        direction = meta["direction"]
        good = False
        if delta is not None and abs(delta) >= 0.03:
            good = (delta > 0 and direction == "up") or (
                delta < 0 and direction == "down"
            )
            if good:
                improvements.append((name, abs(delta)))
            else:
                regressions.append((name, abs(delta)))
        delta_obj = _format_delta(delta)
        if delta is None or abs(delta) < 0.03:
            delta_obj["class"] = "flat"
        else:
            delta_obj["class"] = "pos" if good else "neg"
        cards.append(
            {
                "name": name,
                "label": meta["label"],
                "description": meta["description"],
                "value": _format_metric_value(name, mv.value),
                "raw_value": mv.value,
                "baseline": _format_metric_value(name, base.value),
                "delta": delta_obj,
                "n_posts": mv.n_posts,
                "sparkline": [p.value for p in panel.trend[name]],
                "direction": direction,
            }
        )

    # Hero card selection: biggest regression if any, else biggest improvement,
    # else the single most "informative" metric (for first-run / no-baseline
    # case) — we default to zero_reply_fraction since it's the penalty signal.
    hero_name: str | None = None
    hero_tone: str = "neutral"
    if regressions:
        regressions.sort(key=lambda x: x[1], reverse=True)
        hero_name = regressions[0][0]
        hero_tone = "negative"
    elif improvements:
        improvements.sort(key=lambda x: x[1], reverse=True)
        hero_name = improvements[0][0]
        hero_tone = "positive"
    else:
        # First-run / no deltas: show the reality check
        hero_name = "zero_reply_fraction"
        hero_tone = "neutral"

    # Verdict tone → card color class
    verdict_color = {
        "negative": "card-hero-coral",
        "positive": "card-hero-green",
        "neutral": "card-hero-yellow",
    }[hero_tone]
    hero_metric_color = {
        "negative": "card-hero-pink",
        "positive": "card-hero-green",
        "neutral": "card-hero-pink",
    }[hero_tone]

    return {
        "cards": cards,
        "headline": panel.verdict_headline,
        "computed_at": panel.computed_at,
        "window_days": panel.window_days,
        "hero_metric_name": hero_name,
        "hero_tone": hero_tone,
        "verdict_color": verdict_color,
        "hero_metric_color": hero_metric_color,
    }


# ---------- router ----------


def build_router(templates: Jinja2Templates) -> APIRouter:
    router = APIRouter()

    @router.get("/", response_class=HTMLResponse)
    def ground_truth(request: Request) -> HTMLResponse:
        settings = get_settings()
        with session_scope() as session:
            profile = _profile_payload(session)
            panel = _ground_truth_payload(session)
            active_count = session.query(Experiment).filter(
                Experiment.status == "active"
            ).count()
            proposed_count = session.query(Experiment).filter(
                Experiment.status == "proposed"
            ).count()
            runs = _recent_runs(session)
        return templates.TemplateResponse(
            request,
            "ground_truth.html",
            {
                "handle": settings.threads_handle,
                "profile": profile,
                "panel": panel,
                "active_count": active_count,
                "proposed_count": proposed_count,
                "runs": runs,
            },
        )

    # ---------- experiments ----------

    @router.get("/experiments", response_class=HTMLResponse)
    def experiments_index(request: Request) -> HTMLResponse:
        with session_scope() as session:
            active = [
                _exp_summary(e, session) for e in list_experiments(session, "active")
            ]
            completed = [
                _exp_summary(e, session) for e in list_experiments(session, "completed")
            ]
            proposed = [
                _exp_summary(e, session) for e in list_experiments(session, "proposed")
            ]
            abandoned = [
                _exp_summary(e, session) for e in list_experiments(session, "abandoned", limit=10)
            ]
            track = personal_category_performance(session)
            track_payload = {
                cat: {
                    "total": cs.total,
                    "wins": cs.wins,
                    "losses": cs.losses,
                    "nulls": cs.nulls,
                    "insufficient": cs.insufficient,
                    "win_rate": cs.win_rate(),
                    "avg_win_effect_pct": cs.avg_win_effect_pct,
                }
                for cat, cs in track.items()
            }
        return templates.TemplateResponse(
            request,
            "experiments.html",
            {
                "active": active,
                "completed": completed,
                "proposed": proposed,  # rendered as the top carousel (Suggestions)
                "abandoned": abandoned,
                "track_record": track_payload,
            },
        )

    @router.post("/experiments/{exp_id}/delete")
    def experiment_delete(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            # Also remove classifications + verdict; cascade on the model handles it
            session.delete(exp)
        return RedirectResponse("/experiments", status_code=303)

    @router.get("/experiments/new", response_class=HTMLResponse)
    def experiment_new_form(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "experiment_new.html",
            {
                "metric_order": METRIC_ORDER,
                "metric_meta": METRIC_META,
            },
        )

    @router.post("/experiments/new")
    def experiment_new_submit(
        title: str = Form(...),
        hypothesis: str = Form(...),
        category: str = Form(...),
        primary_metric: str = Form(...),
        predicate_json: str = Form(""),
        variant_window_days: int = Form(14),
        target_delta_pct: str = Form(""),
        start_now: str = Form(""),
    ) -> RedirectResponse:
        import json as _json

        spec: dict = {}
        if predicate_json.strip():
            try:
                spec = _json.loads(predicate_json)
            except _json.JSONDecodeError:
                raise HTTPException(400, "predicate_json is not valid JSON")

        tdp: float | None = None
        if target_delta_pct.strip():
            try:
                tdp = float(target_delta_pct)
            except ValueError:
                pass

        with session_scope() as session:
            exp = create_experiment(
                session,
                title=title,
                hypothesis=hypothesis,
                category=category.upper(),
                predicate_spec=spec,
                primary_metric=primary_metric,
                source="user_defined",
                target_delta_pct=tdp,
                variant_window_days=variant_window_days,
                status="proposed",
            )
            if start_now == "on":
                start_experiment(session, exp)
            exp_id = exp.id
        return RedirectResponse(f"/experiments/{exp_id}", status_code=303)

    @router.get("/experiments/{exp_id}", response_class=HTMLResponse)
    def experiment_detail(request: Request, exp_id: int) -> HTMLResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            payload = _exp_summary(exp, session)
            # Load per-post classifications (variant + control)
            classifications = session.scalars(
                select(ExperimentPostClassification).where(
                    ExperimentPostClassification.experiment_id == exp_id
                )
            ).all()
            class_payload = []
            for c in classifications[:40]:
                p = session.get(MyPost, c.post_thread_id)
                class_payload.append(
                    {
                        "thread_id": c.post_thread_id,
                        "bucket": c.bucket,
                        "reason": c.reason,
                        "text": (p.text or "")[:200] if p else "",
                        "permalink": p.permalink if p else None,
                        "created_at": p.created_at if p else None,
                    }
                )
        return templates.TemplateResponse(
            request,
            "experiment_detail.html",
            {
                "exp": payload,
                "classifications": class_payload,
                "metric_meta": METRIC_META,
            },
        )

    @router.post("/experiments/{exp_id}/start")
    def experiment_start(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            start_experiment(session, exp)
        return RedirectResponse(f"/experiments/{exp_id}", status_code=303)

    @router.post("/experiments/{exp_id}/evaluate")
    def experiment_evaluate(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            evaluate_now(session, exp)
        return RedirectResponse(f"/experiments/{exp_id}", status_code=303)

    @router.post("/experiments/{exp_id}/end")
    def experiment_end(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            end_experiment(session, exp, final_status="completed")
        return RedirectResponse(f"/experiments/{exp_id}", status_code=303)

    @router.post("/experiments/{exp_id}/abandon")
    def experiment_abandon(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None:
                raise HTTPException(404, "experiment not found")
            abandon_experiment(session, exp)
        return RedirectResponse("/experiments", status_code=303)

    # ---------- suggestions (merged into /experiments) ----------

    @router.get("/suggestions")
    def suggestions_redirect() -> RedirectResponse:
        return RedirectResponse("/experiments#suggestions", status_code=301)

    @router.post("/suggestions/{exp_id}/run")
    def suggestions_run(exp_id: int) -> RedirectResponse:
        with session_scope() as session:
            exp = session.get(Experiment, exp_id)
            if exp is None or exp.status != "proposed":
                raise HTTPException(400, "experiment not in proposed state")
            start_experiment(session, exp)
        return RedirectResponse(f"/experiments/{exp_id}", status_code=303)

    # ---------- secondary pages (kept) ----------

    @router.get("/perception", response_class=HTMLResponse)
    def perception(request: Request) -> HTMLResponse:
        with session_scope() as session:
            pp = session.scalar(
                select(PublicPerception).order_by(desc(PublicPerception.created_at)).limit(1)
            )
            payload = None
            if pp is not None:
                raw = pp.raw_json or {}
                payload = {
                    "thin_slice": raw.get("thinSliceJudgment") or pp.one_sentence_cold,
                    "big_five": raw.get("bigFive") or {},
                    "cue_clarity": raw.get("cueClarity") or {},
                    "misread_risks": raw.get("misreadRisks") or [],
                    "signal_quality": raw.get("profileSignalQuality") or {},
                    "highest_leverage_fix": raw.get("highestLeverageFix") or {},
                    "follow_triggers": pp.follow_triggers or [],
                    "bounce_reasons": pp.bounce_reasons or [],
                    "created_at": pp.created_at,
                }
        return templates.TemplateResponse(
            request, "perception.html", {"perception": payload}
        )

    @router.get("/algorithm", response_class=HTMLResponse)
    def algorithm(request: Request) -> HTMLResponse:
        with session_scope() as session:
            ai = session.scalar(
                select(AlgorithmInference).order_by(desc(AlgorithmInference.created_at)).limit(1)
            )
            payload = None
            if ai is not None:
                payload = {
                    "narrative_diagnosis": ai.narrative_diagnosis or ai.summary,
                    "signals": [
                        ("Reply velocity (first 30-60 min)", ai.reply_velocity_signal or {}),
                        ("Conversation depth (replies vs likes)", ai.conversation_depth_signal or {}),
                        ("Self-reply behavior (author → commenter)", ai.self_reply_signal or {}),
                        ("Zero-reply penalty loop", ai.zero_reply_penalty_signal or {}),
                        ("Format diversity (text vs image/video)", ai.format_diversity_signal or {}),
                        ("Posting cadence", ai.posting_cadence_signal or {}),
                    ],
                    "inferred_weights": ai.inferred_signal_weights or {},
                    "highest_roi_lever": ai.highest_roi_lever or {},
                    # v1 legacy fields
                    "legacy_penalties": ai.penalties or [],
                    "legacy_boosts": ai.boosts or [],
                    "legacy_levers": ai.levers or [],
                    "created_at": ai.created_at,
                }
        return templates.TemplateResponse(
            request, "algorithm.html", {"algorithm": payload}
        )

    @router.get("/you", response_class=HTMLResponse)
    def you_route(request: Request) -> HTMLResponse:
        with session_scope() as session:
            yp = session.scalar(
                select(YouProfile).order_by(desc(YouProfile.created_at)).limit(1)
            )
            payload = None
            if yp is not None:
                payload = {
                    "core_identity": yp.core_identity,
                    "distinctive_voice_traits": yp.distinctive_voice_traits or [],
                    "unique_topic_crossovers": yp.unique_topic_crossovers or [],
                    "stylistic_signatures": yp.stylistic_signatures or [],
                    "posts_that_sound_most_like_you": yp.posts_that_sound_most_like_you or [],
                    "protect_list": yp.protect_list or [],
                    "double_down_list": yp.double_down_list or [],
                    "homogenization_risks": yp.homogenization_risks or [],
                    "created_at": yp.created_at,
                }
        return templates.TemplateResponse(request, "you.html", {"you": payload})

    @router.get("/posts", response_class=HTMLResponse)
    def posts(request: Request) -> HTMLResponse:
        with session_scope() as session:
            # Noteworthy posts with Claude commentary (replaces the old posts table)
            rows = session.scalars(
                select(NoteworthyPost).order_by(desc(NoteworthyPost.created_at))
            ).all()
            payload = []
            for np_row in rows:
                post = session.get(MyPost, np_row.post_thread_id)
                payload.append(
                    {
                        "category": np_row.category,
                        "remarkable_metric": np_row.remarkable_metric,
                        "remarkable_value": np_row.remarkable_value,
                        "ratio_vs_median": np_row.ratio_vs_median,
                        "commentary": np_row.claude_commentary,
                        "algo_hypothesis": np_row.algo_hypothesis,
                        "created_at": np_row.created_at,
                        "text": (post.text or "")[:400] if post else "",
                        "permalink": post.permalink if post else None,
                        "posted_at": post.created_at if post else None,
                        "media_type": post.media_type if post else None,
                        "likes": None,
                        "replies": None,
                        "views": None,
                    }
                )
                # enrich with latest insights
                if post:
                    ins = session.scalar(
                        select(MyPostInsight)
                        .where(MyPostInsight.thread_id == post.thread_id)
                        .order_by(desc(MyPostInsight.fetched_at))
                        .limit(1)
                    )
                    if ins:
                        payload[-1]["likes"] = ins.likes
                        payload[-1]["replies"] = ins.replies
                        payload[-1]["views"] = ins.views
        return templates.TemplateResponse(
            request, "posts.html", {"noteworthy": payload}
        )

    # ---------- sunset redirects ----------

    @router.get("/recommendations")
    def recommendations_redirect() -> RedirectResponse:
        return RedirectResponse("/experiments#suggestions", status_code=301)

    @router.get("/learning")
    def learning_redirect() -> RedirectResponse:
        return RedirectResponse("/experiments", status_code=301)

    @router.get("/affinity", response_class=HTMLResponse)
    def affinity(request: Request) -> HTMLResponse:
        # Still reachable, but shows a banner explaining the lock.
        with session_scope() as session:
            creators = session.scalars(
                select(AffinityCreator)
                .order_by(AffinityCreator.engagement_score.desc())
                .limit(50)
            ).all()
            payload = []
            for c in creators:
                posts_ = session.scalars(
                    select(AffinityPost)
                    .where(AffinityPost.creator_id == c.id)
                    .order_by(AffinityPost.likes.desc())
                    .limit(3)
                ).all()
                payload.append(
                    {
                        "handle": c.handle,
                        "engagement_score": round(c.engagement_score, 2),
                        "last_refreshed_at": c.last_refreshed_at,
                        "top_posts": [
                            {
                                "text": (p.text or "")[:240],
                                "likes": p.likes,
                                "replies": p.replies,
                            }
                            for p in posts_
                        ],
                    }
                )
        return templates.TemplateResponse(
            request, "affinity.html", {"creators": payload, "locked": True}
        )

    @router.get("/topics", response_class=HTMLResponse)
    def topics(request: Request) -> HTMLResponse:
        with session_scope() as session:
            rows = session.scalars(select(Topic).order_by(Topic.extracted_at.desc())).all()
            topic_payload = [
                {
                    "id": t.id,
                    "label": t.label,
                    "description": t.description,
                    "last_searched_at": t.last_searched_at,
                }
                for t in rows
            ]
        return templates.TemplateResponse(
            request, "topics.html", {"topics": topic_payload}
        )

    # ---------- pipeline trigger ----------

    @router.post("/run")
    def trigger_run() -> JSONResponse:
        if not _run_lock.acquire(blocking=False):
            return JSONResponse({"status": "already_running"}, status_code=409)

        def _bg() -> None:
            global _last_run_summary
            try:
                _last_run_summary = run_full_cycle()
            finally:
                _run_lock.release()

        threading.Thread(target=_bg, daemon=True).start()
        return JSONResponse({"status": "started"})

    @router.get("/run/status")
    def run_status() -> JSONResponse:
        return JSONResponse(
            {
                "running": _run_lock.locked(),
                "last_summary": _last_run_summary,
            }
        )

    return router


def _latest_insights_with_posts(session, limit: int = 20) -> list[dict]:
    posts = session.scalars(select(MyPost).order_by(desc(MyPost.created_at))).all()
    all_insights = session.scalars(
        select(MyPostInsight).order_by(MyPostInsight.fetched_at.desc())
    ).all()
    latest: dict[str, MyPostInsight] = {}
    for ins in all_insights:
        latest.setdefault(ins.thread_id, ins)
    rows = []
    for p in posts:
        ins = latest.get(p.thread_id)
        rows.append(
            {
                "thread_id": p.thread_id,
                "text": (p.text or "")[:240],
                "permalink": p.permalink,
                "created_at": p.created_at,
                "views": ins.views if ins else 0,
                "likes": ins.likes if ins else 0,
                "replies": ins.replies if ins else 0,
            }
        )
        if len(rows) >= limit:
            break
    rows.sort(key=lambda r: r["likes"], reverse=True)
    return rows
