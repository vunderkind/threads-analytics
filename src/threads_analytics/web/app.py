"""FastAPI app factory."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..db import init_db
from .routes import build_router

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


def create_app() -> FastAPI:
    init_db()
    app = FastAPI(title="threads-analytics")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.state.templates = templates
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(build_router(templates))
    return app
