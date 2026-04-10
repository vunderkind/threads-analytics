"""Environment-backed configuration.

The .env file is the source of truth for this local-first project: values there
override any pre-existing shell environment variables. This matters because users
often have an empty `ANTHROPIC_API_KEY=` exported in their shell, which would
otherwise silently shadow the real key in .env.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import dotenv_values
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env values and set them ONLY when the corresponding env var is unset or
# empty. This lets .env fill in missing values (e.g. a shell that exports
# ANTHROPIC_API_KEY="" which would otherwise shadow the real key) while still
# allowing explicit env vars (e.g. pytest monkeypatching DATABASE_URL) to win.
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
if _ENV_PATH.exists():
    for _k, _v in dotenv_values(_ENV_PATH).items():
        if _v is None:
            continue
        current = os.environ.get(_k)
        if current is None or current == "":
            os.environ[_k] = _v


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Meta / Threads app
    meta_app_id: str = ""
    meta_app_secret: str = ""
    meta_redirect_uri: str = "https://localhost/"

    # User token (populated by setup_token.py)
    threads_access_token: str = ""
    threads_user_id: str = ""
    threads_handle: str = ""

    # Anthropic
    anthropic_api_key: str = ""
    claude_recommender_model: str = "claude-opus-4-6"
    claude_topic_model: str = "claude-sonnet-4-6"

    # Storage
    database_url: str = "sqlite:///data/threads.db"

    # Rate limit budgets
    keyword_search_budget_per_run: int = 30
    keyword_search_rolling_budget: int = 400


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_sqlite_url(url: str) -> str:
    """Rewrite a relative sqlite:/// URL to an absolute path rooted at PROJECT_ROOT.

    Without this, any process started from a different cwd (uvicorn reload
    workers, pytest tmp dirs, shell-hopping, etc.) will silently point at a
    different SQLite file, which manifests as 'attempt to write a readonly
    database' or 'no such table' depending on the exact race.
    """
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url
    rest = url[len(prefix) :]
    p = Path(rest)
    if p.is_absolute():
        return url
    abs_path = (PROJECT_ROOT / rest).resolve()
    abs_path.parent.mkdir(parents=True, exist_ok=True)
    return f"{prefix}{abs_path}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Ensure the data dir exists in the project root (not the cwd).
    (PROJECT_ROOT / "data").mkdir(exist_ok=True)
    settings = Settings()
    settings.database_url = _resolve_sqlite_url(settings.database_url)
    return settings
