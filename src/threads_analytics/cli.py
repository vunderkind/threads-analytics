"""Typer CLI: run, serve, refresh."""

from __future__ import annotations

import json
import logging

import typer
import uvicorn

from .backfill import backfill_history
from .config import get_settings
from .db import init_db
from .pipeline import run_full_cycle
from .threads_client import ThreadsClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = typer.Typer(help="Analytics + growth recommender for a personal Threads account.")


@app.command()
def run() -> None:
    """Run a full ingest → analyze → recommend cycle."""
    init_db()
    summary = run_full_cycle()
    typer.echo(json.dumps(summary, indent=2, default=str))


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000, reload: bool = False) -> None:
    """Start the local dashboard."""
    init_db()
    uvicorn.run("threads_analytics.web.app:create_app", host=host, port=port, reload=reload, factory=True)


@app.command()
def refresh() -> None:
    """Refresh the long-lived Threads access token (run every ~50 days)."""
    settings = get_settings()
    with ThreadsClient() as client:
        new_token = client.refresh_long_lived_token()
    # Persist by rewriting .env
    _update_env_file("THREADS_ACCESS_TOKEN", new_token)
    typer.echo("Token refreshed and .env updated.")


@app.command()
def backfill(
    bucket_days: int = 1,
    max_days_back: int = 180,
    window_days: int = 14,
) -> None:
    """Populate historical Ground Truth snapshots from existing post data.

    Walks backwards from today in buckets of `bucket_days`, computing the
    same Ground Truth metrics at each point in time using the posts already
    in the database. Each bucket creates a synthetic run + account insight
    row so the sparklines on / show real history.

    Idempotent — running twice does not create duplicates.

    Caveats: follower count is not historically tracked, so reach rate and
    follower velocity are not accurate during backfill. Reply rate, reply
    ratio, zero-reply fraction, and top-decile reach multiple ARE accurate.
    """
    init_db()
    summary = backfill_history(
        bucket_days=bucket_days,
        max_days_back=max_days_back,
        window_days=window_days,
    )
    typer.echo(json.dumps(summary, indent=2, default=str))


@app.command()
def whoami() -> None:
    """Verify the token by calling /me on the Threads API."""
    try:
        with ThreadsClient() as client:
            data = client.get_me()
    except RuntimeError as exc:
        typer.secho(f"✗ {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    except Exception as exc:  # noqa: BLE001
        typer.secho(
            f"✗ Threads API call failed: {exc}", fg=typer.colors.RED, err=True
        )
        raise typer.Exit(code=1)
    typer.echo(json.dumps(data, indent=2))


def _update_env_file(key: str, value: str, path: str = ".env") -> None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        lines = []
    found = False
    out: list[str] = []
    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={value}\n")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}\n")
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(out)


if __name__ == "__main__":
    app()
