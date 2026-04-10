# threads-analytics

A local-first analytics + growth recommender for a personal Threads account (`@your-handle`). Pulls your own post insights from the official Threads API, discovers affinity creators in your topic space via the public keyword search endpoint, and uses Claude to synthesize ranked, evidence-backed growth recommendations. Each run measures what moved the needle since the previous run, so recommendations improve over time.

## Quick start

```bash
# 1. Install
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Create a Meta app at developers.facebook.com, add the Threads product,
#    add your Threads account as a tester, copy App ID + App Secret into .env
cp .env.example .env
# fill in META_APP_ID, META_APP_SECRET, ANTHROPIC_API_KEY

# 3. One-time OAuth: exchange a short-lived code for a long-lived token
python scripts/setup_token.py
# follow the prompts; your .env will be populated with THREADS_ACCESS_TOKEN + THREADS_USER_ID

# 4. First ingest + analysis run
threads-analytics run

# 5. Open the dashboard
threads-analytics serve
# → http://localhost:8000
```

## What it does

1. **Ingests** your posts and per-post + account-level insights from the Threads Graph API
2. **Extracts topics** from your posts using Claude
3. **Discovers affinity creators** by keyword-searching Threads for your topics and ranking authors by public engagement
4. **Analyzes patterns** (posting time, length, format, hook words) for both you and top affinity creators
5. **Synthesizes recommendations** via Claude, grounded in specific numbers from your data vs. theirs
6. **Tracks outcomes** — each subsequent run measures what changed since prior recommendations were issued, and feeds that back into the next synthesis pass

## CLI

```bash
threads-analytics run       # full ingest + analyze + recommend cycle
threads-analytics serve     # start the dashboard on http://localhost:8000
threads-analytics refresh   # refresh the long-lived access token (auto-runs every 50 days)
```

## Rate limits

The Threads `/keyword_search` endpoint is capped at 500 queries per rolling 7-day window. This service budgets ~30 queries per run and refuses to issue new searches if the projected 7-day usage would exceed 400.

## Project layout

See the plan file for the full design rationale and schema.
