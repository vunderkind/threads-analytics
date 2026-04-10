# threads-analytics

A local-first analytics and growth experimentation dashboard for a single
Threads account. Ingests your own post insights via the official Threads
Graph API, produces research-grounded perception and algorithm-inference
reports via Claude, and runs scientific experiments with statistical verdicts.

Everything runs on your machine. Nothing leaves your environment except the
API calls to Meta (your own data) and Anthropic (your own data, for Claude
synthesis).

## Requirements

- Python 3.11 or newer
- A Meta developer account with a Threads app (free)
- An Anthropic API key

## Setup

### 1. Clone and install

```bash
git clone <this-repo> threads-analytics
cd threads-analytics
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Create a Threads app in the Meta developer console

1. Open <https://developers.facebook.com/apps/> and create a new app.
2. Add the **Threads** product to the app.
3. In the Threads product settings, find the **Threads app ID** and **Threads
   app secret** (these are distinct from the general Meta app ID).
4. Under **Redirect Callback URLs** add `https://localhost/` and save.
5. Under **Threads testers** (or Roles → Threads Testers) invite your own
   Threads account, then open
   <https://www.threads.net/settings/account/website_permissions> on the
   tester account and accept the invitation.
6. If you want public keyword search (used for affinity creator discovery),
   your app will need to go through Meta App Review for the
   `threads_keyword_search` permission. Without it the service still works —
   affinity data is simply empty until approval.

### 3. Fill in `.env`

```bash
cp .env.example .env
```

Edit `.env` and set:

- `META_APP_ID` — the Threads app ID from step 2
- `META_APP_SECRET` — the Threads app secret from step 2
- `ANTHROPIC_API_KEY` — from <https://console.anthropic.com/>

Leave `THREADS_ACCESS_TOKEN`, `THREADS_USER_ID`, and `THREADS_HANDLE` blank.
They'll be populated by the setup script in the next step.

### 4. Exchange an OAuth code for a long-lived token

```bash
python scripts/setup_token.py
```

Follow the prompts. The script prints a URL — open it in a browser logged
in as your Threads account, click **Allow**, and copy the entire redirect
URL from the browser's address bar back into the script. The script
exchanges the code for a 60-day long-lived access token and writes it into
your `.env`.

### 5. First run

```bash
threads-analytics whoami        # sanity-check the token
threads-analytics run           # full ingest + analysis cycle (5–15 minutes)
threads-analytics backfill      # populate historical ground-truth sparklines
threads-analytics serve         # start the dashboard
```

Open <http://localhost:8000> in a browser.

## What it does

- **Ground truth** (`/`) — six scientific metrics the algorithm actually ranks on (reach rate, reply rate per view, reply-to-like ratio, zero-reply fraction, top-decile reach multiple, follower velocity), each with a baseline, delta, and 30-day trend.
- **Experiments** (`/experiments`) — create hypothesis-driven experiments with categorized predicates (timing, length, media, hook, topic, cadence, engagement, custom), run them over a variant window, and get statistical verdicts (Mann-Whitney U, bootstrap CI, Cliff's delta).
- **Suggestions** — Claude proposes experiments grounded in your current metrics, outsider perception, algorithm inference, and your personal experiment track record. Merged into the Experiments page as a carousel.
- **Perception** (`/perception`) — research-grounded thin-slice first-impression read using the Big Five framework and the Brunswik lens model. Shows per-trait ratings with cue evidence, misread risks, and the single highest-leverage profile fix.
- **Algorithm** (`/algorithm`) — signal-by-signal hypothesis of how the Threads ranker treats the account, grounded in X's open-sourced heavy ranker weights and Meta's public statements. Signal carousel, inferred weight bars, and a green highest-ROI lever card.
- **You** (`/you`) — anti-homogenization guardrail. Extracts voice signatures and builds a protect list that the suggestion engine respects so experiments never sand away what makes the account distinctive.
- **Noteworthy** (`/posts`) — outlier detection with Claude commentary on what made each standout post break out or flop, grounded in the account's own internal benchmarks (best, worst, and median exemplar posts).

## CLI

```bash
threads-analytics run        # full ingest + analyze + recommend cycle
threads-analytics serve      # start the dashboard at http://localhost:8000
threads-analytics backfill   # populate historical ground-truth from existing posts
threads-analytics refresh    # rotate the long-lived access token (every ~50 days)
threads-analytics whoami     # verify the token by hitting /me
```

## Tech

- Python 3.11+, FastAPI + Jinja2, SQLite via SQLAlchemy 2.0
- Claude via the Anthropic SDK (Opus for synthesis, Sonnet for topic extraction)
- scipy + numpy for the statistical verdict engine
- No frontend build step — vanilla templates, system fonts, Chart.js via CDN

## Caveats

- **Affinity creator discovery** requires Meta App Review for the
  `threads_keyword_search` permission. Before approval, the affinity page is
  empty but the rest of the service works fully.
- **Rate limits**: Threads API uses impression-based rate limiting. For a
  typical personal account this is effectively unbounded, but a full run
  with 1000+ posts takes ~5–15 minutes of wall time.
- **Ground truth baselines** fill in over multiple runs across several days.
  The `backfill` command reconstructs historical snapshots from existing
  post data so you don't have to wait.

## License

MIT (or whatever you choose — this is a personal tool, not a product).
