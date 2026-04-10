# threads-analytics · Style guide

This is the design contract for the service. It exists so visual decisions are
made once and then followed consistently, and so Claude (me) can review its own
work against a written standard instead of improvising.

## Prime directives

1. **Content is the punchline, chrome is the frame.** The service is a research
   instrument for one person. Every pixel that isn't delivering a finding or
   an action is noise.
2. **Copy the Threads app's visual vocabulary.** Light mode. White cards on
   light gray. System font. Left icon sidebar. Generous rounded corners. Black
   text, gray secondary. No gradients in the chrome. If a pattern isn't in
   Threads itself, we need a reason to add it.
3. **Rigor over persuasion.** Never assert as fact what is an inference. Label
   hypotheses as hypotheses. Cite sources (X heavy ranker, Meta statements)
   where claims come from.
4. **One punchline per page.** Most cards are data. One hero per page carries
   the takeaway — and earns its color.
5. **If a word count > 30 and a chart would show it faster, make a chart.**
   Walls of text are the enemy of reading.

## Palette

### Base (chrome — used everywhere)

| Token            | Hex       | Purpose |
|------------------|-----------|---------|
| `--bg`           | `#f5f5f5` | Page background (light gray, Threads-exact) |
| `--surface`      | `#ffffff` | Card background |
| `--surface-hover`| `#fafafa` | Card hover state |
| `--surface-sunken`| `#f0f0f0`| Pills, tags, nested surfaces |
| `--border`       | `#e6e6e6` | Card borders (barely visible) |
| `--border-strong`| `#d0d0d0` | Form controls, hover borders |
| `--divider`      | `#efefef` | Horizontal rules inside cards |
| `--text`         | `#000000` | Headings, handles, primary |
| `--text-body`    | `#1c1e21` | Body copy |
| `--text-muted`   | `#65676b` | Timestamps, labels, metadata |
| `--text-faint`   | `#8e8e93` | Even lower contrast |

### Semantic (data signals only — never decoration)

| Token        | Hex       | When |
|--------------|-----------|------|
| `--pos`      | `#1f8a3a` | Metric improved, win verdict, breakout |
| `--neg`      | `#e0245e` | Metric regressed, loss verdict, flop |
| `--link`     | `#0095f6` | Form focus ring only. Not used for content links (we underline on hover instead). |
| `--warn`     | `#b47704` | `HYPOTHESIS` tag color only |

### Hero (only on the one punchline card per page)

These colors exist **only** inside `.card-hero-*` variants. They are never used
for chrome, borders, or supporting content. Each color has an assigned function
— using the wrong one dilutes the signal.

| Token         | Hex       | Function | Used on |
|---------------|-----------|----------|---------|
| `--hero-coral`  | `#f26b3a` | **Alarm** — regression you should look at | Ground Truth verdict when metrics are moving the wrong way |
| `--hero-pink`   | `#f4a8c8` | **Penalty / flop** — suppression signal | Ground Truth worst metric; Noteworthy flops |
| `--hero-yellow` | `#ffd84d` | **Observational pullquote / neutral snapshot** | Ground Truth verdict when neutral/first-run; Perception one-sentence cold read; Noteworthy reach outliers |
| `--hero-green`  | `#4cb77e` | **Action / breakout / win** | Algorithm highest-ROI lever; Noteworthy breakouts & conversation starters |
| `--hero-blue`   | `#8bc7e8` | **Identity / stable fact about you** | You core identity |
| `--hero-royal`  | `#2c4cdb` | Reserved | — |
| `--hero-black`  | `#0d0d0f` | Reserved | — |

### The color rule in one sentence

> Coral screams, pink warns, yellow observes, green acts, blue identifies,
> and white is data. Every other case is white.

## Typography

### Stacks

- **Body / chrome / UI:** `-apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", "Helvetica Neue", Arial, sans-serif` — the system stack that matches Threads.
- **Display (hero cards only):** `"Space Grotesk", -apple-system, "SF Pro Display", sans-serif` at weight 600–700 with tight negative tracking. Never used outside hero cards.
- **Mono:** `"SF Mono", "Menlo", "Consolas", ui-monospace, monospace` — for tabular numbers, predicate JSON, and timestamps.

No serifs. No Google Fonts in the chrome.

### Scale

| Use                  | Size           | Weight | Tracking  |
|----------------------|----------------|--------|-----------|
| h1 (page title)      | 22px           | 700    | -0.01em   |
| h2 (section header)  | 16px           | 700    | -0.01em   |
| h3 (card title)      | 15px           | 600    | normal    |
| Body                 | 15px           | 400    | normal    |
| Secondary            | 13px           | 400    | normal    |
| Metadata / labels    | 11–12px        | 500–600| +0.02em   |
| Hero display         | `clamp(18, 2vw, 28)` | 600–700 | -0.02em |
| Hero number          | `clamp(56, 7vw, 96)` | 700 | -0.04em   |

Tabular figures (`font-variant-numeric: tabular-nums`) on any number that sits
in a column and needs to align.

## Spacing rhythm

- **Card padding:** 20px × 24px (standard), 28px × 32px (hero), 44px × 44px (xl hero).
- **Card gap (grid):** 14px.
- **Section vertical rhythm:** 22px between a `.section-head` and the block above; 10px below.
- **Border radius:** 20px (cards), 24px (hero), 999px (pills, tags, buttons).

## Cards — when to use which variant

1. **`.card`** (white, border, 20px radius) — the default. 95% of content.
2. **`.card-hero-*`** — the page's single punchline. Must carry the headline or
   the action. Never used for secondary content. If you can't articulate what
   the hero is *saying* in one sentence, it shouldn't be a hero.
3. **Nothing else.** No gradients, no glass-morphism, no shadows beyond
   `--shadow-sm`, no tinted borders.

## Motion

- **Fast (140ms):** hover bg, button press scale, nav tooltip.
- **Base (240ms):** card hover lift, padding/radius transitions on hero swap.
- **Transitions across pages:** browser-native View Transitions API (cross-fade).
- **Reduced motion:** `@media (prefers-reduced-motion: reduce)` kills all
  animations and transitions. Respect it.
- **Banned:** breathe/pulse loops on anything other than the run-banner
  spinner. Attention-seeking motion is noise.

## Content rules

1. **Words vs charts.** If a section is >30 words and describes a distribution
   (frequencies, weights, counts), replace with a visual. Bar chart, tag
   cloud, chip row, whatever reads faster.
2. **No filler.** No "Here's a look at…", "Let's dive into…", "It's worth
   noting…". Delete. Start on the finding.
3. **Hypothesis labels.** Anything that isn't a documented fact or a direct
   measurement from the user's data is a hypothesis. It gets the
   `<span class="hypothesis-tag">HYPOTHESIS</span>` chip.
4. **Citations.** When a claim comes from research (X heavy ranker, Meta
   public statements), cite it inline.
5. **Hedged language.** "Likely", "consistent with", "plausible explanation
   is" — never "the algorithm is penalizing you."

## Page recipes

### Ground truth (`/`)
- **Hero:** verdict card (6-col grid, spans 4). Tone-colored: coral/green/yellow.
- **Hero 2:** worst-regressing metric card, spans 2, pink.
- Rest: white metric cards with sparklines.
- Recent runs table at the bottom.

### Experiments (`/experiments`)
- **No hero.** Active experiments get an accent left-border only.
- Track record widget at top. Tabs for active/completed/proposed.

### Suggestions (`/suggestions`)
- **No hero.** All cards equal. Each is a hypothesis, tagged as such.

### Perception (`/perception`)
- **Hero:** `oneSentenceCold` as a yellow pullquote. Text ≤ 32px.
- Rest: small white cards with labeled sections. No walls of text.

### Algorithm (`/algorithm`)
- **Hero:** `highestRoiLever` card, green.
- **Signal breakdown:** 6 small white cards, one per signal. Not verbose.
- **Inferred weights:** horizontal bar chart (no lists).
- Everything labeled HYPOTHESIS or FACT.

### You (`/you`)
- **Hero:** core identity, blue.
- **Protect / Double down:** two-column lists.
- **Signatures:** tag chip row, not a list.
- **Quintessential posts:** max 3 blockquotes.
- **Risks:** short list.
- Remove: distinctive voice traits (overlaps with signatures), unique topic
  crossovers (already in core identity).

### Noteworthy posts (`/posts`)
- Max 5 cards.
- First card is the hero by default. Any card can be clicked to become the
  hero; others revert. Selection persists via localStorage.
- Hover shows a "✦ feature this" chip in the top-right.
- Hero color is category-coded: breakout→green, reach outlier→yellow,
  flop/served-but-fell-flat→pink.

## Review checklist

Before shipping any visual change, answer these:

- [ ] Does this page have exactly one hero (or two on Ground Truth)?
- [ ] Is the hero color correct per the functional rule?
- [ ] Every claim about the algo labeled HYPOTHESIS or cited as FACT?
- [ ] Any text block > 30 words that could be a chart?
- [ ] Any card doing nothing useful — can it be removed?
- [ ] Reduced motion honored?
- [ ] Spacing follows the rhythm (14/20/28/44)?
- [ ] No serif fonts in the chrome?
- [ ] No gradients outside the run banner?
