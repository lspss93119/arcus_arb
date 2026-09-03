# arcus-arb

**[中文文档 / Chinese documentation → README.zh-CN.md](README.zh-CN.md)**

Open-source two-venue perp arbitrage bot. One leg is always **Entropy**
(the `io` builder dex on Hyperliquid); the other leg — the hedge — is one of:

| `--hedge` | venue | quote | taker fee | protocol |
|---|---|---|---|---|
| `lighter` | Lighter mainnet | USDC | 0 bps | zkLighter ws (diff books, async settle) |
| `lighter-rh` | Lighter Robinhood chain | **USDG** | 0 bps | zkLighter ws |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book, sync IOC settle |

> **Referral links** — signing up through these supports this project:
> - Entropy — Tier 4 referral, 100% rebates: <https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood chain: <https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz (Hyperliquid): <https://app.hyperliquid.xyz/join/QUANTGUY>

When the same symbol trades rich on one venue and cheap on the other, the bot
simultaneously sells the rich book and buys the cheap book with taker orders,
carrying a delta-neutral position until the premium reverts and the opposite
crossing unwinds it. Every price it acts on is the **actual order book of the
exchange that will fill the order** — Hyperliquid books come from the official
websocket (`wss://api.hyperliquid.xyz/ws`), Lighter books from Lighter's
official websocket.

While it runs — including `--record-only` without credentials — the recorder
stores approximately 1 Hz raw BBO samples and minute aggregates in SQLite.
Supported Lighter reference data is stored in the same database. Those records
support offline market analysis and human selection of strategy parameters; the
recorder and live bot do not diagnose markets, select a strategy, or switch one
for you.

## The signal

The live bot uses one explicitly selected strategy in `config.yaml`:

- `stable_basis` uses a human-selected fixed `center_bps` and is ready
  immediately at startup.
- `drifting_basis` uses a causal, timestamp-based rolling-median center from
  approximately 1 Hz valid fresh-BBO observations. It requires a full-window
  warm-up and at least 90% valid coverage. A valid-observation gap longer than
  30 seconds resets its history and warm-up; a process restart starts empty,
  with no CSV preload or persisted center.

There is no automatic market diagnosis, strategy selection, or strategy
switching in the live bot. You review recordings and select the strategy and
parameters explicitly.

For either strategy, the signal uses the mid-to-mid premium as its center
reference while entry decisions use executable prices:

```
premium_bps = (Entropy price / hedge price − 1) × 10 000

                          ┌──────────────  SELL entropy + BUY hedge
center + upper   ───────────────────────────────────────────────────
                                       ▲
center           ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   the selected center
                                       ▼
center − lower   ───────────────────────────────────────────────────
                          └──────────────  BUY entropy + SELL hedge
```

- `center_bps` (for `stable_basis`) — the human-selected level where the
  premium normally sits. Cross-venue premiums are rarely centered at zero
  (different oracles, quote assets, or listing premia).
- `upper_bps` / `lower_bps` — the entry bands on each side of the selected
  center. For `drifting_basis`, the center is supplied by the causal rolling
  median after warm-up.

Both hurdles are applied to **executable** prices (entropy bid vs hedge ask,
and vice versa) and are **net of both venues' taker fees** — the engine adds
fees on top before a slice qualifies. A full round trip therefore nets
**≥ upper + lower bps after fees by construction**.

One consequence worth understanding: with a selected center of `5` bps, the
buy-entropy hurdle is `lower_bps − center_bps`, which can be **negative**.
That is intentional — if Entropy is persistently 5 bps rich, buying it at a
0 bps premium is cheap versus its own equilibrium and can unwind an earlier
sell. A wrong human-selected center can lose money, so measure first and trade
with small caps.

## Quick start

```bash
git clone https://github.com/your-quantguy/entropy-arb.git && cd entropy-arb
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # data collection needs only this

cp config.example.yaml config.yaml       # explicit strategy, sizing, and risk
cp .env.example .env                     # credentials — required to trade
```

The markets are **not** in the config file — you state them explicitly on
every start: `--symbol` (traded on both venues) and `--hedge` (one of
`lighter`, `lighter-rh`, `tradexyz`; Entropy is always the
other leg).

There is **no paper mode** — the bot either collects data (`--record-only`)
or trades live. Validate with recorded data and tiny position caps, not with
simulated fills.

**1. Collect data first** (no credentials needed):

```bash
python3 main.py --record-only --symbol SNDK --hedge lighter-rh
```

Let it run for at least a few hours (a day is better — premiums have
intraday regimes). It writes the same SQLite database used by live mode;
the default is `data/market-history.sqlite`.

**2. Review the market and select strategy parameters:**

```bash
python3 tools/analyze.py --csv /path/to/legacy-minutes.csv
```

`tools/analyze.py` is a legacy CSV ad-hoc tool: use it only with an existing
CSV export. It does not read the live SQLite database and does not select or
switch strategies. For current market history, create a SQLite snapshot and
review or upload that single file for manual/ChatGPT analysis before choosing
`stable_basis` or `drifting_basis` and its parameters explicitly in
`config.yaml`.

**3. Go live** — fill in `.env`, install the signing SDKs, and start with
the smallest position caps that clear the venue minimums:

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge lighter-rh
```

Running without `--record-only` sends real orders immediately once both
feeds are fresh and the band is crossed.

**Dashboard.** On a terminal the bot shows a live Rich dashboard: both
books with age/spread, positions and caps, equity and session PnL, the
executable premium of each direction against its full hurdle (fees and
inventory surcharge included, ● = armed), recorder progress, the last
executions, and a tail of the log (the full log goes to `logging.file`,
default `logs/engine.log`). It works in `--record-only` too. Add `--cn` to
display the dashboard in Chinese. Use `--no-dashboard` for plain console
logs (nohup/systemd — off-terminal runs fall back automatically), or set
`logging.dashboard: false`.

## Data collection & analysis

The recorder runs automatically in every mode (`recorder.enabled: true`). It
stores approximately 1 Hz raw BBO samples and minute aggregates in
`recorder.database`; the default path is `data/market-history.sqlite`.
Supported Lighter reference observations are stored in the same database. All
bot processes can share this database through SQLite WAL, so each process can
record its selected market pair without a separate CSV file. `--record-only`
writes the same database without trading credentials.

Minute aggregates retain these fields:

| column | meaning |
|---|---|
| `minute_ts` | minute start (epoch seconds); `time_utc` can be derived when needed |
| `entropy_bid/ask`, `hedge_bid/ask` | last fresh top-of-book of the minute |
| `premium_open/high/low/close/mean/std_bps` | mid-to-mid premium of Entropy over the hedge |
| `sell_edge_mean/max_bps` | executable premium for SELL entropy (entropy bid / hedge ask − 1) |
| `buy_edge_mean/max_bps` | executable premium for BUY entropy (hedge bid / entropy ask − 1) |
| `samples` | how many of the ~60 seconds both books were fresh |

Recorded edges are pre-fee. The current database workflow is:

1. Record live or with `--record-only` into `recorder.database`.
2. Optionally import existing CSV files non-destructively with
   `tools/migrate_market_history.py`; the original CSV files remain in place.
3. While bots are active, create one standalone SQLite snapshot with
   `tools/snapshot_data.py`.
4. Upload that snapshot for manual or ChatGPT analysis.

`tools/analyze.py` remains a legacy CSV ad-hoc tool only. It can inspect an
existing export but is not a SQLite reader and never diagnoses a market,
selects a strategy, or switches strategies. Premiums drift, so review the
recorded data and update the explicitly selected `config.yaml` strategy
deliberately.

## Configuration

Strategy lives in `config.yaml` (validated — unknown keys are startup
errors), credentials in `.env`, and the markets on the command line
(`--symbol`, `--hedge`). Full commented reference:
[config.example.yaml](config.example.yaml). The essentials:

| key | meaning | default |
|---|---|---|
| `strategy.name` | explicitly selected `stable_basis` or `drifting_basis` | `stable_basis` |
| `strategy.params.center_bps` | human-selected fixed center for `stable_basis` | `0.0` in example |
| `strategy.params.upper_bps` / `lower_bps` | entry bands (> 0) | `4.0` in example |
| `strategy.params.window_minutes` | timestamp-based rolling window for `drifting_basis` | `60` in alternate example |
| `entropy.dex` | Entropy's dex name on Hyperliquid | `io` |
| `*.taker_fee_bps` | per-venue taker fee | 0.0 (tradexyz hedge: 1.0) |
| `*.max_position_usd` | per-venue position cap | 1000 |
| `*.max_orders_per_min` | per-venue send budget (sliding 60 s) | 120; lighter hedges 30 |
| `sizing.take_fraction` | fraction of crossable depth taken | 0.5 |
| `sizing.max_order_notional_usd` | per-slice cap | 500 |
| `inventory.scale_bps` / `floor_frac` | inventory ladder (extra bps past `floor_frac` of the cap) | 10 / 0.5 |
| `execution.premium_persist_sec` | edge must persist before firing | 0.3 |
| `execution.*` | slippage bounds, timeouts, reconcile cadence… | see file |
| `recorder.enabled` / `recorder.database` | market-history storage | true / `data/market-history.sqlite` |
| `logging.dashboard` / `logging.file` | Rich dashboard on a tty; log file while it runs | on, `logs/engine.log` |

## Credentials (`.env`, live only)

- **Entropy / tradexyz (Hyperliquid)** — create an API ("agent") wallet at
  <https://app.hyperliquid.xyz/API>. `HL_PRIVATE_KEY` is the **agent** key,
  `HL_ACCOUNT_ADDRESS` your main account address. With `--hedge tradexyz`
  both legs share this account by default (one nonce sequence is handled
  internally); set `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`
  to split them. Fund the dex-specific clearinghouses you trade.
- **Lighter** — `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`,
  `LIGHTER_API_PRIVATE_KEY`, registered on the **same deployment** as your
  `--hedge` flag (mainnet and the Robinhood chain are separate accounts and
  keys — see [lighter-python](https://github.com/elliottech/lighter-python)).

## How execution works

- Both legs are **taker** orders sent concurrently: Lighter market orders
  with average-price protection settling on the authenticated account
  websocket; Hyperliquid IOC limits settling synchronously (with
  orderStatus polling for unknown outcomes).
- A **persistence gate** (`premium_persist_sec`) arms each direction and only
  fires if the edge survives — one-tick phantoms are filtered.
- **Inventory ladder**: past `floor_frac` of a venue's cap, adding to the
  position requires linearly more edge, up to `scale_bps` extra at the cap.
- **Net-delta hedge**: if legs fill unevenly, the imbalance is immediately
  reduced (reduce-only, price-protected), and positions are reconciled
  against the chain every `reconcile_sec`.
- **Failure containment**: a rate-limited venue pauses briefly; an
  unreachable venue (e.g. exchange maintenance) pauses trading and is probed
  every `venue_probe_sec` until it recovers; `max_consecutive_errors`
  execution pathologies halt the engine entirely.
- **Live-only**: there is no simulated-fill mode. `--record-only` is the
  risk-free way to run it; anything else trades real money.

## Layout

```
main.py                  entry point (--record-only, or live by default)
entropy_arb/config.py    YAML + .env contract, validation
entropy_arb/book.py      order books + fee-aware crossing/sizing math
entropy_arb/feeds.py     official HL ws + zkLighter ws book feeds
entropy_arb/venue_hl.py  Hyperliquid dex adapter (Entropy, tradexyz)
entropy_arb/venue_lighter.py  zkLighter adapter (mainnet, Robinhood chain)
entropy_arb/engine.py    the two-venue strategy loop
entropy_arb/dashboard.py Rich terminal dashboard
entropy_arb/recorder.py  raw BBO + minute market-history recorder
entropy_arb/storage.py   SQLite WAL market-history store
entropy_arb/migration.py non-destructive legacy CSV import
entropy_arb/snapshot.py  consistent SQLite backup API snapshot
tools/migrate_market_history.py  import legacy CSV into SQLite
tools/snapshot_data.py   create one standalone SQLite snapshot
tools/analyze.py         legacy CSV ad-hoc analysis only
tests/                   python3 -m pytest tests/
```

## Known risks

- **A wrong center is a losing strategy.** Premiums drift; re-measure regularly
  and keep the explicitly selected strategy parameters in `config.yaml`
  current.
- **USDG basis** (`lighter-rh`): the hedge quotes in USDG. Part of any
  persistent premium is the stablecoin itself; your selected center absorbs the
  level, but a USDG *move* is real PnL.
- **Funding**: two venues, two independent funding rates; carry is not
  modeled. Position caps bound it — keep them modest.
- **Thin books**: Entropy depth can be tiny; `take_fraction` and notional
  caps keep clips small, but slippage on the hedge leg after a partial fill
  is real.
- **Market hours**: for equity perps (e.g. SNDK), off-hours oracle regimes
  differ per venue; consider wider bands or not trading them.
- **One-leg risk**: a leg can fail after the other filled. The bot hedges
  and reconciles automatically, but you should still watch it.

Use at your own risk. This is trading software operating with real money;
nothing here is investment advice. Start with tiny position caps.

## License

[MIT](LICENSE)
