# arcus-arb

Phase A is a public-market-data and record-only fork of `entropy-arb` for:

```text
Arcus SNDK × Lighter-RH SNDK
```

Record-only remains the default-safe workflow and needs no credentials. Phase
B0 adds a separate, tiny-live preflight for one-sided Arcus LIMIT+ALO maker
calibration and an existing Lighter-RH hedge after an authoritative Arcus
fill. B0 needs both `--tiny-live` and `--confirm-mainnet`; it also requires
the separate `--approve-first-order` gate before the first Arcus order. This
repository does not submit that first order automatically.

## Run

```bash
cp config.example.yaml config.yaml
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --record-only
```

Use `--no-dashboard` for plain logs. The recorder writes to the independent
`data/market-history.sqlite` database by default; it never opens the source
project's database.

For B0 preflight only, configure the existing Arcus Ed25519 API identity and
the existing Lighter-RH credentials in a local, ignored `.env`, then run:

```bash
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --tiny-live --confirm-mainnet --no-dashboard
```

This prints the fresh account, market, BBO, fee, center, quote, and safety
state, then stops before `placeOrder` unless the separate first-order approval
flag is deliberately supplied after a human review. No wallet generation or
API-key registration is performed.

## Arcus public API used

The implementation follows the official documentation at
<https://docs.arcus.xyz/>:

- REST market discovery: `GET https://api.arcus.xyz/v1/markets`.
- One public multiplexed WebSocket: `wss://api.arcus.xyz/v1/ws`.
- Exactly three subscriptions: `l2OrderbookUpdates` for `SNDK-USD`, `trades`
  for `SNDK-USD`, and global `marketAttributes`.
- The `bbo` channel is intentionally not subscribed. The L2 update snapshot
  already supplies the BBO and the deltas maintain the local book.

B0's account websocket uses four additional account-state subscriptions on its
separate connection: `userFills`, `orders`, `positions`, and
`accountAttributeUpdates`. These are not redundant public market-data
subscriptions. The account stream is used for asynchronous lifecycle and fee
state; only signed `placeOrder`/`cancelOrder` RPCs can mutate the Arcus
account, and B0 exposes no Arcus taker, modify, cancel-all, or private trading
channel operation.

The CLI symbol is resolved from live metadata (`baseAsset` or
`marketDisplayName`); tick and quantity precision are never hard-coded. L2
`lastSequenceId` is the per-market continuity anchor. `globalSequenceId` is
stored as cross-market telemetry and is not used as the gap anchor. A gap
clears the book, marks it `RESYNC`, requests a fresh L2 snapshot, and resumes
only after that snapshot is accepted. Disconnects mark the book `STALE`.

Arcus public trades are stored separately with exchange timestamp, local wall
receive timestamp, local monotonic receive timestamp, price, size, trade ID,
sequence number, and any explicitly supplied aggressor side. The current
public schema does not supply an aggressor side, so the collector does not
infer one. Market attributes preserve nullable RTH state, settlement price,
current/next bounds, event timestamp, and market sequence number.

## Storage and analysis

Phase A.1 additionally stores every wire price-level in the append-only
arcus_l2_events table. Snapshot and delta rows retain event type, the
SQLite receive-order id, event_index, book_epoch, per-market lastSequenceId,
telemetry globalSequenceId, both local receive clocks, side, price, and
absolute size. Zero-size delta rows are retained as deletes; levels at the
same price are never aggregated. A sequence-gap delta is also stored as
received, while the local book is invalidated until a fresh snapshot starts
the next epoch. The bounded WAL writer batches these rows and flushes them on
shutdown. arcus_l2_stats() and the shutdown log expose committed row count,
receive-span event rate, database bytes, and WAL bytes.

arcus_l2_events is aggregated market-by-price data, not individual order data,
so it does not provide exact maker queue position by itself. Future queue
replay must use a conservative model.

`arcus_samples` stores approximately one valid BBO sample per second for both
legs, including sizes, midpoint prices, premium, Arcus sequence IDs, both
timestamp domains, and nullable market attributes. `arcus_trades`,
`arcus_market_attributes`, and `arcus_market_metadata` are separate append-safe
datasets. `arcus_minutes` retains the existing minute-summary pattern.

The premium remains the mature midpoint definition:

```text
((arcus_bid + arcus_ask) / 2) /
((rh_bid + rh_ask) / 2) - 1
```

multiplied by 10,000. The existing `stable_basis` and optional rolling-center
logic consume this Arcus/RH midpoint premium. The configured center is
informational in Phase A; no Arcus thresholds are tuned here.

The dashboard displays `ARCUS`, `RH`, BBO age, premium, center, recorder rows,
RTH state, sequence health (`OK`/`RESYNC`/`STALE`), and an explicit
`RECORD-ONLY · Arcus trading disabled` banner. In B0 preflight it instead
shows `TINY-LIVE PRE-ORDER`, `Arcus ALO only`, and the separate approval gate;
it does not expose generic live-trading controls.

The record-only dashboard also shows the session raw L2 event count alongside
the Arcus sequence health, without exposing any trading controls.

## B0 safety boundary

B0 is a calibration envelope, not a production strategy: one Arcus SNDK order
at a fixed `0.01` quantity, one side at a time, maximum 20 Arcus fill events,
`$500` filled notional, `$5` session loss, and 60 minutes. Quotes are LIMIT
ALO only and wait when the modeled post-hedge edge is below 4 bps; the 1.5 bps
cancel threshold is hysteretic. A fill is hedged on RH only after it is
authoritatively received, with residual quantities below the RH minimum kept
visible rather than rounded up. Any account disconnect, stale/resync market,
RTH transition, unresolved RH hedge, telemetry failure, or hard limit halts
new quoting and explicitly reconciles/cancels the known Arcus order.

The Arcus identity is loaded only from `ARCUS_ACCOUNT_ADDRESS`,
`ARCUS_API_KEY`, and `ARCUS_ED25519_PRIVATE_KEY` or
`ARCUS_ED25519_PRIVATE_KEY_FILE`; credentials are never written to config or
logged. Fee tier is resolved from `GET https://api.arcus.xyz/v1/feetiers` plus
the account attribute stream; unknown fees abort B0.

The documented `userFills` stream can omit store-only `createdAt` and `fee`
fields. B0 hedges an actionable fill immediately using the resolved maker-tier
fee as a provisional accounting value, then reconciles the public
`GET /v1/fills` row without replaying the hedge; if the actual fee cannot be
recovered, the session halts and does not quote again.

The inherited Entropy/Hyperliquid and mature execution modules remain only as
historical/reusable architecture. The Arcus path never uses them for Arcus
orders. Shadow maker simulation, maker strategy optimization, execution
research, and production deployment are out of scope.

The authentication docs currently describe the compact sorted-JSON Ed25519
request scheme for orders, while the registration material includes a current
EIP-712 flow and a legacy EIP-191 quickstart note. B0 does not register keys;
it only validates a user-provided keypair and signs the documented LIMIT+ALO
request when the separate approval gate is used.
