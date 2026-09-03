# arcus-arb

Phase A is a public-market-data and record-only fork of `entropy-arb` for:

```text
Arcus SNDK × Lighter-RH SNDK
```

It contains zero trading capability for Arcus. `--record-only` is mandatory;
starting without it exits locally before market discovery, and
`ArcusVenue.supports_trading` is permanently `False` in this phase. No Arcus
credential, wallet, private key, signer, or API registration is needed.

## Run

```bash
cp config.example.yaml config.yaml
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --record-only
```

Use `--no-dashboard` for plain logs. The recorder writes to the independent
`data/market-history.sqlite` database by default; it never opens the source
project's database.

## Arcus public API used

The implementation follows the official documentation at
<https://docs.arcus.xyz/>:

- REST market discovery: `GET https://api.arcus.xyz/v1/markets`.
- One public multiplexed WebSocket: `wss://api.arcus.xyz/v1/ws`.
- Exactly three subscriptions: `l2OrderbookUpdates` for `SNDK-USD`, `trades`
  for `SNDK-USD`, and global `marketAttributes`.
- The `bbo` channel is intentionally not subscribed. The L2 update snapshot
  already supplies the BBO and the deltas maintain the local book.

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
`RECORD-ONLY · Arcus trading disabled` banner. It has no Arcus execution
controls.

The record-only dashboard also shows the session raw L2 event count alongside
the Arcus sequence health, without exposing any trading controls.

## Scope boundary

This phase does not implement Arcus orders, signing, API keys, withdrawals,
maker/taker execution, cancels, private channels, positions, fills, shadow
maker simulation, production thresholds, or RH hedging. The inherited
Entropy/Hyperliquid and execution modules remain only as historical/reusable
code paths for the mature test and analysis architecture; the Arcus config and
Phase A engine never select them.
