# Volume Probe V1 Design

## Goal

Add an independent, one-shot `--volume-probe` runtime mode that can perform one
small Arcus maker to Lighter-RH hedge round and then maker-unwind the exact
matched quantity. The mode is intended for an explicitly approved mainnet
probe, while preflight-only execution remains the default unless both
`--confirm-mainnet` and `--approve-first-order` are present.

## Scope and non-goals

V1 supports one clip, one build direction, immediate hedge, immediate unwind,
and automatic exit. It does not add funding-direction selection, automatic
market selection, a target-volume builder, multiple clips, repeat loops,
dashboard redesign, Telegram, markout optimization, per-market parameters,
basis-threshold optimization, or a hold period. Arcus is maker-only for the
entire mode; no taker or emergency-close path is added.

## Runtime gates

`--volume-probe` is mutually exclusive with `--record-only` and `--tiny-live`.
`--probe-clip-usd` and `--probe-side {buy,sell}` are required with the mode.
`--probe-reprice-sec`, `--probe-max-runtime-sec`, and `--probe-max-loss-usd`
have defaults of 30, 1800, and 10 respectively. The mode requires
`--confirm-mainnet`; without `--approve-first-order` it performs all public and
account preflight, logs the proposed order, writes a `PREORDER_ONLY` round, and
exits before a mutation RPC.

## State machine and execution

The round follows `FLAT -> BUILD -> HEDGED -> UNWIND -> FLAT -> DONE`.

For BUILD, `buy` places BUY Arcus at fresh best bid and hedges SELL RH; `sell`
places SELL Arcus at fresh best ask and hedges BUY RH. Quantity is
`probe_clip_usd / fresh Arcus mid`, rounded down to Arcus `stepSize`, then
validated against positive quantity, Arcus min/max size and min notional, and RH
minimum/step. Client IDs use `vp-<session>-...`. At most one probe order is
live. If it is not complete by the reprice interval, it is canceled and
reconciled to terminal state before any replacement is posted for the remaining
quantity.

Every real Arcus `userFills` event is passed through the existing
`FillAccumulator` and existing authoritative RH IOC hedge path. Duplicate trade
IDs, missing identity, stale/resync/account-stream health, unresolved orders,
RH partial/unresolved fills, the 20 bps RH slippage hard cap, loss cap, and
runtime cap remain fail-closed.

After the actual build quantity is fully RH-hedged, UNWIND immediately reverses
the Arcus side and RH hedge side. Its target is the actual completed build
quantity, not the requested quote quantity. DONE requires both venue positions
within quantity tolerance, no Arcus `vp-` order, no RH active order, and zero
accumulator residual. Any non-flat or unresolved final state is
`RECONCILIATION_REQUIRED`; no unverified market order is sent to force flat.

## Implementation boundary

The new `entropy_arb/volume_probe.py` module owns pure quantity/candidate/state
rules and append-only round CSV serialization. `VolumeProbeController` reuses
the B0 controller's order correlation, user-fill identity, hedge, cancel, and
reconcile plumbing. B0 defaults remain fixed at `0.01` and `b0-`; the generic
maker/controller options default to those exact values so existing B0 behavior
does not change.

## Telemetry

Existing SQLite `arcus_calibration_events` remains unchanged. Each probe round
appends one row to `logs/volume_probe_rounds.csv` with session, timing, build and
unwind fills/reprices, fees/PnL, latency/slippage, final positions, status, and
failure reason. A preflight failure before order placement is also recorded as
`HALTED` so failed invocations remain auditable. Status values include
`COMPLETED`, `TIMEOUT`, `HALTED`, `RECONCILIATION_REQUIRED`, and
`PREORDER_ONLY`.

## Verification

All new tests are pure/local and do not connect to live networks. Required
verification is `python3 -m pytest -q`, `ruff check .`, `ruff format --check .`,
`python3 -m mypy entropy_arb tests main.py`, and
`python3 -m compileall -q main.py entropy_arb tests`.
