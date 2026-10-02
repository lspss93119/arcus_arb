# Arcus Partial-Fill Minimum-Quote Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent RH hedges for partial Arcus exposure until the aligned aggregate quantity satisfies RH minimum base and minimum quote notional, while retaining the residual safely across fills, reconciliation, and terminal paths.

**Architecture:** Extend the existing `FillAccumulator` with optional live `rh_min_quote` metadata and a fresh hedge-side reference price for the executable quantity decision. `CalibrationController` will pass `hedge.min_quote`, use fresh RH ask for Arcus SELL/RH BUY and fresh RH bid for Arcus BUY/RH SELL both when accumulating and immediately before `send_taker()`, returning a now-ineligible instruction to residual without recording a hedge failure. `VolumeProbeController` will preserve its existing phase accounting and explicitly force `RECONCILIATION_REQUIRED` whenever a terminal run still has residual exposure.

**Tech Stack:** Python 3, asyncio, Decimal, pytest, Ruff, mypy.

**Spec:** User-provided partial-fill minimum-quote safety request pasted in `/Users/liaoyuchen/.codex/attachments/c035ee78-1326-4b0b-977a-202331aa41db/貼上的文字.txt`.

## Global Constraints

- Targeted partial-fill minimum-notional fix only; do not add strategy features or change quote selection, sizing, authorization, or live gates.
- Never increase RH hedge quantity beyond actual accumulated Arcus exposure; align down to `rh_step` and retain any remainder in `FillAccumulator`.
- A hedge becomes eligible only when aligned quantity satisfies RH minimum base, RH size step, and `quantity * fresh hedge-side reference >= rh_min_quote`.
- Arcus SELL → RH BUY uses fresh RH ask; Arcus BUY → RH SELL uses fresh RH bid. Do not use Arcus price for RH min-quote eligibility.
- Re-read fresh RH BBO immediately before `send_taker()`; if the quantity no longer satisfies min quote, retain it and do not call `send_taker()` or record `hedge_failure`.
- Read `hedge.min_quote` from the live hedge object; never hard-code `$10`. If a test/dummy hedge does not expose it, preserve existing B0 behavior by treating the optional constraint as absent.
- REST-recovered fills and websocket fills use the same `CalibrationController.on_fill()` path; `AccountState.should_hedge_fill` idempotency remains unchanged.
- Residual exposure at probe termination must not produce `DONE`/`COMPLETED`; it remains `RECONCILIATION_REQUIRED` unless authoritative final state resolves it.
- No live mutation; commit and push only `feature/volume-probe-v1`; `main` remains unmerged.

## Review Focus

- A quantity aligned to RH step but below min quote remains residual and does not invoke `send_taker()`.
- A fresh price drop after accumulation but before submission returns the instruction to residual without a generic hedge failure or over-hedge.
- Aggregate hedges may cover multiple Arcus fills; Volume Probe metrics must not double-count or reject the aggregate hedge.
- REST replay of an already-accounted partial fill must not increase residual or send a second hedge.
- A below-min residual during BUILD/UNWIND timeout, cancellation, or finalization must remain reconciliation-required even when venue positions read flat.

---

### Task 1: Add failing accumulator, runtime, reconciliation, and terminal regressions

**Files:**
- Modify: `tests/test_phase_b0.py`
- Modify: `tests/test_volume_probe.py`

**Interfaces:**
- Exercises `FillAccumulator(rh_min_qty, rh_step, rh_min_quote)` with Decimal hedge-side references.
- Exercises the real `CalibrationController`/`FillAccumulator` stack for websocket-like and REST-recovered fills.
- Exercises `VolumeProbeController` finalization with residual exposure.

- [x] **Step 1: Write the failing tests**

  Add direct accumulator tests for HYPE-like `.100` at `$89` (no instruction, residual `.100`), accumulation at `$50` until `.200` produces exactly one aligned instruction, and both SELL/BUY directions. Extend the in-memory reconciliation stack with configurable RH bid/ask, size decimals, min base, and min quote. Add tests that verify: SELL uses ask, BUY uses bid, REST-recovered partial fills retain residual then produce one aggregate hedge when eligible, duplicate replay does not hedge twice, and a price move between accumulation and send retains the instruction without a hedge failure. Add a terminal/finalization regression proving flat venue reads plus nonzero residual returns `RECONCILIATION_REQUIRED`, not `COMPLETED`.

- [x] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'min_quote or partial_fill or residual or recovered or duplicate or finalization'`

  Expected: new min-quote and terminal residual assertions fail because the accumulator releases at `rh_min_qty`, the runtime does not pass a min quote/reference, and terminal residual handling can remain halted/non-completed without the specified reconciliation status.

### Task 2: Implement min-quote-aware accumulation and fresh pre-submit recheck

**Files:**
- Modify: `entropy_arb/calibration.py`
- Modify: `entropy_arb/calibration_runtime.py`

**Interfaces:**
- `FillAccumulator` gains optional `rh_min_quote` and a Decimal hedge reference argument while retaining the existing no-min-quote API behavior.
- `CalibrationController` derives `hedge.min_quote`, passes it to the accumulator, and retains an ineligible aggregate instruction through a small residual method.

- [x] **Step 1: Change `FillAccumulator` minimally**

  Add `rh_min_quote: Decimal | None = None`; after adding a fill and flooring to `rh_step`, return `None` unless aligned quantity meets `rh_min_qty` and, when configured, `hedge_qty * hedge_reference_price >= rh_min_quote`. Keep residual unchanged whenever no instruction is eligible. Add a narrow method to return an instruction quantity to the same-side residual for the pre-submit price recheck; use it for existing partial RH fill residual accounting where appropriate.

- [x] **Step 2: Wire fresh hedge-side references into `CalibrationController`**

  Derive optional `hedge.min_quote` from the live hedge object in `__init__` and construct `FillAccumulator` with it. Before `add_fill()`, read a fresh RH BBO when min quote is configured and pass ask for RH BUY or bid for RH SELL. In `_hedge_instruction()`, keep the existing freshness/BBO safety gates, then recheck the aligned instruction quantity against the same min quote using the fresh hedge-side reference before calling `send_taker()`. If now below minimum, retain the quantity, return normally, and avoid `hedge_failure`, cancel escalation, or RH mutation.

- [x] **Step 3: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'min_quote or partial_fill or residual or recovered or duplicate or finalization'`

  Expected: all new accumulator, side-price, REST replay, price-move, and aggregate hedge tests pass; existing B0 behavior without `min_quote` remains green.

### Task 3: Make terminal Volume Probe residual handling fail closed

**Files:**
- Modify: `entropy_arb/volume_probe_runtime.py`
- Modify: `tests/test_volume_probe.py`

**Interfaces:**
- Consumes `FillAccumulator.residual_exposure` from Task 2.
- Produces `ProbeStatus.RECONCILIATION_REQUIRED` whenever terminal/final run state still contains unresolved residual exposure.

- [x] **Step 1: Add the terminal residual guard**

  In the Volume Probe run/finalization path, check residual exposure before assigning `HALTED`/`COMPLETED` status. If residual exceeds tolerance, set a concise unresolved-residual reason, transition to `RECONCILIATION_REQUIRED`, and never allow `DONE`/`COMPLETED`; retain existing authoritative final-state checks and normal flat behavior.

- [x] **Step 2: Run focused probe tests**

  Run: `python3 -m pytest -q tests/test_volume_probe.py`

  Expected: the full probe suite passes, including the new terminal residual regression and existing BUILD/UNWIND completion tests.

### Task 4: Full validation and delivery

**Files:**
- Modify: only files from Tasks 1–3 and this plan/ledger.

**Interfaces:**
- Preserves existing rejection, 429 reconciliation, wire-format, B0, and volume-probe behavior.
- Produces a clean pushed branch with no live execution.

- [x] **Step 1: Run the exact validation suite**

  Run:

  ```bash
  python3 -m pytest -q
  python3 -m pytest -q tests/test_phase_b0.py
  python3 -m pytest -q tests/test_volume_probe.py
  ruff check .
  ruff format --check .
  python3 -m mypy entropy_arb tests main.py
  python3 -m compileall -q main.py entropy_arb tests
  ```

  Expected: every command exits zero; record full and focused test counts.

- [x] **Step 2: Review scope and safety semantics**

  Confirm no strategy or live-gate changes, no hard-coded min quote, no RH call when below min quote, aggregate hedge never exceeds residual Arcus exposure, websocket/REST duplicate idempotency remains intact, and `main` is not merged.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/calibration.py entropy_arb/calibration_runtime.py entropy_arb/volume_probe_runtime.py tests/test_phase_b0.py tests/test_volume_probe.py docs/superpowers/plans/2026-10-02-arcus-fill-min-quote.md
  git commit -m "fix: gate partial RH hedges by minimum quote"
  git push origin feature/volume-probe-v1
  ```

  Report starting SHA, ending SHA, exact min-quote aggregation semantics, test counts, and that no live trading occurred.
