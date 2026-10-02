# Arcus Pending Hedge Reservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve executable Arcus exposure as an explicit RH hedge reservation until the RH result is authoritative, so failed or ambiguous hedges cannot appear flat or be retried.

**Architecture:** Extend `FillAccumulator` with `pending_hedge_qty` and explicit reservation settlement/rollback operations. `CalibrationController` reserves before `send_taker()`, settles full/partial/definitive-zero outcomes, and leaves ambiguous outcomes pending. `VolumeProbeController` treats both unhedged and pending quantities as unresolved at termination; existing min-quote behavior rolls back a reservation before any RH send.

**Tech Stack:** Python 3, asyncio, Decimal, pytest, Ruff, mypy.

**Spec:** User-provided pending-hedge lifecycle request pasted in `/Users/liaoyuchen/.codex/attachments/b4c5dd3d-3af9-48e4-bdac-28d91d009109/貼上的文字.txt`.

## Global Constraints

- Do not perform live trading or add strategy, market-selection, funding, retries, repeated rounds, or automatic exposure repair behavior.
- An emitted `FillInstruction` is reserved, not resolved: `unhedged_qty + pending_hedge_qty` remains unresolved exposure until an authoritative RH outcome settles it.
- Authoritative full fill clears the reservation; authoritative partial/zero fill clears the reservation and restores only the unfilled quantity to `unhedged_qty`.
- Timeout, `sent-unconfirmed`, `unknown`, transport exception, invalid/no-authoritative-fill, and other genuinely ambiguous outcomes leave the reservation pending and do not retry.
- A pre-send min-quote price failure is not an RH execution outcome: roll the entire reservation back to `unhedged_qty`, leave `pending_hedge_qty` at zero, and preserve the existing no-send behavior.
- Preserve SELL→RH BUY ask, BUY→RH SELL bid, fresh-BBO, REST recovery, duplicate idempotency, B0 compatibility, 429, rejection, and wire-format behavior.
- Pending or unhedged exposure must prevent `DONE`/`COMPLETED` unless a future authoritative reconciliation explicitly settles the pending reservation; no such reconciliation is invented in this task.

## Review Focus

- A transport exception after a possible RH send keeps the full quantity pending and never retries it.
- A result with `filled_base > 0` but no authoritative average price remains pending rather than being treated as a partial fill.
- A definitive zero-fill rejection restores exactly the reserved quantity once and does not leave a stale pending reservation.
- `reset_if_flat()` cannot clear the side or permit an opposite order while a reservation remains pending.
- Flat venue positions alone do not silently clear a local pending reservation during Volume Probe finalization.

---

### Task 1: Add failing reservation lifecycle regressions

**Files:**
- Modify: `tests/test_phase_b0.py`
- Modify: `tests/test_volume_probe.py`

**Interfaces:**
- Exercises `FillAccumulator.pending_hedge_qty`, `residual_exposure`, and the eventual reservation settlement API.
- Exercises the real `CalibrationController` stack with authoritative full, partial, definitive-zero, ambiguous, exception, REST, duplicate, and min-quote paths.
- Exercises `VolumeProbeController.finalize_positions()` with pending exposure.

- [x] **Step 1: Write the failing tests**

  Add direct accumulator tests for reservation creation, authoritative full/partial/zero settlement, and ambiguous pending retention. Extend the in-memory RH fake so each test can return a selected result or raise. Add tests proving no second hedge after ambiguous/zero outcomes, REST recovery and duplicate replay remain idempotent, min-quote rollback leaves `unhedged_qty` full with `pending_hedge_qty == 0`, and finalization with pending quantity returns `RECONCILIATION_REQUIRED`.

- [x] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'reservation or pending or ambiguous or zero_fill or partial_hedge or min_quote or recovered or duplicate or finalization'`

  Expected: new tests fail because the accumulator has no pending reservation and current hedge failures leave emitted instruction quantity absent from exposure accounting.

### Task 2: Implement explicit accumulator reservation semantics

**Files:**
- Modify: `entropy_arb/calibration.py`
- Modify: `tests/test_phase_b0.py`

**Interfaces:**
- Produces `FillAccumulator.pending_hedge_qty: Decimal` and an unresolved-exposure value used by runtime/finalization.
- Produces narrow reserve, authoritative-settle, and pre-send-rollback operations; no retry behavior.

- [x] **Step 1: Add `pending_hedge_qty` and lifecycle methods**

  Make `add_fill()` move eligible quantity from `unhedged_qty` to `pending_hedge_qty`. Keep min-quote ineligible fills entirely unhedged. Add methods to settle a reserved instruction with authoritative `filled_qty`, restore a definitive unfilled quantity, and roll back a reservation that was never sent. Validate quantities and never allow pending to become negative.

- [x] **Step 2: Preserve side/reset and residual compatibility**

  Treat `unhedged_qty + pending_hedge_qty` as unresolved exposure for existing safety gates, while keeping the raw fields available for exact assertions. `reset_if_flat()` clears the side only when both are zero.

- [x] **Step 3: Run accumulator-focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'reservation or pending or partial_fill or min_quote'`

  Expected: full/partial/zero and min-quote accumulator tests pass, while existing B0 tests remain green.

### Task 3: Settle or retain reservations through RH execution and Volume Probe finalization

**Files:**
- Modify: `entropy_arb/calibration_runtime.py`
- Modify: `entropy_arb/venue_lighter.py`
- Modify: `entropy_arb/volume_probe_runtime.py`
- Modify: `tests/test_volume_probe.py`

**Interfaces:**
- Consumes the accumulator lifecycle methods from Task 2.
- Produces authoritative full/partial/zero settlement, ambiguous pending retention, and pre-send rollback without retry.
- Keeps final Volume Probe status reconciliation-required while pending or unhedged exposure remains.

- [x] **Step 1: Classify RH results without changing strategy behavior**

  In `_hedge_instruction()`, settle only validated authoritative fills, restore definitive zero-fill rejections, leave ambiguous result/exception reservations pending, and call existing halt/cancel safety handling. Mark Lighter transport exceptions unresolved so a `send-failed` caused by uncertain transport is not mistaken for a definitive rejection; explicit response rejections remain definitive zero-fill outcomes.

- [x] **Step 2: Roll back only the no-send min-quote path**

  When the immediate fresh-BBO min-quote recheck fails before `send_taker()`, roll back the full reservation and retain it as unhedged residual. Do not create pending exposure or `hedge_failure` for that path.

- [x] **Step 3: Make Volume Probe finalization pending-aware**

  Ensure runtime residual guards and `finalize_positions()` inspect total unresolved exposure, so pending reservations cannot yield `DONE`/`COMPLETED` from flat local positions. Keep REST/websocket duplicate accounting unchanged and add assertions for build/unwind-safe behavior where applicable.

- [x] **Step 4: Run focused lifecycle tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'reservation or pending or ambiguous or zero_fill or partial_hedge or min_quote or recovered or duplicate or finalization'`

  Expected: all new lifecycle tests pass and existing B0/Volume Probe behavior remains green.

### Task 4: Full validation and delivery

**Files:**
- Modify: only files from Tasks 1–3 and this plan/ledger.

**Interfaces:**
- Preserves existing min-quote, B0, rejection, 429, wire-format, and Volume Probe behavior.
- Produces a clean pushed `feature/volume-probe-v1` branch with no live execution and no `main` merge.

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

  Confirm no retry or strategy additions, ambiguous outcomes remain pending, definitive zero/partial outcomes restore only unfilled quantity, min-quote rollback has no pending reservation, and finalization cannot complete with pending exposure.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/calibration.py entropy_arb/calibration_runtime.py entropy_arb/venue_lighter.py entropy_arb/volume_probe_runtime.py tests/test_phase_b0.py tests/test_volume_probe.py docs/superpowers/plans/2026-10-02-arcus-pending-hedge-reservation.md
  git commit -m "fix: track pending RH hedge reservations"
  git push origin feature/volume-probe-v1
  ```

  Report starting SHA, ending SHA, exact unhedged/pending semantics, test counts, and that no live trading occurred.
