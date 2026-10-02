# Arcus Rate-Limit Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Arcus 429 responses explicit and keep volume-probe reconciliation bounded, rate-limit-aware, and fail-closed.

**Architecture:** `ArcusAccountRest.get()` will expose a narrow `ArcusRateLimited` exception with a parsed `Retry-After`. `CalibrationController.reconcile()` will return a structured outcome and own the next-allowed retry/backoff state while preserving the terminal pending barrier. `VolumeProbeController` will consume that outcome for one bounded terminal barrier and one bounded final-state read, and will mark reconciliation as permanently required before shutdown can retry again.

**Tech Stack:** Python 3, asyncio, aiohttp, Decimal, pytest, Ruff, mypy.

**Spec:** User-provided request pasted on 2026-10-02: “Targeted rate-limit/reconciliation fix only” for `feature/volume-probe-v1`.

## Global Constraints

- Targeted rate-limit/reconciliation fix only; do not add strategy features.
- Do not change market selection, funding, clips, hold, repeated rounds, fill-observer/idempotency, or `vp-` isolation behavior.
- A 429 is temporary/unresolved: keep `_terminal_reconcile_pending`, prevent new quotes, and never treat one 429 as successful reconciliation.
- Without `Retry-After`, use bounded reconciliation delays `1s`, `2s`, `4s`, then `5s` cap; use a supplied valid `Retry-After` preferentially.
- While reconciliation is pending, do not place, reprice, or transition to the next maker leg.
- A bounded deadline ending without authoritative confirmation must produce `RECONCILIATION_REQUIRED` and leave final state unknown.
- Never report `COMPLETED` without authoritative flat Arcus/RH positions and open-order reads.
- No live mutation is part of implementation verification.
- Existing B0 behavior, reconciliation fill observer/idempotency, and `vp-` identity isolation must remain unchanged.

## Review Focus

- Numeric and missing `Retry-After`: 429 retries must use the server delay when valid and `1/2/4/5` bounded backoff otherwise.
- 429 from either `/v1/openOrders` or `/v1/fills`: no REST hot loop, no pending-bit clear, and no replacement quote.
- Persistent 429 during terminal cancellation: one cancel request, finite reconciliation attempts, then one `RECONCILIATION_REQUIRED` transition.
- Shutdown after the probe has already failed: it must reuse the terminal outcome and not start a second retry storm.
- Final-state 429: transient recovery may complete only after both venues are read flat; persistent failure can never become `COMPLETED`.

---

### Task 1: Explicit Arcus rate-limit and reconciliation outcomes

**Files:**
- Modify: `entropy_arb/arcus_execution.py`
- Modify: `entropy_arb/calibration_runtime.py`
- Test: `tests/test_phase_b0.py`
- Test: `tests/test_volume_probe.py`

**Interfaces:**
- Produces `ArcusRateLimited(retry_after: float | None)` without exposing request URLs or credentials in its normal message.
- Produces a structured `ReconciliationResult` with success, rate-limited, and hard-failure states plus an optional retry delay.
- `CalibrationController.reconcile()` retains `_terminal_reconcile_pending` on 429, suppresses REST calls until its next allowed time, and resets its backoff only after successful reconciliation.

- [x] **Step 1: Write failing tests**

  Add transport tests for a 429 response with and without `Retry-After`, assert the narrow exception and parsed duration, and assert non-429 responses keep existing `raise_for_status()` behavior. Add controller tests for `open_orders: 429, 429, success` and `fills: 429, success`, checking structured outcomes, pending preservation, bounded call counts, and no maker/RH mutation.

- [x] **Step 2: Run the focused tests to verify RED**

  Run: `python -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: new exception/outcome assertions fail against the current generic HTTP error and `None` reconciliation API.

- [x] **Step 3: Implement the minimal REST and controller boundary**

  Detect status 429 before `raise_for_status()`, parse only a finite non-negative delay, and raise `ArcusRateLimited`. Add `ReconciliationResult`, `1/2/4/5` backoff state, next-allowed gating, concise rate-limit logging, and a separate hard-failure path that retains the existing risk-halt/traceback behavior. Do not clear pending on temporary or hard failure.

- [x] **Step 4: Run the focused tests to verify GREEN**

  Run: `python -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: all existing B0 tests and new REST/reconciliation tests pass.

### Task 2: Bounded volume-probe terminal and final-state barriers

**Files:**
- Modify: `entropy_arb/volume_probe_runtime.py`
- Modify: `entropy_arb/calibration_runtime.py`
- Modify: `entropy_arb/engine.py`
- Test: `tests/test_volume_probe.py`

**Interfaces:**
- Consumes Task 1 `ArcusRateLimited` and `ReconciliationResult`.
- Produces one bounded terminal reconciliation helper used by both pending-loop handling and `_cancel_terminal()`.
- Produces one bounded final-state reader wrapper that retries transient Arcus 429s but never turns an unresolved read into `COMPLETED`.
- Produces an executor reconciliation-required marker that `shutdown()` recognizes and does not retry again after the probe has already exhausted its budget.

- [x] **Step 1: Write failing tests**

  Add tests for persistent terminal 429s proving no 50ms REST storm, finite request count, one cancel, `RECONCILIATION_REQUIRED`, no new Arcus order, and no duplicate RH hedge. Add shutdown-after-failure coverage, final-state `429 → success` coverage requiring both venue reads to be flat, and persistent final-state 429 coverage that never completes. Preserve existing reconciliation-fill observer/idempotency tests.

- [x] **Step 2: Run the focused tests to verify RED**

  Run: `python -m pytest -q tests/test_volume_probe.py`

  Expected: current 50ms terminal loop and unbounded final-state call path fail the new finite-count/status assertions.

- [x] **Step 3: Implement the bounded barriers**

  Replace `_cancel_terminal()`'s 50ms REST loop with one cancel submission guarded by existing idempotency, then bounded reconciliation waits using the result's next-allowed delay. Make the main pending path use the same helper, so it cannot quote/reprice/advance while unresolved. On deadline, mark reconciliation required once, retain pending, stop the controller, and log the final failure once. Wrap the final reader with the same bounded 429-aware delay policy; only call `finalize_positions()` after a successful authoritative read. Make `CalibrationController.shutdown()` reuse the stored terminal failure/backoff state and skip a second independent REST storm.

- [x] **Step 4: Run the focused tests to verify GREEN**

  Run: `python -m pytest -q tests/test_volume_probe.py`

  Expected: all volume-probe tests, including persistent/transient 429 and shutdown cases, pass.

### Task 3: Full regression and delivery verification

**Files:**
- Modify: only files from Tasks 1–2 and their tests.

**Interfaces:**
- Consumes the completed bounded retry behavior.
- Produces a clean `feature/volume-probe-v1` commit with no main merge and no live execution.

- [x] **Step 1: Run the exact validation suite**

  Run:

  ```bash
  python -m pytest -q
  python -m pytest -q tests/test_volume_probe.py
  ruff check .
  ruff format --check .
  python -m mypy entropy_arb tests main.py
  python -m compileall -q main.py entropy_arb tests
  ```

  Expected: every command exits zero; report total and focused test counts.

- [x] **Step 2: Review the diff and verify scope**

  Confirm no strategy, market-selection, funding, clip, hold, repeated-round, or live-network mutation changes; confirm no credentials/full request URLs appear in expected 429 logs.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/arcus_execution.py entropy_arb/calibration_runtime.py entropy_arb/volume_probe_runtime.py entropy_arb/engine.py tests/test_phase_b0.py tests/test_volume_probe.py docs/superpowers/plans/2026-10-02-arcus-rate-limit-reconciliation.md
  git commit -m "fix: bound Arcus reconciliation retries"
  git push origin feature/volume-probe-v1
  ```

  Report starting SHA, ending SHA, exact retry/backoff semantics, test counts, and that `main` remains unmerged.
