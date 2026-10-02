# Arcus Explicit Order-Rejection Classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Classify explicit Arcus placeOrder 4xx responses as terminal rejections so B0 and Volume Probe halt without cancel/reconciliation side effects, while preserving fail-closed handling for ambiguous placement outcomes.

**Architecture:** `ArcusMakerClient.place_alo()` will raise a narrow `ArcusOrderRejected` carrying only sanitized status/code/message fields for explicit 4xx application responses. `CalibrationController._place_quote()` will retire the local context, mark the lifecycle `REJECTED`, record `place_rejected`, and halt without calling cancel or reconciliation; all other placement exceptions will continue through the existing unresolved path. Volume Probe will consume the controller's terminal state and complete its flat-state halt normally.

**Tech Stack:** Python 3, asyncio, Decimal, pytest, Ruff, mypy.

**Spec:** Current user request: targeted placement-result classification fix for `feature/volume-probe-v1`, with no new strategy feature and no live network mutation.

## Global Constraints

- Targeted placement-result classification fix only; do not add strategy features.
- Explicit Arcus application 4xx responses, including 401, are authoritative rejection; do not send `cancelOrder`, set terminal reconciliation pending, or retry/requote.
- Rejected placement telemetry must use `place_rejected` and lifecycle state `REJECTED`, with sanitized server error details.
- Never include API key, signature, private key, or complete signed request payload in an exception or log.
- Transport timeout, websocket disconnect, and connection loss after send remain ambiguous and must preserve the existing cancel/reconciliation fail-closed path without duplicate placement.
- Existing 429 reconciliation behavior, B0 behavior, fill observer/idempotency, and volume-probe state transitions remain unchanged.
- No live mutation is part of implementation or validation.
- `main` remains unmerged; commit and push only `feature/volume-probe-v1`.

## Review Focus

- Nested or alternate server error shapes must preserve only safe code/message details and never stringify the full signed request or credentials.
- A 4xx response with no usable error detail must still be a terminal rejection with a concise status-only reason.
- A non-4xx application response and an RPC/transport exception must remain unresolved rather than becoming an authoritative rejection.
- Rejection cleanup must remove both client/order context registrations and the local calibration client identity before the volume-probe terminal barrier runs.
- A delayed websocket fill after a rejected placement must not be treated as a valid fill for the retired rejected lifecycle.

---

### Task 1: Arcus explicit rejection result

**Files:**
- Modify: `entropy_arb/arcus_execution.py`
- Test: `tests/test_phase_b0.py`

**Interfaces:**
- Produces `ArcusOrderRejected(ArcusOrderError)` with `status: int`, `code: str | None`, and `message: str | None`.
- `ArcusMakerClient.place_alo()` raises `ArcusOrderRejected` only for response statuses in `400 <= status < 500`; non-4xx response errors retain the existing generic behavior.

- [x] **Step 1: Write the failing transport tests**

  Add tests using the existing in-memory RPC/signing seam for status 401 and another client rejection such as 400/403. Assert the narrow exception type, status, retained server code/message from top-level or nested error data, and a concise string. Include secret-shaped values in the response detail and assert the exception string does not contain the API key, signature, private key, or full request payload. Add a non-4xx/transport case proving it does not become `ArcusOrderRejected`.

- [x] **Step 2: Run the focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'order_rejected or place_order_rejection'`

  Expected: the new import/type and classification assertions fail because `place_alo()` currently raises only `ArcusOrderError` with status text.

- [x] **Step 3: Implement the minimal sanitized rejection boundary**

  Add a small exception carrying only status/code/message. Extract only scalar values from known response/error fields, sanitize and length-limit text before storing it, and never include the request body, signer output, URL, or raw response representation. Raise it before the generic status error for every 4xx status; leave non-4xx placement errors on their existing generic/ambiguous path. Existing 429 reconciliation behavior refers to REST reconciliation calls and remains unchanged.

- [x] **Step 4: Run the focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'order_rejected or place_order_rejection'`

  Expected: all new classification and sanitization tests pass.

### Task 2: Terminal controller and Volume Probe behavior

**Files:**
- Modify: `entropy_arb/calibration_runtime.py`
- Modify: `tests/test_volume_probe.py`
- Test: `tests/test_phase_b0.py`

**Interfaces:**
- Consumes `ArcusOrderRejected` from Task 1.
- Produces a terminal `REJECTED` local lifecycle and `place_rejected` telemetry for explicit placeOrder 4xx responses.
- Preserves the existing `place_failure` → cancel/reconciliation behavior for timeout, websocket disconnect, and connection-loss exceptions.

- [x] **Step 1: Write the failing controller/probe regressions**

  Extend the existing real `CalibrationController`/`FillAccumulator` probe stack with a maker that raises `ArcusOrderRejected`. Assert for 401 and 400/403 that placement is attempted once, cancel calls are zero, REST reconciliation calls are zero, RH hedge calls are zero, lifecycle state is `REJECTED`, no reconciliation-required marker is set, and telemetry contains `place_rejected` with sanitized status/reason rather than `place_failure` or `CANCEL_SENT`. Run the Volume Probe with flat final-state reads and assert it ends `HALTED`, not `RECONCILIATION_REQUIRED`.

  Add timeout and connection-loss-after-send cases to assert one placement attempt, the existing unresolved halt/cancel path, and no retry/duplicate placement. Preserve the existing 429 and B0 reconciliation tests.

- [x] **Step 2: Run the focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_volume_probe.py tests/test_phase_b0.py -k 'rejected or placement_timeout or connection_loss'`

  Expected: explicit rejection tests fail because the generic `_place_quote()` handler records `place_failure` and calls cancellation; ambiguity tests remain green or expose only test-seam adjustments needed to exercise the existing path.

- [x] **Step 3: Implement minimal rejected-placement cleanup**

  Catch `ArcusOrderRejected` before the generic placement exception in `_place_quote()`. Remove the local order context and calibration client identity, record lifecycle status `REJECTED`, clear the current candidate/execution/context and any local cancel/pending state, halt with the exception's sanitized reason, and record `place_rejected` with `lifecycle_state=REJECTED`. Do not call `cancel_outstanding()` or `reconcile()`. Leave the generic exception handler unchanged for ambiguous placement failures.

- [x] **Step 4: Run the focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_volume_probe.py tests/test_phase_b0.py`

  Expected: all explicit-rejection, ambiguity, 429, B0, and existing volume-probe tests pass.

### Task 3: Full regression and delivery verification

**Files:**
- Modify: only files from Tasks 1–2 and their tests.

**Interfaces:**
- Consumes the explicit rejection and terminal cleanup behavior from Tasks 1–2.
- Produces a clean commit pushed to `feature/volume-probe-v1` with no main merge and no live execution.

- [x] **Step 1: Run the exact validation suite**

  Run:

  ```bash
  python3 -m pytest -q
  python3 -m pytest -q tests/test_volume_probe.py
  ruff check .
  ruff format --check .
  python3 -m mypy entropy_arb tests main.py
  python3 -m compileall -q main.py entropy_arb tests
  ```

  Expected: every command exits zero; record the full and focused test counts.

- [x] **Step 2: Review scope and secret safety**

  Inspect the diff and test output for strategy, market-selection, funding, clip, hold, repeated-round, or live-network changes. Confirm rejection exceptions and telemetry contain only status/code/message and do not expose credentials, signatures, or signed payloads. Confirm the starting branch and target remain `feature/volume-probe-v1` and `main` is not merged.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/arcus_execution.py entropy_arb/calibration_runtime.py tests/test_phase_b0.py tests/test_volume_probe.py docs/superpowers/plans/2026-10-02-arcus-order-rejection.md
  git commit -m "fix: classify explicit Arcus order rejections"
  git push origin feature/volume-probe-v1
  ```

  Report starting SHA, ending SHA, test counts, exact rejection/ambiguity behavior, and that `main` remains unmerged.
