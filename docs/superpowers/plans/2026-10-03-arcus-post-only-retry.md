# Arcus Zero-Fill Post-Only Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (recommended) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Treat only an exactly proven zero-fill `REJECTED + POST_ONLY_WOULD_CROSS` Arcus order as a bounded Volume Probe retry, while keeping every other rejection fail-closed.

**Architecture:** Preserve `filledSize` and `rejectionReason` in `ArcusOrderUpdate` and expose a pure predicate that requires every zero-fill identity field. `CalibrationController` records the retryable terminal event and keeps its existing reconciliation barrier; `VolumeProbeController` owns the 1-second cooldown, fresh-BBO replacement, per-phase consecutive counter, and local would-cross handling. The existing ambiguous placement, hedge, reconciliation, and order-signing paths remain unchanged.

**Tech Stack:** Python 3, asyncio, Decimal, pytest, Ruff, mypy.

**Spec:** User request pasted in `/Users/liaoyuchen/.codex/attachments/f6a7512b-4e33-48da-b35e-dfdf2c72e3f0/貼上的文字.txt`.

## Global Constraints

- Fix one observed live reliability issue only; do not redesign the builder, sizing, hedge logic, fees, loss limits, reconciliation ownership, or order type.
- `is_retryable_post_only_reject(update)` is true only for status `REJECTED`, exact reason `POST_ONLY_WOULD_CROSS`, `filled_size == 0`, and exact `original_size == remaining_size`, with no missing fields.
- Every other Arcus rejection, including missing/positive fill evidence or a changed remaining size, remains fatal.
- An asynchronous retryable rejection must complete the existing terminal REST reconciliation before replacement; reconciliation failure/rate limiting past the existing deadline remains `RECONCILIATION_REQUIRED`.
- Volume Probe retry delay is at least `1.0` second from the rejection, uses a newly read BBO/candidate, and halts after `5` consecutive retryable post-only rejects in one phase.
- Local `ArcusAloWouldCross` creates no surviving order context, does not halt, does not log a placed quote, and uses the same cooldown/counter path.
- Preserve B0 behavior except for the explicitly required shared retryable rejection policy; preserve historical-fill watermark/idempotency, 429 handling, and all non-post-only rejection behavior.
- No live exchange tests or mutations; commit and push only `feature/volume-builder-v2`; `main` remains unmerged.

## Review Focus

- A retryable websocket rejection may be replayed by terminal reconciliation; the same order must count once and still reconcile before replacement. Test duplicate order delivery.
- A zero-fill predicate with any missing or inconsistent size evidence must halt, never retry. Test missing `filledSize`, positive `filledSize`, and `remainingSize != originalSize`.
- A local would-cross has no exchange order and must not leave a lifecycle/context that later receives a fill. Test no context, no false placement log, and fresh retry.
- A normal `OPEN` or `PARTIALLY_FILLED` update must reset the phase counter; test that a later five-reject sequence starts from zero.
- A retryable reject followed by REST-discovered fill or failed/rate-limited reconciliation must not silently place a replacement. Test both actionable race safety and existing deadline behavior.

---

### Task 1: Preserve order rejection evidence and classify exact zero-fill rejects

**Files:**
- Modify: `entropy_arb/arcus_execution.py`
- Test: `tests/test_phase_b0.py`

**Interfaces:**
- Extend `ArcusOrderUpdate` with `filled_size: Decimal | None = None` and `rejection_reason: str | None = None` without breaking existing direct constructors.
- Parse only Arcus `filledSize` and `rejectionReason` fields.
- Produce `is_retryable_post_only_reject(update: ArcusOrderUpdate) -> bool`.

- [x] **Step 1: Write failing parser and predicate tests**

  Add a parser fixture with `status=REJECTED`, `originalSize=0.227`, `remainingSize=0.227`, `filledSize=0`, and `rejectionReason=POST_ONLY_WOULD_CROSS`; assert both fields are preserved as `Decimal("0")` and the exact string. Add parametrized negative cases for missing `filledSize`, positive `filledSize`, changed remaining size, another reason, and another status.

- [x] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'post_only or rejection_predicate'`

  Expected: the new fields/helper are absent or the predicate cases fail.

- [x] **Step 3: Implement the minimal parser and pure predicate**

  Add the two optional dataclass fields at the end, parse the exact camel-case fields with existing Decimal/text helpers, and implement an all-conditions predicate with no reason inference or fallback aliases.

- [x] **Step 4: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'post_only or rejection_predicate'`

  Expected: all parser and exact-predicate tests pass.

### Task 2: Keep shared controller rejection policy fail-closed and expose submission outcome

**Files:**
- Modify: `entropy_arb/calibration_runtime.py`
- Test: `tests/test_phase_b0.py`
- Test: `tests/test_volume_probe.py`

**Interfaces:**
- `CalibrationController._place_quote(...) -> bool` and `place_quote(...) -> bool` return `True` only after Arcus acknowledged submission; local would-cross and all failed placement paths return `False`.
- Retryable asynchronous `REJECTED` updates set `_terminal_reconcile_pending=True` for the current context, record `post_only_reject` telemetry with client/order and reason in existing safe telemetry fields, and do not halt.
- All other `REJECTED` updates preserve the existing fatal halt path exactly.

- [x] **Step 1: Write failing controller policy tests**

  Using the real `CalibrationController`/`FillAccumulator` stack, place an order, deliver exact zero-fill `POST_ONLY_WOULD_CROSS`, and assert lifecycle `REJECTED`, risk not halted, terminal reconciliation pending, one `post_only_reject` event, and no RH hedge. Add negative parametrized updates for positive/missing/inconsistent fill evidence and other reasons, asserting fatal halt. Add a local `ArcusAloWouldCross` test asserting `place_quote()` returns false, no live order/context/client identity remains, and no halt.

- [x] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'post_only or would_cross or rejected_order'`

  Expected: parser/policy tests fail because all rejects currently halt and local placement has no submission result.

- [x] **Step 3: Implement the smallest shared-controller change**

  Import the pure predicate. In `on_order()`, retain normal order telemetry/lifecycle recording, then branch only the exact predicate to set the current terminal reconciliation barrier and emit `post_only_reject`; leave all other rejected updates on `risk.halt("Arcus calibration order rejected")`. Return booleans from `_place_quote()`/`place_quote()` while preserving the existing explicit rejection and ambiguous placement cleanup/halts; only the local would-cross branch returns false without a surviving context.

- [x] **Step 4: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py -k 'post_only or would_cross or rejected_order'`

  Expected: exact zero-fill policy passes, all other rejection cases still halt, and ambiguous placement tests remain fail-closed.

### Task 3: Add bounded Volume Probe retry/cooldown and reconciliation barrier behavior

**Files:**
- Modify: `entropy_arb/volume_probe_runtime.py`
- Test: `tests/test_volume_probe.py`

**Interfaces:**
- Add `POST_ONLY_RETRY_DELAY_SEC = 1.0` and `MAX_CONSECUTIVE_POST_ONLY_REJECTS = 5`.
- `VolumeProbeController.on_order()` observes the shared policy, deduplicates repeated terminal updates for one order, increments the current phase counter once, resets it on current-order `OPEN`/`PARTIALLY_FILLED`, and halts at five before a sixth quote.
- `place_next_quote()` logs placement and starts the reprice timer only when the executor actually submitted; local non-submission schedules the same cooldown and fresh retry.

- [x] **Step 1: Write failing retry lifecycle tests**

  Add tests proving an async retryable reject is not replaced before `reconcile()` completes or before one second; after the gates it submits a new client/execution identity and uses a changed current BBO. Test duplicate reject delivery counts once, a normal `OPEN`/`PARTIALLY_FILLED` resets the counter, and five consecutive rejects halt with no sixth placement. Add local would-cross coverage for no false quote log, no halt, same cooldown, fresh BBO, and new client identity. Add reconcile-failure/rate-limit deadline assertions that remain `RECONCILIATION_REQUIRED`, plus existing historical-fill watermark cases.

- [x] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_volume_probe.py -k 'post_only or would_cross or retry or cooldown'`

  Expected: current controller immediately retries/logs local non-submissions and lacks post-only retry state, so the new timing/count assertions fail.

- [x] **Step 3: Implement the Volume Probe-only retry state machine**

  Track a monotonic retry-not-before timestamp, per-phase consecutive count, and seen reject identities. Gate replacement on `reprice_allowed` first, then the cooldown; compute `_runtime_candidate()` only at the actual retry, so it reads fresh books. Treat a real `False` submission result as local would-cross, schedule the cooldown, and suppress the placed log. For async rejects, let the existing `_reconcile_terminal()` clear the controller barrier before the cooldown gate. At the fifth unique reject, halt the probe and risk before any sixth placement; reset retry state at phase changes and current-order normal open/partial updates. Keep ordinary 30-second reprice logic unchanged.

- [x] **Step 4: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_volume_probe.py -k 'post_only or would_cross or retry or cooldown'`

  Expected: all retry timing, fresh-BBO, identity, deduplication, reset, and fail-closed reconciliation tests pass.

### Task 4: Full validation and delivery

**Files:**
- Modify: only files from Tasks 1–3 and this plan.

**Interfaces:**
- Produces a clean commit pushed to `feature/volume-builder-v2` with no live exchange mutation and no `main` merge.

- [x] **Step 1: Run the exact validation suite**

  Run:

  ```bash
  python3 -m pytest -q
  ruff check .
  ruff format --check .
  python3 -m mypy entropy_arb tests main.py tools
  python3 -m compileall -q main.py entropy_arb tests tools
  git diff --check
  ```

  Expected: every command exits zero; report the full test count and focused regression count.

- [x] **Step 2: Review scope and safety**

  Inspect the diff for no strategy, sizing, hedge, fee, loss-limit, order-type, or builder architecture changes. Confirm exact predicate fields, one-second/five-reject bounds, no false placement telemetry, and unchanged fatal behavior for every other rejection.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/arcus_execution.py entropy_arb/calibration_runtime.py entropy_arb/volume_probe_runtime.py tests/test_phase_b0.py tests/test_volume_probe.py docs/superpowers/plans/2026-10-03-arcus-post-only-retry.md
  git commit -m "fix: retry zero-fill Arcus post-only rejects"
  git push origin feature/volume-builder-v2
  ```

  Report starting SHA, ending SHA, files changed, tests passed, the exact predicate, retry delay/max behavior, confirmation that all non-post-only rejects remain fatal, and that `main` remains unmerged.
