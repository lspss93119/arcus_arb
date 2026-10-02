# Arcus Scheme-1 Wire-Format Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the Arcus Scheme-1 place/cancel request body fields and clientId signing canonicalization without changing strategy behavior, safety gates, or the websocket envelope.

**Architecture:** Keep `ArcusMakerClient` as the only wire-format boundary. `place_alo()` will send the documented body timestamp as an integer, `reduceOnly=False`, and the existing microsecond `goodTilTime`; `cancel_calibration_order()` will include the same request timestamp in its body. The Scheme-1 auth helpers will lowercase only the canonical signed clientId field `c`, while generated `b0`/`vp` identities, ownership checks, and websocket envelope structure remain unchanged.

**Tech Stack:** Python 3, asyncio, Decimal, pytest, Ruff, mypy.

**Spec:** Current user request for `feature/volume-probe-v1`; no live network mutation and no merge to `main`.

## Global Constraints

- Targeted wire-format correctness fix only; do not add strategy features or alter quote sizing, phase transitions, reconciliation, hedging, approval, or isolation behavior.
- Preserve Scheme-1 canonical signing semantics: `ct=timestamp_ns`, `g=goodTilTime_us*1000`, `op=1/2`, integer price/quantity units, `r=0`, `s=BUY0/SELL1`, `t=ALO3`, `v=1`.
- Preserve the websocket post envelope, including outer `request.timestamp` as a string and its existing request/signature fields.
- Place body schema must be exactly: `address`, `accountIndex`, `marketId`, `orderSide`, `orderType`, `quantity`, `price`, `timeInForce`, `goodTilTime`, `timestamp`, `reduceOnly`, `clientId`.
- Cancel body must retain ownership/isolation fields and add `timestamp`; order-id and client-id cancellation forms remain distinct.
- Do not include credentials, signatures, or signed payloads in tests or logs beyond local assertions of the existing fake seam.
- Keep `GOOD_TIL_TIME_DAYS` unchanged; the optional lifetime adjustment is outside this targeted fix.
- No live mutation; commit and push only `feature/volume-probe-v1`; `main` remains unmerged.

## Review Focus

- The place body, websocket envelope timestamp, and signed `ct` must all derive from the same frozen nanosecond value.
- `goodTilTime` remains the documented microsecond string in the body while the canonical signer continues converting it to nanoseconds.
- `reduceOnly` is explicitly `False`, and obsolete `clientTime` is absent.
- Mixed-case client IDs canonicalize to lowercase in signed `c`; generated IDs and caller-facing identity behavior are otherwise unchanged.
- Cancel body timestamp, outer envelope timestamp, and signed `ct` use the same value.
- Existing B0, 429 reconciliation, explicit rejection, ambiguity, volume-probe, and safety tests remain green.

---

### Task 1: Add failing wire-format and signer regressions

**Files:**
- Modify: `tests/test_phase_b0.py`

**Interfaces:**
- Captures the real in-memory `ArcusAccountFeed` websocket post envelope and fake signer payloads.
- Verifies exact place/cancel body schemas, shared timestamps, and lowercase Scheme-1 clientId canonicalization.

- [x] **Step 1: Write the failing tests**

  Strengthen `test_arcus_maker_client_sends_only_publicly_documented_alo_payload` with frozen time and a fake websocket/signing seam. Assert the complete place body, no `clientTime`, `timestamp` as an integer, `reduceOnly=False`, expected `goodTilTime`, exact address/accountIndex/marketId/side/type/quantity/price/TIF/clientId, unchanged websocket envelope timestamp, and signed `ct`/`g` values. Add a cancel regression asserting body timestamp, envelope timestamp, signed `ct`, and the exact order-id cancellation body. Add a mixed-case clientId signing regression for the canonical lowercase `c` field.

- [x] **Step 2: Run the focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'publicly_documented_alo_payload or arcus_maker_client_cancel_wire_format or ordersign_client_id'`

  Expected: the new assertions fail because the place body still sends `clientTime`, omits `reduceOnly`, cancel omits body timestamp, and the signing helper preserves mixed-case `c`.

### Task 2: Implement the smallest wire-format correction

**Files:**
- Modify: `entropy_arb/arcus_execution.py`
- Modify: `entropy_arb/arcus_auth.py`

**Interfaces:**
- `place_alo()` sends the exact documented body while retaining existing signing and response handling.
- `cancel_calibration_order()` adds only the missing body timestamp.
- Scheme-1 order/cancel signing helpers lowercase canonical clientId `c`.

- [x] **Step 1: Change only the request boundary**

  Replace `clientTime` with integer `timestamp_ns` and add `reduceOnly: False` to the place body. Add integer `timestamp_ns` to the cancel body. Lowercase `client_id` only when populating signed canonical `c` in the order and cancel helpers; preserve generated IDs, body identity behavior, ownership checks, and all existing lifecycle logic. Do not modify `ArcusAccountFeed.post()`.

- [x] **Step 2: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py -k 'publicly_documented_alo_payload or cancel.*timestamp or client_id.*lower|ordersign.*lower'`

  Expected: all new wire-format and signer regressions pass.

### Task 3: Full regression, review, commit, and push

**Files:**
- Modify: only files from Tasks 1–2 and this plan/ledger.

**Interfaces:**
- Consumes the corrected wire boundary with all prior safety behavior intact.
- Produces a clean commit pushed to `feature/volume-probe-v1` with no live execution.

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

  Expected: every command exits zero; record the full and focused test counts.

- [x] **Step 2: Review scope and final schemas**

  Inspect the diff for strategy/lifecycle/risk changes, verify websocket envelope code is unchanged, confirm the exact final place/cancel body schemas and timestamp relationships, and confirm no live API calls were made.

- [x] **Step 3: Commit and push**

  ```bash
  git add entropy_arb/arcus_auth.py entropy_arb/arcus_execution.py tests/test_phase_b0.py docs/superpowers/plans/2026-10-02-arcus-wire-format.md
  git commit -m "fix: align Arcus Scheme-1 wire format"
  git push origin feature/volume-probe-v1
  ```

  Report starting SHA, ending SHA, exact final place/cancel body schemas, test counts, and that `main` remains unmerged.
