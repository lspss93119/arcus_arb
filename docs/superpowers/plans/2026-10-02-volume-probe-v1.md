# Volume Probe V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a gated, one-shot Arcus maker → Lighter-RH hedge → maker-unwind volume probe while preserving B0's fixed `0.01`/`b0-` behavior.

**Architecture:** Put pure sizing, candidate, state, phase metrics, and CSV-row rules in `entropy_arb/volume_probe.py`. Add narrow generic parameters/hooks to the existing B0 controller and signed Arcus maker so the new controller reuses authoritative user-fill, hedge, cancel, and reconciliation paths. Wire a separate Engine runtime and CLI gate; do not merge it into the mature strategy engine.

**Tech Stack:** Python 3, asyncio, Decimal, existing Arcus account websocket/REST adapters, existing LighterVenue IOC adapter, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-10-02-volume-probe-v1-design.md`

## Global Constraints

- `--volume-probe` cannot be combined with `--record-only` or `--tiny-live`.
- Live mutation requires `--volume-probe --confirm-mainnet --approve-first-order`.
- Without `--approve-first-order`, run market/account preflight, show the proposal, write `PREORDER_ONLY`, and perform no mutation RPC.
- Arcus V1 orders are only LIMIT + ALO; no taker fallback or emergency-close order.
- BUILD and UNWIND use real Arcus `userFills`, `FillAccumulator`, authoritative RH IOC fills, duplicate protection, reconciliation, stale/account health gates, and the 20 bps RH hard cap.
- At most one `vp-` Arcus order is live; cancel and terminally reconcile before reprice.
- B0 keeps `0.01` quantity, `b0-` IDs, and existing tests/behavior.
- No live network execution is part of implementation verification.

## Review Focus

- A tiny clip that rounds below either venue's executable minimum must fail preflight without an order — Task 1 tests quantity validation.
- A stale or crossing BBO must never produce a taker/crossing candidate — Task 1 tests fresh best-side candidate and ALO safety.
- A cancel/fill race must not cause overlapping probe orders — Task 3 tests terminal cancel barrier before reprice.
- A partial or unresolved RH hedge must halt and never advance to unwind — Task 3 tests fail-closed phase handling.
- A non-flat final read must not be reported as completed or force a market close — Task 3 tests final reconciliation status.

---

### Task 1: Pure volume-probe rules and round telemetry

**Files:**
- Create: `entropy_arb/volume_probe.py`
- Create: `tests/test_volume_probe.py`

**Interfaces:**
- Consumes: `Decimal`, Arcus metadata values, `QuoteCandidate` only where useful for existing telemetry compatibility.
- Produces: `ProbeSide`, `ProbeState`, `ProbeStatus`, `ProbeConfig`, `ProbeCandidate`, `compute_probe_quantity(...)`, `build_probe_candidate(...)`, `unwind_side(...)`, `ProbeRoundMetrics`, and `VolumeProbeRoundWriter.append(...)`.

- [ ] **Step 1: Write failing pure tests**

  Add tests for USD-to-step quantity, min/max/min-notional/RH validation, BUY-at-best-bid and SELL-at-best-ask, non-crossing ALO safety, side reversal, state transition guards, and stable CSV header/status serialization.

- [ ] **Step 2: Run the focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_volume_probe.py`

  Expected: collection or assertion failures because the new module/interfaces do not exist.

- [ ] **Step 3: Implement the pure rules**

  Use Decimal floor rounding. Compute mid from fresh Arcus bid/ask, reject invalid/stale inputs before producing a candidate, validate both venue grids and all available Arcus constraints, set build hedge side to the opposite side, and construct unwind candidates with the exact reverse side. Keep the CSV writer append-only, create only its parent directory, and emit the exact round fields from the spec.

- [ ] **Step 4: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_volume_probe.py`

  Expected: all pure volume-probe tests pass with no live/network fixture.

- [ ] **Step 5: Commit**

  ```bash
  git add entropy_arb/volume_probe.py tests/test_volume_probe.py
  git commit -m "feat: add volume probe pure rules"
  ```

### Task 2: Parameterize the existing B0 execution plumbing safely

**Files:**
- Modify: `entropy_arb/arcus_execution.py`
- Modify: `entropy_arb/calibration_runtime.py`
- Modify: `entropy_arb/calibration.py`
- Modify: `tests/test_phase_b0.py`
- Modify: `tests/test_volume_probe.py`

**Interfaces:**
- Consumes: Task 1 `ProbeCandidate`/quantity rules.
- Produces: `ArcusMakerClient(client_prefix="b0-", fixed_quantity=Decimal("0.01"))`, generic controller prefix/quantity hooks, and a public probe placement/hedge-result boundary used by Task 3.

- [ ] **Step 1: Write failing regression tests**

  Add tests proving a volume maker accepts `vp-` and an arbitrary validated quantity, rejects non-`vp-` cancels/placements, exposes authoritative hedge result metrics for the probe wrapper, and still rejects B0 quantity/prefix changes. Add a test that real `on_fill` uses `FillAccumulator` and sends the opposite RH side.

- [ ] **Step 2: Run the focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: new volume-specific assertions fail while existing B0 tests remain green.

- [ ] **Step 3: Implement the minimal generic hooks**

  Add optional maker prefix/fixed-quantity settings with B0-compatible defaults; make controller order quantity/prefix configurable through `SessionLimits`/constructor; replace only hardcoded B0 checks that must be shared; expose one narrow placement method and last authoritative hedge result/latency record. Keep B0 error semantics and defaults unchanged.

- [ ] **Step 4: Run the focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: existing B0 suite and new plumbing tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add entropy_arb/arcus_execution.py entropy_arb/calibration_runtime.py entropy_arb/calibration.py tests/test_phase_b0.py tests/test_volume_probe.py
  git commit -m "feat: generalize b0 maker plumbing for volume probes"
  ```

### Task 3: Add the one-shot controller, CLI gates, and Engine runtime

**Files:**
- Modify: `main.py`
- Modify: `entropy_arb/engine.py`
- Modify: `entropy_arb/calibration_runtime.py`
- Create: `entropy_arb/volume_probe_runtime.py`
- Modify: `tests/test_volume_probe.py`
- Modify: `tests/test_phase_b0.py`

**Interfaces:**
- Consumes: Task 1 pure rules/writer and Task 2 generic B0 execution hooks.
- Produces: `VolumeProbeController` with `run(stop)`, callbacks compatible with `ArcusAccountFeed`, `pre_order_state(...)`, and final status/metrics; CLI arguments and `validate_runtime_gates(...)` support for the independent mode.

- [ ] **Step 1: Write failing controller/gate tests**

  Cover all mode combinations, required probe args/defaults, pre-order-only no-mutation behavior, BUILD partial fill followed by opposite RH hedge, BUILD→HEDGED→UNWIND, correct reverse unwind quantity, cancel/reconcile before reprice, timeout/partial/unresolved halt, completed flat final state, non-flat reconciliation-required final state, and record-only credential-free behavior.

- [ ] **Step 2: Run focused tests to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: new controller and gate tests fail because the mode is not wired.

- [ ] **Step 3: Implement the controller and runtime wiring**

  Implement one-shot BUILD/UNWIND orchestration around the inherited callback and reconciliation path. Start only with a fresh BBO and healthy account/public feeds; recognize/cancel only stale `vp-` orders; abort on unknown Arcus/RH orders or non-flat startup positions; pass `vp-` to the maker; never place two orders; require terminal cancellation before replacement; stop additions on every health/hedge/loss/runtime failure; re-read final positions/orders and classify `COMPLETED` only when fully flat. Add `--probe-clip-usd`, `--probe-side`, `--probe-reprice-sec`, `--probe-max-runtime-sec`, and `--probe-max-loss-usd`, with the exact gates. Append one round row in `finally`.

- [ ] **Step 4: Run focused tests to verify GREEN**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: all existing B0 and new volume-probe tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add main.py entropy_arb/engine.py entropy_arb/calibration_runtime.py tests/test_phase_b0.py tests/test_volume_probe.py
  git commit -m "feat: add gated one-shot volume probe runtime"
  ```

### Task 4: Documentation and complete verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-10-02-volume-probe-v1-design.md`
- Modify: `docs/superpowers/plans/2026-10-02-volume-probe-v1.md`

**Interfaces:**
- Consumes: Task 3 CLI behavior and statuses.
- Produces: shortest preflight and approved-round examples, explicit non-goals, and a checked implementation plan.

- [ ] **Step 1: Write failing documentation/CLI smoke assertions**

  Add local parser/gate assertions for the exact README commands and verify the plan's required status/field names are represented in code tests.

- [ ] **Step 2: Run the assertions to verify RED**

  Run: `python3 -m pytest -q tests/test_phase_b0.py tests/test_volume_probe.py`

  Expected: the new documentation-facing assertion fails until the README and final status wiring are complete.

- [ ] **Step 3: Update README and mark the plan complete**

  Document both `PREORDER_ONLY` and approved one-round commands, the `vp-`/maker-only safety boundary, the CSV path/statuses, and the five explicitly deferred features. Do not claim live execution or add a mainnet call.

- [ ] **Step 4: Run the full verification suite**

  Run: `python3 -m pytest -q && ruff check . && ruff format --check . && python3 -m mypy entropy_arb tests main.py && python3 -m compileall -q main.py entropy_arb tests && git diff --check`

  Expected: all tests pass, Ruff/mypy/compileall/diff checks exit 0. Any unavailable tool is reported explicitly, but pytest is always run.

- [ ] **Step 5: Commit**

  ```bash
  git add README.md docs/superpowers/specs/2026-10-02-volume-probe-v1-design.md docs/superpowers/plans/2026-10-02-volume-probe-v1.md
  git commit -m "docs: document volume probe v1"
  ```
