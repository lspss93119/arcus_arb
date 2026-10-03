# Volume-probe Arcus startup stability barrier

## Scope

Fix only the confirmed startup race where the volume probe can submit its
first Arcus quote while the public Arcus L2 book is still in `BOUNDARY`.
Preserve all order placement, sizing, retry, hedge, reconciliation, builder,
and L2 state-machine semantics.

## Implementation tasks

### Task 1: Add failing startup-barrier and health-detail tests

**Files:**
- Modify: `tests/test_engine.py`
- Modify: `tests/test_volume_probe.py`

- [x] Add focused Engine/runtime tests for the startup predicate, boundary/BBO
   recovery, bounded timeout, post-barrier BBO reread, and detailed runtime
   health diagnostics. Run the focused tests first to capture the expected
   red state.

### Task 2: Implement the read-only startup barrier and fresh BBO reread

**Files:**
- Modify: `entropy_arb/engine.py`

- [x] Add a read-only, 10-second Engine barrier immediately before
   `controller.begin_build(quantity)`. Require Arcus ready/healthy/first-delta
   complete/BBO/freshness, RH book ready/freshness, and healthy account
   channels; poll at most every 100ms and fail with current health details on
   timeout.
- [x] Reread both venue BBOs after the barrier, require them present and fresh,
   and use those values for the pre-order candidate/log without recomputing
   sizing.

### Task 3: Add detailed runtime health diagnostics without changing the gate

**Files:**
- Modify: `entropy_arb/volume_probe_runtime.py`

- [x] Centralize a small health-detail formatter and include its fields in the
   existing runtime market-health halt reason without changing the gate.

### Task 4: Full validation and delivery

**Files:**
- Modify: only files from Tasks 1–3 and this plan.

- [x] Run the full required validation, review the diff for scope, commit, and
   push `feature/volume-builder-v2`.

## Verification

Run the focused startup/barrier tests, the existing Arcus boundary and
post-only tests, then:

```text
python -m pytest -q
ruff check .
ruff format --check .
python -m mypy entropy_arb tests main.py tools
python -m compileall -q main.py entropy_arb tests tools
git diff --check
```

No live exchange tests or mutations.
