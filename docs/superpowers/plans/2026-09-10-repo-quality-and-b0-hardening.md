# Repo Quality and B0 Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the repo pass its declared quality checks, remove the current type and formatting failures, and harden the B0 reconciliation failure path without performing live operations.

**Architecture:** Keep the existing Arcus record-only and gated B0 boundaries intact. Add typed boundaries around untrusted JSON and SDK objects, centralize developer-tool configuration, and make reconciliation errors diagnosable while preserving fail-closed cancellation and halt behavior.

**Tech Stack:** Python 3, asyncio, aiohttp, websockets, SQLite/WAL, pytest, Ruff, Mypy, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-10-repo-quality-and-b0-hardening-design-zh-TW.md`

## Global Constraints

- Do not call live APIs, load credentials into a command, submit/cancel orders, or run `--tiny-live`.
- Do not change B0 fixed quantity, edge thresholds, cancel threshold, loss cap, runtime, or gate semantics.
- Preserve record-only behavior and existing public parser/storage schemas.
- Use TDD for behavior changes: write a failing regression test, verify RED, implement the smallest fix, verify GREEN.
- Do not add blanket Mypy ignores, global `ignore_errors`, or formatter exclusions to hide failures.
- Do not reset, delete, or overwrite existing SQLite data.
- The current sandbox cannot create Git refs/worktrees; work in the existing clean checkout and record each verification checkpoint.

---

### Task 1: Establish quality configuration and fix mechanical Ruff failures

**Files:**
- Create: `pyproject.toml`
- Create: `requirements-dev.txt`
- Modify: `entropy_arb/book.py:51-55`
- Modify: `tests/test_recorder.py:13`
- Modify: `tests/test_reference.py:7`
- Test: existing full suite

**Interfaces:**
- Produces Ruff, pytest, and Mypy configuration consumed by local commands and CI.
- Changes only names/imports/formatting; no runtime behavior.

- [x] **Step 1: Capture the baseline**

Run `ruff check .` and `ruff format --check .`. Expected baseline: 4 lint errors and 41 files requiring formatting.

- [x] **Step 2: Add minimal tool configuration**

Create `pyproject.toml` with:

~~~toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]

[tool.ruff]
line-length = 88
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]
ignore = ["E501"]

[tool.mypy]
python_version = "3.11"
files = ["entropy_arb", "tests", "main.py"]
check_untyped_defs = true
disallow_untyped_defs = false
warn_unused_ignores = true
ignore_missing_imports = false
~~~

Create `requirements-dev.txt` with the base requirements and:

~~~text
-r requirements.txt
pytest>=8.0
ruff>=0.6
mypy>=1.11
coverage>=7.0
types-PyYAML>=6.0
~~~

- [x] **Step 3: Fix the reported lint issues**

Rename the Hyperliquid snapshot comprehension variable from `l` to `level`, remove the two unused imports, then run `ruff check . --fix` and `ruff format .`.

- [x] **Step 4: Verify Task 1**

Run:

~~~bash
ruff check .
ruff format --check .
python3 -m pytest -q
git diff --check
~~~

Expected: zero Ruff/format errors and all existing tests pass.

---

### Task 2: Harden source and test type boundaries until Mypy is clean

**Files:**
- Modify: `entropy_arb/storage.py`, `migration.py`, `recorder.py`, `reference.py`, `feeds.py`, `arcus_feed.py`
- Modify: `entropy_arb/venue_arcus.py`, `venue_hl.py`, `venue_lighter.py`, `engine.py`, `main.py`
- Modify: `tests/test_arcus.py`, `tests/test_phase_b0.py`
- Test: `mypy entropy_arb tests` and the full suite

**Interfaces:**
- Preserve public venue methods and dataclass fields.
- Use concrete annotations for Engine optional venues/recorders, typed protocols for venue operations, and `Mapping[str, Any]` only at JSON/SDK ingress.

- [x] **Step 1: Capture the exact Mypy baseline**

Run `mypy entropy_arb tests`; expected baseline is the observed 233 errors across 14 files. Do not suppress them.

- [x] **Step 2: Fix low-risk local annotations**

Annotate the storage buffer, narrow nullable recorder values after existing readiness checks, type migration row objects before timestamp access, and make `main.py` optional parameters and handler variables explicit. Run the affected tests and Mypy after each file group.

- [x] **Step 3: Fix parser and reference-feed boundaries**

Introduce typed local mappings for parsed JSON fields, validate numeric values before conversion, make the reference writer protocol expose the method actually called, and narrow the optional quota coordinator before use.

- [x] **Step 4: Fix venue adapter boundaries**

Keep third-party SDK imports at the adapter boundary. Add explicit credential/config guards and typed signer optionals. Use narrowly scoped import annotations only for packages without usable stubs, while retaining fail-closed credential behavior.

- [x] **Step 5: Type the Engine state machine**

Add class-level annotations for optional Arcus, reference, recorder, store, and venue collections. Use a shared venue protocol or precise union where methods differ. Narrow optionals after initialization gates instead of using unsafe non-null assertions.

- [x] **Step 6: Correct test-only typing**

Annotate mutable test doubles and replace invalid method assignments with typed stubs or boundary casts. Keep tests asserting real behavior.

- [x] **Step 7: Verify Task 2**

Run:

~~~bash
mypy entropy_arb tests
python3 -m pytest -q
~~~

Expected: Mypy exits 0 and all tests pass. Remaining errors must be fixed at their source, not hidden with a broad ignore.

---

### Task 3: Reproduce and fix the B0 reconciliation diagnostic/root-cause path

**Files:**
- Modify: `entropy_arb/calibration_runtime.py:1235-1290`
- Modify: `entropy_arb/arcus_execution.py:1008-1081` only if response parsing is proven to be the source
- Modify: `tests/test_phase_b0.py`

**Interfaces:**
- `CalibrationController.reconcile()` remains asynchronous and idempotent.
- Existing `CalibrationTelemetry` and `SessionRisk` fail-closed semantics remain unchanged.

- [x] **Step 1: Write the RED regression test**

Add a test with a fake `ArcusAccountRest` whose `open_orders()` or `fills()` raises an exception with an empty string. Assert that reconciliation halts with a diagnostic containing the exception class, operation, and safe response context, without calling order mutation or hedge methods. Run only the test and confirm it fails because the current halt reason is `Arcus reconciliation failed:` without actionable detail.

- [x] **Step 2: Trace the failing boundary**

Use the fake to identify whether the failure is transport, response shape parsing, or lifecycle ordering. Do not log credential-bearing URLs or perform live requests.

- [x] **Step 3: Implement the smallest root-cause fix**

Preserve the exception as the cause, include a stable operation label and exception type in the fail-closed halt reason, and correct parser/ordering behavior only if the RED test proves it is the source. Keep cancellation, terminal reconciliation, and idempotent fill handling intact.

- [x] **Step 4: Add safety assertions**

Cover successful reconciliation, empty response, malformed response, transport failure, cancel/fill race, no duplicate hedge, and no replacement quote after halt using temporary stores/fakes.

- [x] **Step 5: Verify Task 3**

~~~bash
python3 -m pytest -q tests/test_phase_b0.py
python3 -m pytest -q
~~~

Expected: the new test passes, all B0 safety tests remain green, and no live order path is exercised.

---

### Task 4: Add CI, dependency reproducibility, and credential-safety documentation

**Files:**
- Create: `.github/workflows/quality.yml`
- Modify: `requirements-live.txt`, `README.md`, `README.zh-CN.md`, `.env.example`
- Local-only: `.env` mode `600`

- [x] **Step 1: Add CI**

Create a Python 3.11/3.12 matrix that installs `requirements-dev.txt` and runs pytest, Ruff, format check, Mypy, compileall, and `git diff --check`. Do not install live requirements, load `.env`, or run live flags.

- [x] **Step 2: Make the live SDK reproducible**

Inspect the installed/known-good Lighter SDK revision and official repository refs. Update `requirements-live.txt` to an exact verified commit only when compatibility is confirmed; otherwise document the blocker instead of inventing a SHA.

- [x] **Step 3: Document credential safety**

Add bilingual instructions that `.env` is ignored, must be owner-readable only via `chmod 600 .env`, and must never appear in logs, config, or commits. Keep `.env.example` value-free.

- [x] **Step 4: Apply local permission hardening**

Run `chmod 600 .env` and verify with `stat -f "%Sp %OLp %N" .env`; do not stage `.env`.

- [x] **Step 5: Verify Task 4**

Check workflow/config discovery, run the full quality suite, run `git diff --check`, and inspect the diff for credential values.

---

### Task 5: Final verification and handoff

- [x] **Step 1: Run the complete matrix**

~~~bash
python3 -m pytest -q
ruff check .
ruff format --check .
mypy entropy_arb tests
python3 -m compileall -q main.py entropy_arb tests
git diff --check
~~~

- [x] **Step 2: Verify safety boundaries**

Confirm the diff contains no live CLI invocation, credential values, threshold changes, order mutation bypass, or database deletion/overwrite.

- [x] **Step 3: Verify repository state**

~~~bash
git status --short --branch
git diff --stat
git log -5 --oneline --decorate
~~~

Report exact test/tool results, changed files, commits, known blockers, and the fact that live API validation was intentionally not performed.
