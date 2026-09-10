# Repo 品質與 B0 Reconciliation Hardening 設計

## 目標

把 `arcus-arb` 從「pytest 通過但 lint、formatter、型別與 live 路徑仍未收斂」提升到可重複驗證的開發基線，同時針對既有 B0 `telemetry failure: Arcus reconciliation failed` 建立可重現的本地回歸測試與 fail-closed 修正。

## 範圍

1. 修正目前 `ruff check .` 報出的 production code 與測試問題。
2. 讓 `ruff format --check .` 通過，不改變既有執行語義。
3. 修正 `mypy entropy_arb tests` 的型別錯誤，將動態資料與第三方 SDK 的不確定性限制在明確的 boundary；不得以全域 `ignore_errors` 或大範圍錯誤忽略掩蓋問題。
4. 建立集中式 `pyproject.toml` 工具設定、development requirements 與 CI quality workflow。
5. 追查 B0 reconciliation／telemetry 的錯誤資料流，先以 failing test 鎖定根因，再以最小變更修復；保留 telemetry failure halt、取消已知 Arcus order 與不自動重試的安全語義。
6. 補充本地憑證檔案權限與安全操作文件；實際 `.env` 權限改為 owner-only，但不把憑證寫入 repo。

## 不在範圍內

- 不設定未知的 Git remote。
- 不修正使用者全域 Python environment 中與本 repo 無關的 `pip check` 衝突。
- 不進行 live API、帳戶查詢、下單、取消或任何需要憑證的 smoke test。
- 不調整 B0 固定數量、edge threshold、cancel threshold、loss cap、runtime 或其他交易參數。
- 不重設、刪除或覆寫既有 market-history SQLite 資料。

## 設計

### 1. Root-cause-first B0 修正

先閱讀 `CalibrationTelemetry`、`CalibrationController`、`ArcusAccountFeed`、REST fills backfill 與 shutdown/reconcile 邊界，並使用現有測試 fixture 或隔離的 temporary SQLite store 重現前次錯誤。新增的回歸測試必須在 production code 修改前先失敗，且要能區分：

- telemetry row 建立失敗；
- SQLite WAL flush／buffer drop 失敗；
- Arcus actual-fill fee reconciliation 的 REST 失敗；
- terminal order update 與 late fill／account stream watermark 的競態。

修正只處理被測試證明的根因，並驗證已知 order 仍會被取消、session 仍會 halt、不得重複 hedge 或繼續 quote。

### 2. 型別與工具設定

以 `pyproject.toml` 集中管理 pytest、Ruff 與 Mypy。對外部 JSON、WebSocket frame、SDK response 使用 typed parser／`Mapping[str, Any]` boundary；核心 state machine、storage row、venue interface 使用明確 dataclass、Protocol 與 `Optional` narrowing。第三方沒有完整 stub 時，只在 adapter import／轉換的窄邊界使用明確註解，不能將整個模組降級成 untyped。

Development requirements 會明確列出 pytest、ruff、mypy、coverage 與 PyYAML stubs，使本地與 CI 使用同一套工具版本下限。live SDK 依賴必須記錄已驗證的可重現版本或 commit；不能把「current main」當作可重現部署來源。

### 3. CI 與安全基線

新增 CI workflow 執行：

```text
python -m pytest -q
ruff check .
ruff format --check .
mypy entropy_arb tests
python -m compileall -q main.py entropy_arb tests
git diff --check
```

CI 不載入 `.env`，不執行 live CLI。README 補充 `.env` 的 `chmod 600` 要求與「不得在 log／config／commit 暴露 credential」的檢查方式。

## 驗收條件

- 工作樹中沒有未預期的修改，既有 324 個測試與新增回歸測試全部通過。
- `ruff check .`、`ruff format --check .`、`mypy entropy_arb tests`、compileall 與 `git diff --check` 均 exit 0。
- B0 telemetry/reconciliation regression test 證明錯誤會 fail-closed，且不會自動 retry、resubmit、replace 或 hedge twice。
- record-only 啟動與 public parser/storage 行為不變。
- 所有安全相關文件與 CI 都不需要真實憑證；本地 `.env` 權限為 `600`。
- 未執行任何 live API 或帳戶 mutation。
