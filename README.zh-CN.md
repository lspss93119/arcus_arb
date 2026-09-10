# arcus-arb

Phase A 是从 `entropy-arb` fork 出来的公开行情、仅记录系统：

```text
Arcus SNDK × Lighter-RH SNDK
```

Record-only 仍然是默认安全流程，不需要任何凭证。Phase B0 另外提供独立
的 tiny-live 预检，用于单边 Arcus LIMIT+ALO maker 校准，以及在收到权威
Arcus fill 后通过既有 Lighter-RH 路径对冲。B0 必须同时指定
`--tiny-live` 与 `--confirm-mainnet`，并且还需要独立的
`--approve-first-order` 才允许第一笔 Arcus 订单；本仓库不会自动提交第一笔
订单。

## 启动

```bash
cp config.example.yaml config.yaml
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --record-only
```

`--no-dashboard` 可切换到普通日志。默认写入独立的
`data/market-history.sqlite`，不会读写 `entropy-arb` 的数据库。

## 本地安全与质量检查

`.env` 与 `config.yaml` 已被 Git 忽略。真实凭证只能放在本地 `.env`，不得
写入 source、日志或 commit；执行 B0 预检前请先限制文件权限：

```bash
chmod 600 .env
stat -f "%Sp %OLp %N" .env
```

开发检查只执行本地测试与静态分析；CI 不安装实盘 SDK、不读取 `.env`，也不
调用任何实盘 flag：

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
ruff check .
ruff format --check .
python3 -m mypy entropy_arb tests main.py
python3 -m compileall -q main.py entropy_arb tests
```

可选的实盘 SDK 已在 `requirements-live.txt` 固定到审查过的 Lighter Python
SDK v1.1.2 commit。安装 SDK 不代表获准联网交易；运行时 gates 与新鲜的
preflight 仍然必须通过。

B0 预检才需要在本地、已被 Git 忽略的 `.env` 中配置现有 Arcus Ed25519 API
身份与现有 Lighter-RH 凭证，然后运行。Arcus 的 canonical 变量是
`ARCUS_ACCOUNT_ADDRESS`、`ARCUS_ACCOUNT_INDEX`、`ARCUS_API_KEY` 与
`ARCUS_PRIVATE_KEY`；`ARCUS_PRIVATE_KEY` 直接保存 Ed25519 key value，不是
文件名。`LIGHTER_ACCOUNT_INDEX` 是独立的 Lighter 变量。

```bash
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --tiny-live --confirm-mainnet --no-dashboard
```

程序会先打印实时账户、市场、BBO、费率、center、报价及安全限制，然后在
调用 `placeOrder` 前停止，除非人工审核后明确传入额外的第一笔订单 gate。不会
自动生成钱包或注册 API key。

## Arcus 公开 API

实现以官方文档 <https://docs.arcus.xyz/> 为准：

- 市场发现：`GET https://api.arcus.xyz/v1/markets`。
- 一个公开 multiplexed WebSocket：`wss://api.arcus.xyz/v1/ws`。
- 严格使用三个订阅：`SNDK-USD` 的 `l2OrderbookUpdates`、`trades`，以及
  全局 `marketAttributes`。
- 不订阅 `bbo`：L2 初始 snapshot 已提供 BBO，增量事件维护本地 orderbook，
  因此不增加第四个冗余的 public market-data subscription。

B0 的 account websocket 是另一条连接，另外使用四个 account-state
订阅：`userFills`、`orders`、`positions`、`accountAttributeUpdates`。这四个
不是重复的 public market-data 订阅，而是用于异步订单/成交/费率状态；Arcus
只有签名的 `placeOrder`/`cancelOrder` RPC 会改变账户，B0 没有 Arcus taker、
modify、cancel-all 或 private trading channel 操作。

CLI 的 `SNDK` 会从实时 metadata 的 `baseAsset` 或
`marketDisplayName` 解析；tick/数量精度不会硬编码。L2 使用每市场的
`lastSequenceId` 判断连续性；`globalSequenceId` 只作为跨市场 telemetry
保存，不作为 gap 锚点。发现 gap 时清空本地 book、标记 `RESYNC`、重新请求
L2 snapshot，直到新 snapshot 成功才恢复；断线则标记 `STALE`。

公开 trades 独立保存 exchange timestamp、本地 wall/monotonic receive
timestamp、价格、数量、trade ID、sequence number，以及 API 明确提供的
aggressor side。当前公开 schema 没有 aggressor side，因此不会自行推断。
market attributes 会保留 nullable 的 RTH 状态、settlement price、当前/下次
上下界、事件 timestamp 与 market sequence number。

## SQLite 与分析

Phase A.1 另外把每一个 wire-level 写入 append-only 的 arcus_l2_events。
snapshot/delta 会保存 event type、SQLite 接收顺序 id、消息内 event_index、
book_epoch、每市场 lastSequenceId、仅作 telemetry 的 globalSequenceId、
local wall/monotonic receive clocks、side、price 与 absolute size。delta 的
zero-size 行会原样保留作删除记录；同一价位的不同更新不会聚合。sequence
gap 的 delta 也会先原样保存，但本地 book 会失效，直到新 snapshot 开始
下一个 epoch。既有 bounded WAL writer 会批量写入，并在 shutdown flush；
arcus_l2_stats() 与 shutdown log 会显示已提交行数、按 receive timestamp
span 计算的 events/sec、SQLite bytes 与 WAL bytes。

arcus_l2_events 是按价位聚合的 L2，不是逐笔订单的 L3，因此单凭它不能
得到精确 maker queue position；未来 replay 仍需采用保守 queue model。

`arcus_samples` 每秒左右记录一笔两个 venue 都有效的 BBO，包含 bid/ask
数量、mid、premium、Arcus sequence ID、两种 timestamp 与 nullable attributes。
`arcus_trades`、`arcus_market_attributes`、`arcus_market_metadata` 是分开的
append-safe datasets，`arcus_minutes` 保留原有分钟摘要模式。

Premium 保持既有 midpoint 定义：

```text
(((arcus_bid + arcus_ask) / 2) / ((rh_bid + rh_ask) / 2) - 1) × 10,000
```

既有 `stable_basis` 与 rolling-center 会读取 Arcus/RH midpoint premium。
Phase A 中 center 仅供观察；本任务不调整 Arcus 门槛。

Dashboard 会显示 `ARCUS`、`RH`、BBO age、premium、center、recorder rows、
RTH state、sequence health（`OK`/`RESYNC`/`STALE`），并明确显示
`RECORD-ONLY · Arcus trading disabled`。B0 预检时会显示
`TINY-LIVE PRE-ORDER`、`Arcus ALO only` 与独立 approval gate，不提供通用
实盘交易控制项。

Record-only dashboard 也会在 Arcus sequence health 旁显示本次运行的 raw L2
event count，并且不会提供任何交易控制项。

## B0 安全边界

B0 是校准实验，不是生产策略：Arcus SNDK 固定每单 `0.01`，同时最多一个
订单、最多 20 个 Arcus fill event、`$500` filled notional、`$5` session
loss、运行 60 分钟。报价只能是 LIMIT ALO；模型 post-hedge edge 低于 4 bps
就等待，1.5 bps 为 hysteresis cancel threshold。只有权威收到 Arcus fill
后才会通过 RH 对冲；低于 RH minimum 的 residual 会明确保留，不会向上取整。
Outside-RTH 状态只作为 regime telemetry 记录，本身不会阻挡 B0 报价。断线、
行情 stale/resync、RH hedge 未决、telemetry 失败或任一 hard limit 都会停止
新报价并显式 reconcile/cancel 已知 Arcus 订单。rolling center 历史不足时使用
文档化的 `0.0` bps fallback，并记录 `center_source=fallback`；历史充分时记录
`center_source=rolling`。

Arcus 身份从 `ARCUS_ACCOUNT_ADDRESS`、`ARCUS_ACCOUNT_INDEX` 与
`ARCUS_API_KEY` 读取。canonical 的 direct-value private key 来源是
`ARCUS_PRIVATE_KEY`，然后按兼容性顺序使用 `ARCUS_ED25519_PRIVATE_KEY`、
`ARCUS_ED25519_PRIVATE_KEY_FILE`（最后一个仍然是文件名）。不会把凭证写进
config 或日志；credential diagnostics 只显示 `PRESENT`/`MISSING`。费率必须
由 `GET https://api.arcus.xyz/v1/feetiers` 与 account attribute stream 解析，
无法确定时 B0 会停止。RH 费率另外通过配置的 `LIGHTER_ACCOUNT_INDEX`，使用
官方 API key auth token 请求 authenticated `GET /api/v1/accountLimits?account_index=...`
解析；`current_maker_fee_tick` 与 `current_taker_fee_tick` 按官方
`FeeTick=1_000_000` 换算（100 ticks = 1 bps），包括已验证的 0 bps。public
`orderBooks.taker_fee` 与 YAML 兼容费率都不会作为 account-specific verification；
`accountLimits` 无法认证、解析或安全换算时 B0 会停止。

当前文档的 `userFills` 流可能不携带仅存储侧的 `createdAt` 与 `fee`。B0
仍会在可执行填单后立即使用已解析的 maker tier 费率作临时核算并执行 RH
对冲，随后从公开的 `GET /v1/fills` 补齐实际费用；不会因此重复对冲。实际
费用无法恢复时会停机并停止继续报价。

继承的 Entropy/Hyperliquid 与成熟 execution 模块只作为历史/可复用架构保留，
Arcus 路径不会把它们用于 Arcus 订单。Shadow maker simulation、策略优化、
execution research 与生产部署均不在本阶段。

当前 authentication 文档同时说明 compact sorted-JSON Ed25519 request scheme，
而 key registration 内容包含当前 EIP-712 流程及 legacy EIP-191 quickstart
说明。B0 不注册 key，只验证用户提供的 keypair，并在独立 approval gate 后
签名 LIMIT+ALO 请求。
