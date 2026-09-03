# arcus-arb

Phase A 是从 `entropy-arb` fork 出来的公开行情、仅记录系统：

```text
Arcus SNDK × Lighter-RH SNDK
```

本阶段 Arcus 没有任何交易能力。必须使用 `--record-only`；没有该参数时
会在市场发现前于本地直接退出。`ArcusVenue.supports_trading` 固定为
`False`，不需要 Arcus 凭证、钱包、私钥、签名器或 API 注册。

## 启动

```bash
cp config.example.yaml config.yaml
python3 main.py --config config.yaml --symbol SNDK \
  --hedge lighter-rh --record-only
```

`--no-dashboard` 可切换到普通日志。默认写入独立的
`data/market-history.sqlite`，不会读写 `entropy-arb` 的数据库。

## Arcus 公开 API

实现以官方文档 <https://docs.arcus.xyz/> 为准：

- 市场发现：`GET https://api.arcus.xyz/v1/markets`。
- 一个公开 multiplexed WebSocket：`wss://api.arcus.xyz/v1/ws`。
- 严格使用三个订阅：`SNDK-USD` 的 `l2OrderbookUpdates`、`trades`，以及
  全局 `marketAttributes`。
- 不订阅 `bbo`：L2 初始 snapshot 已提供 BBO，增量事件维护本地 orderbook，
  因此没有第四个冗余订阅。

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
`RECORD-ONLY · Arcus trading disabled`；没有 Arcus 交易控制项。

Record-only dashboard 也会在 Arcus sequence health 旁显示本次运行的 raw L2
event count，并且不会提供任何交易控制项。

## 范围边界

本阶段不实现 Arcus 下单、签名、API keys、提现、maker/taker execution、
cancel、private channels、positions、fills、shadow maker simulation、生产
门槛或 RH 对冲。继承的 Entropy/Hyperliquid 与执行模块只作为历史/可重用的
测试和分析代码保留；Arcus 配置与 Phase A engine 不会选取它们。
