# arcus-arb

**[English documentation / 英文文档 → README.md](README.md)**

开源双交易所永续合约套利机器人。其中一条腿永远是 **Entropy**（Hyperliquid 上的
`io` builder dex）；另一条腿（对冲腿）三选一：

| `--hedge` | 交易所 | 计价货币 | 吃单费 | 协议 |
|---|---|---|---|---|
| `lighter` | Lighter 主网 | USDC | 0 bps | zkLighter ws（增量订单簿，异步结算） |
| `lighter-rh` | Lighter Robinhood 链 | **USDG** | 0 bps | zkLighter ws |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book，IOC 同步结算 |

> **推荐链接** —— 通过以下链接注册即可支持本项目：
> - Entropy — Tier 4 推荐，100% 返佣：<https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood 链：<https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz（Hyperliquid）：<https://app.hyperliquid.xyz/join/QUANTGUY>

当同一品种在一边贵、另一边便宜时，机器人同时在贵的一边卖出、便宜的一边买入
（均为吃单），持有 delta 中性仓位，等溢价回归后反向平仓。所有交易决策使用的
价格都来自**将要实际成交的那个交易所的真实订单簿**——Hyperliquid 的盘口来自
官方 websocket（`wss://api.hyperliquid.xyz/ws`），Lighter 的盘口来自 Lighter
官方 websocket。

机器人运行期间（包括无需密钥的 `--record-only`）会把约 1 Hz 的原始 BBO 样本
与分钟聚合数据写入 SQLite；受支持的 Lighter 参考数据也写入同一数据库。这些
记录用于离线市场分析和人工选择策略参数；采集器和实盘机器人都不会自动诊断市场、
选择策略或替你切换策略。

## 信号逻辑

实盘机器人使用 `config.yaml` 中由你明确选择的一套策略：

- `stable_basis` 使用人工选择的固定 `center_bps`，启动后立即 READY。
- `drifting_basis` 使用约 1 Hz、仅取有效且新鲜 BBO 的因果、按时间窗口滚动
  中位数作为中枢。它需要完整窗口 warm-up，并要求至少 90% 有效覆盖率；有效
  观测间隔超过 30 秒会清空历史并重新 warm-up；进程重启从空状态开始，不会
  预加载 CSV 或恢复持久化中枢。

实盘机器人不会自动进行市场诊断、策略选择或策略切换。请先审阅采集数据，
再在 `config.yaml` 中明确选择策略和参数。

两种策略都以中间价溢价作为中枢参考，但入场判断使用可实际成交的价格：

```
premium_bps =（Entropy 价格 / 对冲腿价格 − 1）× 10 000

                          ┌──────────────  卖出 Entropy + 买入对冲腿
center + upper   ───────────────────────────────────────────────────
                                       ▲
center           ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   人工选择的中枢
                                       ▼
center − lower   ───────────────────────────────────────────────────
                          └──────────────  买入 Entropy + 卖出对冲腿
```

- `center_bps`（用于 `stable_basis`）—— 人工选择的溢价常态水平。跨所溢价几乎
  从不以零为中心（预言机、计价货币或新上市溢价不同）。
- `upper_bps` / `lower_bps` —— 选定中枢上下两侧的入场带宽；`drifting_basis`
  在 warm-up 后由因果滚动中位数提供中枢。

两个方向的门槛都作用于**可实际成交的价格**（Entropy 买一 对 对冲腿卖一，
反之亦然），并且是**扣除双边吃单手续费之后的净门槛**——引擎会在阈值之上
另行叠加手续费。因此一次完整往返扣费后**净赚 ≥ upper + lower bps**，这是
结构上保证的。

有一点必须理解：当人工选择的中枢为 5 bps 时，买入 Entropy 的门槛是
`lower_bps − center_bps`，可能为**负数**。这是有意为之——如果 Entropy 长期贵
5 bps，那么在溢价为 0 时买入它，相对自身均衡水平就是便宜了 5 bps，可以平掉
此前在 `center + upper` 处的卖出。中枢选错可能亏损，因此应先用数据测量，再
以小仓位运行。

## 快速开始

```bash
git clone https://github.com/your-quantguy/entropy-arb.git && cd entropy-arb
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # 数据采集只需要这些

cp config.example.yaml config.yaml       # 明确策略、规模与风控配置
cp .env.example .env                     # 密钥——交易必填
```

交易哪个市场**不在**配置文件中——每次启动时用命令行参数显式指定：
`--symbol`（两个交易所共同交易的品种）和 `--hedge`（三选一：
`lighter`、`lighter-rh`、`tradexyz`；Entropy 永远是
另一条腿）。

本机器人**没有模拟盘**——要么采集数据（`--record-only`），要么实盘交易。
请用采集的数据和最小的仓位上限来验证策略，而不是模拟成交。

**第一步：先采集数据**（不需要任何密钥）：

```bash
python3 main.py --record-only --symbol SNDK --hedge lighter-rh
```

至少运行几个小时（最好一整天——溢价存在日内规律），数据写入与实盘模式共用的
SQLite 数据库；默认路径为 `data/market-history.sqlite`。

**第二步：审阅市场、选择策略参数：**

```bash
python3 tools/analyze.py --csv /path/to/legacy-minutes.csv
```

`tools/analyze.py` 是仅供旧 CSV 使用的临时分析工具：请只对既有 CSV export
使用它。它不会读取实时 SQLite 数据库，也不会自动选择或切换策略。对于当前市场
历史，请先建立 SQLite snapshot，再将单一文件交给人工或 ChatGPT 分析，并在
`config.yaml` 中明确选择 `stable_basis` 或 `drifting_basis` 及其参数。

**第三步：实盘** —— 填写 `.env`，安装签名 SDK，仓位上限从刚好满足
交易所最小名义的水平开始：

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge lighter-rh
```

不带 `--record-only` 运行时，只要两边行情就绪且溢价越过带宽，就会立即
发送真实订单。

**仪表盘。** 在终端运行时会显示实时 Rich 仪表盘：两边盘口（含数据龄/点差）、
持仓与上限、账户权益与本次会话盈亏、两个方向的可成交溢价对比完整门槛
（已含手续费与库存加价，● 表示已武装）、数据采集进度、最近成交，以及日志
尾部（完整日志写入 `logging.file`，默认 `logs/engine.log`）。`--record-only`
模式同样可用。加 `--cn` 参数可使仪表盘全部以中文显示。`--no-dashboard`
可切换为纯日志输出（nohup/systemd 等非终端环境会自动退回纯日志），也可
设置 `logging.dashboard: false`。

## 数据采集与分析

采集器在所有模式下自动运行（`recorder.enabled: true`）。它将约 1 Hz 的原始
BBO 样本与分钟聚合数据写入 `recorder.database`，默认路径为
`data/market-history.sqlite`；受支持的 Lighter 参考观察也写入同一数据库。多个
bot process 通过 SQLite WAL 共用该数据库，因此每个 process 都可记录自己的市场
组合而不需要各自的 CSV。`--record-only` 不需要交易密钥，也会写入同一数据库。

分钟聚合保留以下字段：

| 列 | 含义 |
|---|---|
| `minute_ts`, `time_utc` | 分钟起点（epoch 秒 / ISO UTC） |
| `entropy_bid/ask`, `hedge_bid/ask` | 该分钟最后一次有效盘口 |
| `premium_open/high/low/close/mean/std_bps` | Entropy 相对对冲腿的中间价溢价 |
| `sell_edge_mean/max_bps` | 卖出 Entropy 方向的可成交溢价（Entropy 买一 / 对冲腿卖一 − 1） |
| `buy_edge_mean/max_bps` | 买入 Entropy 方向的可成交溢价（对冲腿买一 / Entropy 卖一 − 1） |
| `samples` | 该分钟约 60 秒中两边盘口同时有效的秒数 |

采集的 edge 为费前口径。目前的数据库工作流如下：

1. 以实盘或 `--record-only` 写入 `recorder.database`。
2. 可选地使用 `tools/migrate_market_history.py` 非破坏式导入既有 CSV；原始 CSV
   会保留在原处。
3. bot 运行期间使用 `tools/snapshot_data.py` 建立一个独立的 SQLite snapshot。
4. 上传该 snapshot，供人工或 ChatGPT 分析。

`tools/analyze.py` 仅保留为旧 CSV 的临时分析工具。它可检查既有 export，但不是
SQLite reader，且不会诊断市场、选择策略或切换策略。溢价中枢会漂移，请审阅记录
的数据，并有意更新 `config.yaml` 中明确选择的策略。

## 配置说明

策略在 `config.yaml`（严格校验——未知键名直接报错），密钥在 `.env`。
交易市场由命令行指定（`--symbol`、`--hedge`）。完整的双语注释参考：
[config.example.yaml](config.example.yaml)。核心项：

| 键 | 含义 | 默认值 |
|---|---|---|
| `strategy.name` | 明确选择 `stable_basis` 或 `drifting_basis` | `stable_basis` |
| `strategy.params.center_bps` | `stable_basis` 的人工固定中枢 | 示例为 `0.0` |
| `strategy.params.upper_bps` / `lower_bps` | 入场带宽（> 0） | 示例为 `4.0` |
| `strategy.params.window_minutes` | `drifting_basis` 的按时间滚动窗口 | 备用示例为 `60` |
| `entropy.dex` | Entropy 在 Hyperliquid 上的 dex 名 | `io` |
| `*.taker_fee_bps` | 各所吃单费 | 0.0（tradexyz 对冲腿：1.0） |
| `*.max_position_usd` | 各所持仓上限 | 1000 |
| `*.max_orders_per_min` | 各所每分钟下单预算（滑动 60 秒） | 120；Lighter 对冲腿 30 |
| `sizing.take_fraction` | 吃掉可套利深度的比例 | 0.5 |
| `sizing.max_order_notional_usd` | 单笔名义上限 | 500 |
| `inventory.scale_bps` / `floor_frac` | 库存阶梯（仓位超过上限的 `floor_frac` 后额外加价） | 10 / 0.5 |
| `execution.premium_persist_sec` | 信号需持续多久才触发 | 0.3 |
| `execution.*` | 滑点保护、超时、对账周期等 | 见配置文件 |
| `recorder.enabled` / `recorder.database` | 市场历史存储 | true / `data/market-history.sqlite` |
| `logging.dashboard` / `logging.file` | 终端仪表盘；开启时日志写入文件 | 开启，`logs/engine.log` |

## 密钥配置（`.env`，仅实盘需要）

- **Entropy / tradexyz（Hyperliquid）** —— 在
  <https://app.hyperliquid.xyz/API> 创建 API（agent）钱包。`HL_PRIVATE_KEY`
  填 **agent 钱包私钥**，`HL_ACCOUNT_ADDRESS` 填主账户地址。当
  `--hedge tradexyz` 时两条腿默认共用该账户（内部自动共享 nonce 序列）；
  如需分开，设置 `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`。注意给
  所交易的各 dex 分别充入保证金。
- **Lighter** —— `LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、
  `LIGHTER_API_PRIVATE_KEY`，必须注册在与启动参数 `--hedge` **相同的部署**上
  （主网与 Robinhood 链是两套独立的账户和密钥——参见
  [lighter-python](https://github.com/elliottech/lighter-python)）。

## 执行机制

- 两条腿**同时发出吃单**：Lighter 用带均价保护的市价单，在鉴权 websocket
  上异步确认成交；Hyperliquid 用 IOC 限价单同步结算（结果未知时轮询
  orderStatus 兜底）。
- **持续性闸门**（`premium_persist_sec`）：信号先"武装"，持续存在才触发，
  过滤单 tick 的假信号。
- **库存阶梯**：仓位超过上限的 `floor_frac` 后，同方向加仓需要线性递增的
  额外溢价，满仓时最高加 `scale_bps`。
- **净敞口对冲**：两腿成交不对等时立即用 reduce-only 单（带滑点保护）
  削减敞口，并每 `reconcile_sec` 与链上仓位对账。
- **故障隔离**：被限频的交易所短暂暂停；交易所不可达（如例行维护）时暂停
  交易并每 `venue_probe_sec` 探测直至恢复；连续 `max_consecutive_errors`
  次执行异常则整体停机。
- **仅实盘**：没有模拟成交模式。`--record-only` 是唯一无风险的运行方式，
  其余都是真金白银。

## 目录结构

```
main.py                  入口（--record-only，默认即实盘）
entropy_arb/config.py    YAML + .env 配置契约与校验
entropy_arb/book.py      订单簿 + 含手续费的套利规模计算
entropy_arb/feeds.py     官方 HL ws + zkLighter ws 行情
entropy_arb/venue_hl.py  Hyperliquid dex 适配器（Entropy、tradexyz）
entropy_arb/venue_lighter.py  zkLighter 适配器（主网、Robinhood 链）
entropy_arb/engine.py    双交易所策略主循环
entropy_arb/dashboard.py Rich 终端仪表盘
entropy_arb/recorder.py  原始 BBO + 分钟聚合市场历史采集器
entropy_arb/storage.py   SQLite WAL 市场历史存储
entropy_arb/migration.py 非破坏式旧 CSV 导入
entropy_arb/snapshot.py  一致性 SQLite backup API snapshot
tools/migrate_market_history.py  将旧 CSV 导入 SQLite
tools/snapshot_data.py   建立单一独立 SQLite snapshot
tools/analyze.py         仅旧 CSV 的临时分析工具
tests/                   python3 -m pytest tests/
```

## 已知风险

- **中枢填错就是亏钱策略。** 溢价中枢会漂移，请定期重新测量，并保持
  `config.yaml` 中人工选择的策略参数与市场同步。
- **USDG 基差**（`lighter-rh`）：对冲腿以 USDG 计价，持续溢价中有
  一部分是稳定币本身的基差；选定的中枢吸收其水平，但 USDG 的*变动*是真实盈亏。
- **资金费**：两个交易所、两套独立的资金费率，持仓成本未建模——仓位上限
  请设小一些。
- **薄盘口**：Entropy 深度可能很小；`take_fraction` 与名义上限控制单笔规模，
  但部分成交后对冲腿的滑点是真实存在的。
- **交易时段**：股票类永续（如 SNDK）盘后各所预言机行为不同，建议加宽带宽
  或避开盘后。
- **单腿风险**：一条腿成交后另一条可能失败。机器人会自动对冲并对账，但
  仍需人工关注。

风险自负。本软件直接操作真实资金，本文档不构成任何投资建议。请从最小的
仓位上限开始。

## 开源协议

[MIT](LICENSE)
