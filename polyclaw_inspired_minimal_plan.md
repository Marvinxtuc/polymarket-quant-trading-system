# Polyclaw Inspired Minimal Plan

## Goal

借鉴 `polyclaw` 的核心优点，但不引入发币、托管、多链入金这类重工程和高合规复杂度模块。

本项目更适合优先吸收下面三件事：

1. 把策略从“代码行为”升级成“可配置的策略身份”
2. 把信号从“可执行”升级成“可解释、可复盘”
3. 把运行面板从“状态展示”升级成“研究-决策-执行闭环”

---

## Current Fit

当前项目已经有很好的基础设施，适合做最小演进：

- 策略入口清晰：`WalletFollowerStrategy`
- 风控入口清晰：`RiskManager`
- 执行入口清晰：`Trader.step()`
- 运行态已经有 `sources`、`alerts`、`timeline`
- 前端已经能消费结构化状态

对应代码位置：

- [src/polymarket_bot/strategies/wallet_follower.py](~/Desktop/Polymarket/src/polymarket_bot/strategies/wallet_follower.py)
- [src/polymarket_bot/risk.py](~/Desktop/Polymarket/src/polymarket_bot/risk.py)
- [src/polymarket_bot/runner.py](~/Desktop/Polymarket/src/polymarket_bot/runner.py)
- [src/polymarket_bot/daemon.py](~/Desktop/Polymarket/src/polymarket_bot/daemon.py)
- [frontend/app.js](~/Desktop/Polymarket/frontend/app.js)

---

## What To Borrow

### 1. Strategy Identity

借鉴 `polyclaw` 的不是它的 `strategyType` 枚举本身，而是“策略要有可读身份”这个思路。

建议新增顶层策略元信息：

- `strategy_id`
- `strategy_type`
- `strategy_description`
- `risk_profile`

建议默认值：

- `strategy_id=wallet_follower_v1`
- `strategy_type=wallet_follower`
- `strategy_description=Follow qualified Polymarket wallets when they open or materially add to positions.`
- `risk_profile=medium`

这层配置应该主要服务：

- 日志解释
- 前端展示
- 报告输出
- 未来多策略共存

不需要一开始就支持动态热更新。

### 2. Explainable Signals

当前 `Signal` 只够执行，不够解释。

现状：

- 有 `wallet`
- 有 `confidence`
- 有 `observed_notional`
- 没有“为什么买”
- 没有“证据来自哪里”
- 没有“这个单子属于什么 thesis”

建议把 `Signal` 扩展为最小可解释结构。

建议新增字段：

```python
reason_code: str
reason_text: str
strategy_type: str
source_wallets: list[str]
source_summary: str
risk_flags: list[str]
catalysts: list[str]
tags: list[str]
```

对当前跟单策略，第一版完全可以不用 LLM，直接规则生成：

- `reason_code="wallet_new_position"`
- `reason_code="wallet_size_increase"`
- `reason_text="Wallet 0xabc... increased position by $420 in a qualified market."`
- `source_wallets=[wallet]`
- `source_summary="1 qualified wallet triggered the signal."`
- `risk_flags=["single_wallet_signal"]`
- `catalysts=[]`
- `tags=["wallet_follower", "onchain_behavior"]`

这样做的价值：

- 无需引入外部 AI 依赖
- 可以立刻提升可读性
- 为未来接新闻/研究模块预留字段

### 3. Decision Trace

`polyclaw` 的强项之一，是把“分析结果”变成结构化决策对象。

本项目不需要一开始做复杂 AI 分析，但非常值得补一个轻量 decision trace。

建议在 `Trader.step()` 中为每个 signal 记录：

- `signal_generated`
- `risk_evaluated`
- `budget_clamped`
- `duplicate_skipped`
- `order_filled`
- `order_rejected`

并且每一条事件都尽量带上：

- `strategy_type`
- `reason_code`
- `reason_text`
- `risk_decision`
- `notional_requested`
- `notional_allowed`

现有事件日志已经很接近，只是字段还不够统一。

---

## Minimal Schema Proposal

### Signal

建议从：

```python
@dataclass(slots=True)
class Signal:
    wallet: str
    market_slug: str
    token_id: str
    outcome: str
    side: Side
    confidence: float
    price_hint: float
    observed_size: float
    observed_notional: float
    timestamp: datetime
```

升级成：

```python
@dataclass(slots=True)
class Signal:
    wallet: str
    market_slug: str
    token_id: str
    outcome: str
    side: Side
    confidence: float
    price_hint: float
    observed_size: float
    observed_notional: float
    timestamp: datetime
    strategy_type: str = "wallet_follower"
    reason_code: str = ""
    reason_text: str = ""
    source_wallets: tuple[str, ...] = ()
    source_summary: str = ""
    risk_flags: tuple[str, ...] = ()
    catalysts: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
```

说明：

- 用 tuple 而不是 list，可以让 dataclass 默认值更安全
- 第一阶段不必把每个字段都展示到前端

### Runtime State

建议在 `daemon` 的 `positions`、`orders`、`timeline` 中透出解释字段。

`positions` 可新增：

- `strategy_type`
- `entry_reason`
- `entry_wallet`
- `source_summary`

`orders` 可新增：

- `token_id`
- `strategy_type`
- `reason_code`
- `reason_text`

`timeline` 可从目前的：

```json
{"time":"12:30","text":"signal BUY market-x"}
```

升级成：

```json
{
  "time": "12:30",
  "text": "BUY market-x",
  "detail": "wallet increase +$420 by 0xabc...",
  "kind": "signal"
}
```

### Strategy Metadata

建议在 `config` state 中新增：

- `strategy_id`
- `strategy_type`
- `strategy_description`
- `risk_profile`

这四个字段很适合直接展示在前端 header 或参数面板里。

---

## Concrete Changes By File

### [src/polymarket_bot/types.py](~/Desktop/Polymarket/src/polymarket_bot/types.py)

新增 `Signal` 的解释字段。

### [src/polymarket_bot/config.py](~/Desktop/Polymarket/src/polymarket_bot/config.py)

新增策略身份相关配置：

- `strategy_id`
- `strategy_type`
- `strategy_description`
- `risk_profile`

如果想继续做参数预设，再补：

- `risk_profile_override_enabled`

但第一阶段不必做自动映射。

### [src/polymarket_bot/strategies/wallet_follower.py](~/Desktop/Polymarket/src/polymarket_bot/strategies/wallet_follower.py)

在生成信号时补足：

- `reason_code`
- `reason_text`
- `source_wallets`
- `source_summary`
- `tags`

规则建议：

- 新建仓信号：`wallet_new_position`
- 加仓信号：`wallet_size_increase`
- 大额加仓时把 `risk_flags` 留空或弱化
- 小额/单钱包信号可加入 `single_wallet_signal`

### [src/polymarket_bot/runner.py](~/Desktop/Polymarket/src/polymarket_bot/runner.py)

重点改 3 处：

1. `signal_skip` / `order_filled` / `order_reject` 统一写入解释字段
2. `positions_book` 存入 entry metadata
3. `recent_orders` 存入 reason metadata

建议写入 `positions_book` 的扩展字段：

- `strategy_type`
- `entry_reason`
- `entry_wallet`
- `source_summary`
- `entry_tags`

### [src/polymarket_bot/daemon.py](~/Desktop/Polymarket/src/polymarket_bot/daemon.py)

把已有 UI 数据真正用起来：

- `positions.reason` 不再写死 `"wallet follower"`
- `sources` 增加 `status_reason`
- `timeline` 增加 `detail`
- `alerts` 增加策略级提示

建议新增 alert 规则：

- 单轮出现大量 `single_wallet_signal`
- 最近信号全部来自同一钱包
- 最近多次因为 `price outside allowed band` 被拒绝

### [frontend/app.js](~/Desktop/Polymarket/frontend/app.js)

第一阶段只做轻量展示：

- 持仓表显示 `entry_reason`
- 时间轴显示 `detail`
- 参数区显示 `strategy_type` 和 `risk_profile`

不要一开始就把前端做成研究终端，避免 UI 复杂度膨胀。

---

## Recommended Phases

### Phase 1: Explainability First

目标：不改变交易逻辑，只提升解释能力。

工作内容：

- 扩展 `Signal`
- 扩展 `positions_book`
- 扩展事件日志
- 扩展 daemon state
- 前端多展示 2 到 3 个字段

收益：

- 风险最低
- 对现有表现几乎零扰动
- 立刻提升复盘质量

### Phase 2: Strategy Identity

目标：让系统能表达“自己是什么策略”。

工作内容：

- 新增策略元配置
- 前端展示
- 日志/状态中透出
- README 更新为“策略可配置框架”

收益：

- 方便将来并行多个 alpha
- 方便接报告系统

### Phase 3: Research Inputs

目标：在不改变执行框架的前提下，接入更像 `polyclaw` 的外部研究输入。

可选输入：

- market metadata
- event deadline proximity
- outcome liquidity / spread
- news headline feed

这一步才考虑接搜索、LLM、外部研究，不建议提前做。

---

## What Not To Build Yet

下面这些不建议近期做：

1. 发币与 buyback flywheel
2. 多链托管式入金
3. 自动社交发帖
4. 全自动 AI 决策替代现有 alpha

原因很简单：

- 对当前 alpha 提升不直接
- 工程复杂度明显更高
- 容易把精力从“策略质量”转移到“产品外壳”

---

## Success Criteria

如果这个最小改造做得对，应该看到这些变化：

1. 任意一笔持仓都能回答“为什么开仓”
2. 任意一笔拒单都能回答“为什么没做”
3. 面板能看出“这轮信号主要由哪些钱包驱动”
4. 复盘时能按 `reason_code` 和 `source_wallet` 聚合表现
5. 后续新增第二策略时，不需要改动主框架

---

## Best Next Step

最推荐的实际开发顺序：

1. 扩展 `Signal`
2. 在 `wallet_follower` 中填充解释字段
3. 在 `runner` 中把解释字段写入 `recent_orders` 和 `positions_book`
4. 在 `daemon` 中把解释字段透出到 API state
5. 在前端追加轻量展示

这是当前项目借鉴 `polyclaw` 的最高性价比路径。
