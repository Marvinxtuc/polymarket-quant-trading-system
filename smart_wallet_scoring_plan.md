# Smart Wallet Scoring Plan

## Core Positioning

这套项目的主 alpha 不是通用 AI 选市场，也不是主观宏观判断。

主 alpha 应该明确为：

- 发现 Polymarket 上的聪明钱包
- 评估这些钱包的历史质量
- 在它们出现高价值动作时跟随
- 用自己的风控做二次约束

换句话说：

- `smart wallet quality` 是上游 alpha
- `wallet action strength` 是入场触发
- `risk/execution` 是下游控制
- `AI/news` 最多只能做辅助解释或过滤器

---

## Current Gap

当前仓库已经做了“钱包发现 + 当前持仓筛选 + 跟单执行”，但还没有真正做“历史质量评分”。

现状更偏这些代理指标：

- 当前活跃持仓数
- 当前市场分散度
- 当前总 notional
- 当前单市场集中度
- 最近是否有交易活动

这些指标有用，但它们更像“看起来像专业玩家”，还不等于“历史上真的有 alpha”。

如果我们要严格对齐“参考聪明钱包的行为以及胜率”，那下一步重点就应该是：

1. 给钱包做历史质量评分
2. 用历史质量筛选钱包池
3. 再用当前动作决定是否下单

---

## Correct Mental Model

建议把钱包评估拆成两个层面，而不是混在一起。

### 1. Wallet Quality Score

这是一个相对慢变化的分数。

它回答的问题是：

- 这个钱包过去到底靠不靠谱
- 它是不是长期有优势
- 它的样本是不是足够大
- 它是不是值得进入跟踪池

### 2. Action Strength Score

这是一个相对快变化的分数。

它回答的问题是：

- 这个钱包这一次动作强不强
- 是新开仓还是继续加仓
- 加仓幅度大不大
- 是不是多个高分钱包同时指向同一 token

最终交易时，不应该只看其中一个。

更合理的是：

`final_signal_score = wallet_quality_score x action_strength_score x risk_filters`

---

## Proposed Pipeline

建议把整个流程改成四层。

### Layer 1. Universe Discovery

目标：

- 从 seed wallets 和公开活动里持续发现候选钱包

输入：

- `WATCH_WALLETS`
- `WALLET_DISCOVERY_PATHS`
- 最近活跃交易行为

产出：

- `candidate_wallets`

### Layer 2. Wallet Scoring

目标：

- 对候选钱包计算历史质量分

输入：

- 历史交易
- 历史已结算结果
- 当前持仓结构
- 最近活跃度

产出：

- `wallet_score`
- `wallet_tier`
- `wallet_metrics`

### Layer 3. Real-Time Signal Generation

目标：

- 只对高质量钱包的当前动作生成信号

输入：

- 高分钱包列表
- 当前周期的仓位变化

产出：

- `signals`

### Layer 4. Risk and Execution

目标：

- 按自己的预算和风控实际下单

输入：

- `signals`
- `wallet_score`
- `action_strength`
- 账户风险状态

产出：

- 订单或拒单原因

---

## Score Design

## Qualification First

先做硬门槛，再做分数。

没有通过基础门槛的钱包，不参与评分或直接降权。

建议的基础门槛：

- `min_resolved_markets`
- `min_trades_30d`
- `min_active_positions`
- `min_unique_markets`
- `max_top_market_share`
- `max_inactive_days`

推荐第一版门槛：

- 已结算市场数 `>= 15`
- 近 30 天交易事件数 `>= 10`
- 当前活跃持仓数 `>= 2`
- 当前唯一市场数 `>= 3`
- 单市场集中度 `<= 0.70`
- 最近一次交易距离今天 `<= 14` 天

说明：

- 这些值先偏保守
- 后续根据样本量调
- 没有历史结算数据时，不要把钱包标成“聪明”，最多标成“观察中”

## Composite Score

建议把钱包总分做成 100 分制。

### A. Historical Edge: 45 分

这是最核心部分。

建议组成：

- `resolved_win_rate_score`: 20 分
- `resolved_roi_score`: 15 分
- `profit_factor_score`: 10 分

解释：

- `resolved_win_rate_score`
  看已结算市场里，钱包最终站对方向的比例
- `resolved_roi_score`
  看资金效率，不只是对错次数
- `profit_factor_score`
  看盈利总额 / 亏损总额，避免高胜率但盈亏比差

### B. Reliability: 25 分

这部分解决“样本太小也看起来很强”的问题。

建议组成：

- `sample_size_score`: 10 分
- `recency_score`: 10 分
- `consistency_score`: 5 分

解释：

- `sample_size_score`
  样本越多，越可信
- `recency_score`
  太久没交易，历史成绩参考价值下降
- `consistency_score`
  避免高度依赖一次 lucky run

### C. Portfolio Quality: 15 分

这部分看钱包当前是否具备可持续跟踪价值。

建议组成：

- `diversification_score`: 8 分
- `concentration_penalty`: 7 分

解释：

- 分散不是越高越好，但极端单一市场玩家要打折
- 若一个钱包长期只赌一个主题，即使曾经赢过，也不适合做通用信号源

### D. Copyability: 15 分

这是非常关键但容易被忽略的一层。

一个钱包可能很强，但未必适合我们跟。

建议组成：

- `trade_size_fit_score`: 5 分
- `price_band_fit_score`: 5 分
- `execution_friendliness_score`: 5 分

解释：

- `trade_size_fit_score`
  如果对方常下极小单或极大单，都不一定适合我们的资金体量
- `price_band_fit_score`
  经常在极端价格下注的钱包，未必适合复制
- `execution_friendliness_score`
  需要尽量优先流动性尚可、滑点可控、可跟随的行为

---

## Suggested Formulas

第一版不需要复杂机器学习，规则分数足够。

### Win Rate Score

```text
wr = winning_resolved_markets / max(1, resolved_markets)
resolved_win_rate_score = clamp((wr - 0.45) / 0.25, 0, 1) * 20
```

直觉：

- 45% 以下基本不应被视为聪明钱包
- 70% 左右已属很强

### ROI Score

```text
roi = total_realized_pnl / max(1, total_resolved_notional)
resolved_roi_score = clamp((roi - 0.02) / 0.10, 0, 1) * 15
```

直觉：

- 长期结算 ROI 低于 2%，说服力不够
- 12% 左右已经很强

### Profit Factor Score

```text
profit_factor = gross_profit / max(1, abs(gross_loss))
profit_factor_score = clamp((profit_factor - 1.0) / 1.5, 0, 1) * 10
```

### Sample Size Score

```text
sample_size_score = min(1.0, log1p(resolved_markets) / log1p(50)) * 10
```

### Recency Score

```text
recency_score = exp(-days_since_last_trade / 30) * 10
```

### Concentration Score

```text
concentration_penalty = clamp((0.85 - top_market_share) / 0.35, 0, 1) * 7
```

### Copyability Score

建议第一版简单做成比例指标：

- 最近 N 笔交易中，有多少比例落在我们的可执行价格带内
- 最近 N 笔交易中，有多少比例的单笔规模与我们的预算量级相匹配

---

## Action Strength Score

钱包总分决定“值不值得跟”，动作分决定“这次该不该跟”。

建议动作分只由实时行为构成，不和历史质量混在一起。

### Action Inputs

- `new_position`
- `position_increase_delta_usd`
- `delta_pct_vs_previous_position`
- `wallet_score`
- `multi_wallet_confirmation`
- `signal_age_seconds`

### Action Rules

建议第一版：

- 新开仓强于小幅加仓
- 大额加仓强于小额加仓
- 多个高分钱包同时加仓同一 token 时显著加分
- 信号随时间快速衰减

### Example

```text
base_action =
  1.00 if new_position
  0.75 if delta_usd >= threshold and existing_position
  0.00 otherwise

size_boost = clamp(delta_usd / min_wallet_increase_usd, 1.0, 2.0)
multi_wallet_boost = 1.25 if 2+ tier-A wallets agree else 1.0
freshness_decay = exp(-signal_age_seconds / 1800)

action_strength = base_action x size_boost x multi_wallet_boost x freshness_decay
```

---

## Final Decision Logic

建议不要直接把钱包分数变成“是否跟单”的唯一标准。

更好的逻辑是分层：

### Step 1. Wallet Admission

- `wallet_score < 50` 不进入监控池
- `50 <= wallet_score < 65` 观察池
- `65 <= wallet_score < 80` 可交易池
- `>= 80` 核心跟踪池

### Step 2. Signal Admission

只有满足以下条件才进入风险模块：

- 钱包属于可交易池或核心池
- 当前动作强度达标
- 信号足够新
- 不违反去重和冷却规则

### Step 3. Position Sizing

建议让 `wallet_score` 影响下单上限，而不是只影响是否下单。

例如：

- `score >= 80` 可使用 `1.00x` 风险额度
- `65-79` 使用 `0.75x`
- `50-64` 使用 `0.40x`

这样能保留更多样本，同时让高质量钱包吃到更多权重。

---

## Data Requirements

这是最重要的现实约束。

当前仓库的 `PolymarketDataClient` 只支持：

- 当前活跃持仓
- 钱包活动发现

对应文件：

- [src/polymarket_bot/clients/data_api.py](~/Desktop/Polymarket/src/polymarket_bot/clients/data_api.py)

这意味着：

- 现在我们能做“当前持仓画像”
- 但还不能严格计算“历史胜率和已结算收益”

所以要做真正的钱包评分，必须补历史数据层。

最少需要这几类数据：

- 钱包历史 trades / fills
- 钱包已结算市场结果
- 钱包历史 realized pnl 或可回放的成交记录
- 可选的市场流动性指标

如果外部 API 不直接给 `realized pnl`，也可以通过“成交记录 + 结算结果”回放得到近似值。

---

## V1, V1.5, V2

### V1: Quality Proxy Score

目标：

- 在缺历史结算数据时，先做一个代理评分

只用现有或容易拿到的数据：

- 当前活跃持仓数
- 唯一市场数
- 总 notional
- 顶部市场集中度
- 近 7 天活动频率
- 最近信号命中后的短期 markout

这版不能叫“胜率评分”，更适合叫：

- `wallet_quality_proxy_score`

### V1.5: Resolved Performance Score

目标：

- 补历史结算数据，真正引入胜率与 ROI

新增：

- `resolved_win_rate`
- `resolved_roi`
- `profit_factor`
- `days_since_last_trade`

这版开始能比较接近“聪明钱包质量分”。

### V2: Copyability + Timing

目标：

- 进一步解决“他赚钱，但我们跟不上”的问题

新增：

- 24h / 72h markout
- 是否常在极端价格下注
- 交易时点是否太晚
- 同市场拥挤度和流动性

---

## Recommended Code Changes

## 1. Extend Data Client

建议在 [src/polymarket_bot/clients/data_api.py](~/Desktop/Polymarket/src/polymarket_bot/clients/data_api.py) 增加：

- `get_wallet_trade_history(...)`
- `get_wallet_resolved_history(...)`
- `get_wallet_activity_summary(...)`

这一步是钱包历史评分的前置条件。

## 2. Add Wallet Scoring Module

建议新增：

- `src/polymarket_bot/wallet_scoring.py`

职责：

- 定义 `WalletMetrics`
- 定义 `WalletScore`
- 根据原始历史数据计算分数和 tier

## 3. Add Score Cache

建议新增：

- `/tmp/poly_runtime_data/wallet_scores.json`

原因：

- 钱包评分不需要每轮都算
- 更适合按小时或半天更新

## 4. Change Wallet Resolution Flow

当前 [src/polymarket_bot/runner.py](~/Desktop/Polymarket/src/polymarket_bot/runner.py) 的 `_resolve_wallets()` 主要解决“找谁”。

后续应该改成：

1. 发现候选钱包
2. 读取钱包评分
3. 过滤低分钱包
4. 只把合格钱包送进 `generate_signals()`

## 5. Update UI

当前前端已经有钱包表和来源表。

建议新增展示字段：

- `wallet_score`
- `wallet_tier`
- `resolved_win_rate`
- `resolved_roi`
- `last_active`

这样前端会真正从“钱包列表”升级成“聪明钱包面板”。

---

## First Implementation Priority

如果按性价比排序，建议这样做：

1. 先定义 `WalletScore` 和 `WalletMetrics` 数据结构
2. 再做历史数据抓取接口
3. 先落 `V1 proxy score`
4. 再落 `V1.5 resolved performance score`
5. 最后让分数进入 `generate_signals()` 和仓位 sizing

原因：

- 先把结构搭起来，后面补数据不会推翻设计
- 先做代理分，系统就能开始区分钱包质量
- 真正的胜率和 ROI 在数据拿稳后再接，风险更低

---

## Clear Recommendation

结合你刚才对项目的重新定义，我建议我们后续所有设计都遵守一句话：

`先判断这个钱包过去是不是聪明，再判断它这次动作值不值得跟。`

这比“看到谁动了就跟”要稳得多，也比“让 AI 自己猜市场”更符合这套项目的核心定位。
