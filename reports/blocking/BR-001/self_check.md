# BR-001 自检报告

## 目标

本轮只处理 `BR-001 - 同市场默认禁止重复进入，只允许受控同钱包加仓`。

不处理：

- BR-002（候选过期规则）
- EX-001 / 退出链
- sizing 公式调整
- risk breaker
- 控制面策略

## 本轮实现

- 统一重复进入与 add 的业务判定口径为 `token_id`。
- 统一“已有本地持仓”的判定真相来源为 runtime `positions_book`。
- 默认阻断同 `token_id` 的重复 BUY。
- 同钱包 add 改为显式白名单三条件同时满足才允许：
  - `signal.wallet == existing.entry_wallet`
  - `SAME_WALLET_ADD_ENABLED=true`
  - 钱包命中 `SAME_WALLET_ADD_ALLOWLIST`
- buy 侧 multi-wallet resonance 改为 observe-only，不再放大进入 notional / price / confidence / action 倾向。
- 在候选导出与 signal review 导出中补齐 `block_reason` / `block_layer`。
- 执行前再次检查重复进入规则，防止 approved/manual queue 绕过。

## 规则对照

1. 默认同一 `token_id` 只允许一次进入：已完成
2. 已有本地持仓时默认禁止再次进入：已完成
3. 同钱包 add 只有显式白名单才允许：已完成
4. 别的钱包在同一 `token_id` 上的新信号不得扩大现有仓位：已完成
5. 多钱包共振不再放大进入，只保留观察语义：已完成
6. 所有被拒绝的重复进入都带明确 reason code：已完成
7. `/api/candidates` / signal review 导出同时暴露 `block_reason` 和 `block_layer`：已完成
8. approved/manual 路径不能绕过新规则：已完成

## 反作弊自检

- 是否删除旧逻辑来规避测试：否
- 是否通过弱化断言来伪造通过：否
- 是否引入默认放行 / 静默成功分支：否
- 是否把硬阻断降级为 warning / 日志：否
- 是否保留默认重复进入旁路：否
- 是否让 buy resonance 继续影响进入规模：否

## 业务副作用说明

- 同钱包 add 现在默认关闭；旧行为必须显式配置白名单才能恢复。
- buy 侧多钱包 resonance 仍会保留观察说明，但不会再提升进入力度。
- 候选层的 blocked BUY 仍会被记录为候选/复盘对象，但状态明确为 blocked/watch，不会伪装成可执行候选。

## 结论

- BR-001 在当前结构内可直接完成，不需要前置重构。
- 代码、测试、验证脚本、文档、导出字段、自检工件均已补齐。
