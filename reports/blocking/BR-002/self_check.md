# BR-002 自检报告

## 目标

本轮只处理 `BR-002 - 候选生命周期收缩`。

不处理：

- BR-001（重复进入 / add 规则）
- EX-001 / 退出链
- sizing 公式调整
- risk breaker
- 控制面策略

## 本轮实现

- 候选生命周期统一只认 `created_ts + CANDIDATE_TTL_SECONDS`。
- 市场窗口更短时，候选过期时间会被 market end 提前截断。
- 候选过期后统一进入 `expired_discarded` 生命周期，不再继续影响 decision / queue / execution。
- `approved_queue` 在 decision 层会先检查候选是否过期。
- `execution-precheck` 在所有执行路径之前再次硬检查候选是否过期。
- `/api/state` 的 candidate observability 现在导出 lifecycle summary。
- `/metrics` 现在导出候选过期总量和 `candidate_lifetime_expired` 的按层计数。

## 规则对照

1. 候选失效时间基于统一时间戳和统一生命周期配置：已完成
2. `block_reason` 与 `block_layer` 在 candidate / decision / execution_precheck 对齐：已完成
3. `/api/state` 与 `/metrics` 同步暴露候选过期失效原因：已完成
4. 候选状态层次模型统一并硬化：已完成
5. `execution-precheck` 对过期候选做硬阻断：已完成
6. 旧手工 / 队列 / approved 路径不能绕过候选过期规则：已完成

## 反作弊自检

- 是否删除旧逻辑来规避测试：否
- 是否通过弱化断言来伪造通过：否
- 是否引入默认放行 / 静默成功分支：否
- 是否把硬阻断降级为 warning / 日志：否
- 是否允许候选失效后继续悄悄执行：否

## 业务副作用说明

- 候选 `created_ts` 现在代表“本地候选生成时间”，不再沿用原始信号时间。
- `pending` 之外的 active 候选不再做可交易性重校验，但仍会做生命周期失效检查。
- 已过期的 approved / queued 候选会在 decision / execution-precheck 被显式丢弃，而不是留到更后面才静默失败。

## 结论

- BR-002 在当前结构内可直接完成，不需要前置重构。
- 代码、测试、验证脚本、文档、状态导出、自检工件均已补齐。
