# 阻断项重试模板（Retry Template）

场景：上一轮 gate 失败后的快速纠偏。

填写要点
- BLOCK-ID：
- 本次失败点（按 gate 分类）：static / tests / docs / block-item
- 根因：
- 修复计划：
- 新增或调整的验证：

执行顺序
1. 复现：重跑失败 gate，记录日志。
2. 修复：只改涉及的最小范围。
3. 复测：全部 gate 重跑。
4. 报告：更新 `reports/blocking/<BLOCK-ID>/self_check.md`。
