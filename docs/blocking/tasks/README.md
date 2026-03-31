# 阻断项整改基建说明

目录结构
- `prompts/`：master / task / retry 提示模板。
- `scripts/gates/`：静态、测试、文档、阻断项总控 gate（fail-closed）。
- `reports/blocking/<BLOCK-ID>/`：每个阻断项的验证与自检输出目录。
- `docs/blocking/tasks/`：本说明及后续流程文档。

使用流程（示例）
1. 新建阻断项：复制 `prompts/task_template.md` 填写，BLOCK-ID 例如 `CTRL-000`.
2. 实施改动后，生成/更新：
   - `reports/blocking/<BLOCK-ID>/validation.txt`（验证命令与结果）
   - `reports/blocking/<BLOCK-ID>/self_check.md`（自检报告）
3. 运行 gate：
   ```bash
   bash scripts/gates/gate_block_item.sh <BLOCK-ID>
   ```
   - static/tests/docs 子 gate 缺失或失败会直接 fail（非零退出）。
4. 自检通过后，在汇报中引用上述报告路径。

约束
- gate 默认 fail-closed：未配置检查、不存在文档、缺少参数都会失败。
- 不得删除或弱化 gate 逻辑；如需扩展，需在对应脚本内添加真实检查。
- 报告目录必须随 BLOCK-ID 独立，避免交叉污染。
