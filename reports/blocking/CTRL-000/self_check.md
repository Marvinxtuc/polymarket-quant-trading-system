# CTRL-000 自检报告

## 目标
建立可复用的阻断整改基建，使 CTRL-000 自身 gate 可通过；未配置的 BLOCK 仍 fail-closed。

## 本轮 gate 结果
- static: PASS（必需文件存在，bash -n 通过）
- tests: PASS（tests/test_gate_smoke_ctrl000.py）
- docs: PASS（reports/blocking/CTRL-000 文档非空）
- block-item: PASS

## 演示 fail-closed
- BLOCK-001：docs 缺失/空，block-item FAIL（按设计）

## 命令
- bash scripts/gates/gate_block_item.sh CTRL-000
- bash scripts/gates/gate_block_item.sh BLOCK-001

## 结论
- CTRL-000 基建设施完成并通过自身 gate；未落地的阻断项保持 fail-closed。
