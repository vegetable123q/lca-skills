---
name: lci-data-auto-review
description: Review one process_from_flow run (exports/processes) with review logic v2: material-balance scope filtered to raw-material input vs product/by-product/waste outputs, energy excluded; produce bilingual review and unit-suspect log.
---

# lci-data-auto-review

> 目录名暂定，可后续按团队规范改名。

## 触发条件
- 完成了一次 `process_from_flow` run，且需要复审 `exports/processes` 数据质量。
- 需要统一产出以下文件：
  - `one_flow_rerun_timing.md`
  - `one_flow_rerun_review_v2_zh.md`
  - `one_flow_rerun_review_v2_en.md`
  - `flow_unit_issue_log.md`

## 输入
- `run_root`: `artifacts/process_from_flow/<run_id>`
- `run_id`
- `out_dir`
- 可选：`start_ts` / `end_ts`（ISO 格式）

## 输出
- 上述 4 个 markdown 文件。
- 报告中显式区分：证据充足 vs 证据不足结论。

## 步骤
1. 读取 `exports/processes/*.json`。
2. 基于 exchange comment 标签/描述进行分类：
   - raw_material_input
   - product_output / byproduct_output / waste_output
   - energy_input（仅记录，不计平衡）
3. 按 v2 口径计算平衡：
   - `raw material input = product + by-product + waste`
4. 检查单位疑似错误：仅记录有直接证据的“语义-单位矛盾”。
5. 生成中英复审与 timing、单位问题日志。

## 运行示例
```bash
python scripts/run_lci_review.py \
  --run-root /path/to/artifacts/process_from_flow/<run_id> \
  --run-id <run_id> \
  --out-dir /home/huimin/.openclaw/workspace/review \
  --start-ts 2026-02-22T16:01:51+00:00 \
  --end-ts 2026-02-22T16:21:40+00:00
```
