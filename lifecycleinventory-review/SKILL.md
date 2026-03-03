---
name: lifecycleinventory-review
description: Unified lifecycle inventory (LCI) review skill with profile routing. Use `process` for process_from_flow outputs and `flow` for batch flow JSON review (LLM-driven semantic review with structured findings for remediation workflows); `lifecyclemodel` remains reserved.
---

# lifecycleinventory-review

统一入口的 LCI 复审 skill，采用 **main skill + profile** 架构。

## Profiles
- `process`（默认）：当前可用，执行 process_from_flow 产物复审。
- `flow`：当前可用（初版，LLM 驱动），执行 batch flow JSON 复审并输出结构化 findings。
- `lifecyclemodel`：预留（not implemented yet）。

## 统一入口
使用 `scripts/run_review.py`，通过 `--profile` 选择子能力。

### 默认 profile
若未显式传入 `--profile`，默认使用 `process`。

## process profile
使用 `profiles/process/scripts/run_process_review.py` 执行 process 维度复审：
- 输入：`--run-root --run-id --out-dir [--start-ts] [--end-ts]`
- 输出：
  - `one_flow_rerun_timing.md`
  - `one_flow_rerun_review_v2_1_zh.md`
  - `one_flow_rerun_review_v2_1_en.md`
  - `flow_unit_issue_log.md`

## flow profile
适用于已有 flow JSON 批次（例如 remediation 工作流的 `cache/flows`）的复审，采用：
- 本地结构化证据抽取（name/classification/flow property/quantitative reference）
- 可选本地 reference-context 增强（通过 `process-automated-builder` 的 flow property registry 提供 flowproperty + unitgroup 证据；参数为 `--with-reference-context`）
- LLM 语义复审（输出结构化 `findings.jsonl`）

输入（至少满足一种）：
- `--flows-dir --out-dir`
- `--run-root --out-dir`（默认尝试 `<run-root>/cache/flows`）

常用可选参数：
- `--enable-llm`
- `--disable-llm`
- `--llm-model`
- `--llm-max-flows`
- `--llm-batch-size`
- `--with-reference-context`（内部使用本地 registry）
- `--similarity-threshold`

默认行为（flow profile）：
- 若环境中存在 `OPENAI_API_KEY`，默认启用 LLM 语义复审。
- 使用 `--disable-llm` 可强制关闭，退回 rule-based findings only。

输出（flow profile）：
- `findings.jsonl`
- `rule_findings.jsonl`
- `llm_findings.jsonl`
- `flow_summaries.jsonl`
- `similarity_pairs.jsonl`
- `flow_review_summary.json`
- `flow_review_zh.md`
- `flow_review_en.md`

## 运行示例
```bash
python scripts/run_review.py \
  --profile process \
  --run-root /path/to/artifacts/process_from_flow/<run_id> \
  --run-id <run_id> \
  --out-dir /home/huimin/.openclaw/workspace/review \
  --start-ts 2026-02-22T16:01:51+00:00 \
  --end-ts 2026-02-22T16:21:40+00:00
```

```bash
python scripts/run_review.py \
  --profile flow \
  --run-root /path/to/artifacts/flow-remediator/run-001 \
  --out-dir /home/huimin/.openclaw/workspace/flow-review \
  --with-reference-context \
  --enable-llm \
  --llm-model gpt-5
```

## 后续扩展
- `profiles/flow`：继续沉淀 flow 维度复审规则，与 remediation skill 的 findings schema 对齐。
- `profiles/lifecyclemodel`：沉淀 lifecycle model 维度复审规则与脚本。
当前 `lifecyclemodel` profile 调用会返回 “not implemented yet” 并提示下一步。
