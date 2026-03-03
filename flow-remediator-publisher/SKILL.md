---
name: flow-remediator-publisher
description: Review a curated list of existing flow datasets, remediate each finding with structured LLM patches (plus schema/version gates), and publish append-only new flow versions via MCP CRUD insert when batch flow governance/remediation is needed and direct database access is not allowed.
---

# Flow Remediator Publisher

## Overview

Use this skill for batch flow remediation on existing `flows` records when the input scope is a curated UUID list (for example an operator-exported `flow_list_100_selected.jsonl`, where `100` means `state_code=100` rather than "top 100") and the agent must not access the database directly.

This skill provides a staged pipeline to fetch flow JSON via MCP CRUD, delegate flow review to `lifecycleinventory-review --profile flow`, remediate each finding via structured LLM output, run schema/version gates, and append a new `uuid + version` record via `insert`.

## Scope Boundary (Avoid Overlap)

- `process-automated-builder` owns `process_from_flow` generation and publishing of process/source datasets.
- `lifecycleinventory-review` owns review logic/reporting (including `flow` profile semantic review).
- `flow-remediator-publisher` owns remediation and append-only publish orchestration for existing flow datasets.

This skill's `review`/`validate` stages should delegate to `lifecycleinventory-review --profile flow` and consume its `findings.jsonl`, rather than duplicating review rules.

## Reuse Policy (Do Not Reimplement)

When classification/category or name changes require regenerating a product flow payload, reuse:

- `process-automated-builder/tiangong_lca_spec/product_flow_creation/service.py`

When fetching/publishing via MCP CRUD, reuse:

- `process-automated-builder/tiangong_lca_spec/publishing/crud.py`
- `process-automated-builder/tiangong_lca_spec/core/mcp_client.py`

Do not duplicate MCP CRUD client logic or product flow builder logic inside this skill unless blocked.

## Workflow

1. Operator exports UUID list (manual SQL outside the agent) to a local file such as `references/flow_list_100_selected.jsonl` (`100` indicates `state_code=100` filter, not "first 100 rows").
2. Run `fetch` or `pipeline` to retrieve full flow JSON via MCP CRUD (`select`) and cache locally.
3. Run `review` (delegates to `lifecycleinventory-review --profile flow`) to generate structured `findings.jsonl`.
4. Run `llm-remediate` to process findings one-by-one (`modified true/false`, optional no-change, optional regen-service).
5. Run `bump-version-if-needed` to enforce `patched_version == base_version + 1` (single-step increment only).
6. Run `validate-schema` to gate on ILCD FlowDataSet schema and mark publish eligibility.
7. Run `validate` to re-review schema-valid patched flows.
8. Run `publish --mode insert` to append new versions via MCP CRUD `insert` (only schema-valid + modified rows by default).
9. If concurrent publishers advanced the same UUID, publish stage performs safe version retarget/retry only when latest DB payload is semantically equal to local base (ignoring version); for unreadable private versions it can probe sequential `+1` versions via `--version-conflict-retries`; otherwise it records conflict.

## Commands

### One-shot pipeline (recommended for first run)

```bash
python3 scripts/run_flow_remediator_publisher.py pipeline \
  --uuid-list /abs/path/flow_list_100_selected.jsonl \
  --run-dir artifacts/flow-remediator/run-001 \
  --with-reference-context \
  --remediate-llm-model gpt-4o-mini \
  --publish-mode dry-run
```

### Staged execution (review delegated to `lifecycleinventory-review`)

```bash
python3 scripts/run_flow_remediator_publisher.py fetch --uuid-list /abs/path/flow_list_100_selected.jsonl --run-dir artifacts/flow-remediator/run-001
python3 scripts/run_flow_remediator_publisher.py review --run-dir artifacts/flow-remediator/run-001 --with-reference-context
python3 scripts/run_flow_remediator_publisher.py llm-remediate --run-dir artifacts/flow-remediator/run-001
python3 scripts/run_flow_remediator_publisher.py bump-version-if-needed --run-dir artifacts/flow-remediator/run-001
python3 scripts/run_flow_remediator_publisher.py validate-schema --run-dir artifacts/flow-remediator/run-001
python3 scripts/run_flow_remediator_publisher.py validate --run-dir artifacts/flow-remediator/run-001 --with-reference-context
python3 scripts/run_flow_remediator_publisher.py publish --run-dir artifacts/flow-remediator/run-001 --mode dry-run
```

说明：
- `flow_list_100_selected` 命名中的 `100` 表示 `state_code=100` 的 flow 列表，不表示“前 100 条”。
- 在你的环境（已配置 `OPENAI_API_KEY` / `OPENAI_MODEL`）下，`review` / `validate` 默认会走 `lifecycleinventory-review` 的 LLM 语义复审。
- 如需临时关闭，可加 `--review-disable-llm`（`review`、`validate`、`pipeline` 均支持）。
- remediation 阶段要求 LLM 返回严格 JSON：`modified/reason/patched_flow_json/changes/needs_regen_service`。

## Product Flow Regeneration Helper

Use `regen-product-flow` when a remediation needs classification/name/category updates and in-place patching is not reliable. This subcommand rebuilds the flow payload by calling `ProductFlowCreationService` from `process-automated-builder`.

```bash
python3 scripts/run_flow_remediator_publisher.py regen-product-flow \
  --flow-file /abs/path/original_flow.json \
  --out-file /abs/path/regenerated_flow.json \
  --overrides-file /abs/path/request_overrides.json
```

`request_overrides.json` is a JSON object with `ProductFlowCreateRequest` fields (for example `classification`, `class_id`, `base_name_en`, `base_name_zh`).

## Required Runtime Configuration

This skill reuses `process-automated-builder` MCP configuration. Set the same environment variables before using `fetch` or `publish` (review/validate `--with-reference-context` delegates to local registry context in `lifecycleinventory-review`, not CRUD):

- `TIANGONG_LCA_REMOTE_TRANSPORT`
- `TIANGONG_LCA_REMOTE_SERVICE_NAME`
- `TIANGONG_LCA_REMOTE_URL`
- `TIANGONG_LCA_REMOTE_API_KEY`

Remediation stage additionally needs an OpenAI-compatible LLM runtime (unless you intentionally allow `llm_unavailable` no-change behavior):

- `OPENAI_API_KEY` (or `LCA_OPENAI_API_KEY`)
- `OPENAI_MODEL` / `LCA_OPENAI_MODEL` (optional; default `gpt-4o-mini`)
- `OPENAI_BASE_URL` / `LCA_OPENAI_BASE_URL` (optional)

## Load References On Demand

- `references/workflow.md`: input/output layout, staged commands, and publish safety gates
- `references/schemas.md`: `findings`, remediation action contracts, and publish result schemas
