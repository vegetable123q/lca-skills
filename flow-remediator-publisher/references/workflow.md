# Workflow

## Inputs

- Curated UUID list file (JSON/JSONL/TXT), typically manually exported via SQL by an operator (for example `flow_list_100_selected.jsonl`, where `100` means `state_code=100`, not "top 100").
- MCP CRUD access (read-only for `fetch`, append-only `insert` for publish). Review context enrichment is delegated to `lifecycleinventory-review` and uses local `process-automated-builder` registry when enabled.
- LLM runtime for remediation stage (`OPENAI_API_KEY` + model/base URL as needed).

## Run Directory Layout

Example: `artifacts/flow-remediator/run-001`

- `cache/flows/`: fetched flow JSON files (`<uuid>_<version>.json`)
- `fetch/fetch_log.jsonl`
- `fetch/fetch_summary.json`
- `review/findings.jsonl`
- `review/flow_summaries.jsonl`
- `review/similarity_pairs.jsonl`
- `review/flow_review_summary.json`
- `fix/remediation_actions.jsonl`
- `fix/modified_flags.jsonl`
- `fix/version_bump_log.jsonl`
- `fix/schema_validation.jsonl`
- `fix/patch_manifest.jsonl`
- `fix/patched_flows/`
- `fix/schema_valid_flows/`
- `validate/` (same shape as `review/`)
- `publish/publish_results.jsonl`
- `publish/publish_summary.json`
- `pipeline_summary.json`

## Command Modes

## `fetch`

- Source of truth is the UUID list file, not arbitrary SQL.
- Uses MCP CRUD `select` on `flows`.
- Stores exact returned JSON locally for reproducibility.

## `review`

- Delegates to `lifecycleinventory-review --profile flow`.
- Produces `review/findings.jsonl` consumed by remediation stage.
- `lifecycleinventory-review` internally combines structured evidence extraction + optional LLM semantic review.
- If `OPENAI_API_KEY` is present, `lifecycleinventory-review flow` defaults to LLM-enabled review unless explicitly disabled.
- Use `--with-reference-context` to improve flow property / unitgroup evidence (delegated `lifecycleinventory-review` uses local `process-automated-builder` registry, not CRUD, for this context).

## `llm-remediate`

- Processes every finding independently with strict JSON I/O contract.
- Input contract per finding includes:
- `flow_uuid/base_version`
- full `original_flow_json`
- `issue` (`rule_id/message/evidence/suggestion`)
- constraints (ILCD schema, minimal change, allow no-change, classification policy)
- LLM output contract:
- `modified` (`true|false`)
- `reason` (`string`)
- `patched_flow_json` (`object|null`)
- `changes` (`[{path,before,after,rationale}]`)
- `needs_regen_service` (`true|false`)
- If classification/category/name rebuild is needed, remediator uses `process-automated-builder` product flow service and decision-tree selector for final classification landing.

## `bump-version-if-needed`

- Enforces `dataSetVersion == bump(base_version)` for changed flows.
- Never skips multiple versions (single +1 step only).
- Writes `fix/version_bump_log.jsonl`.

## `validate-schema`

- Validates patched files against ILCD FlowDataSet schema.
- Updates `fix/patch_manifest.jsonl` with `schema_valid/schema_error`.
- Writes `fix/schema_validation.jsonl`.
- Materializes `fix/schema_valid_flows/` as input set for follow-up review stage.

## `validate`

- Re-runs `lifecycleinventory-review --profile flow` on schema-valid patched flows.
- Used as regression signal before publish.

## `publish`

- Reads `fix/patch_manifest.jsonl`.
- Publishes rows that pass gates:
- `modified=true` (unless `--include-unchanged`)
- `schema_valid=true`
- base-version drift check (default enabled)
- Uses patched version from manifest/file (publish stage no longer auto-bumps version).
- Publishes via MCP CRUD `insert` (append-only).
- Concurrency handling:
- If `base_version` mismatches latest DB version but latest payload is semantically equal to local base (ignoring version), publish auto-retargets to `latest+1`.
- If insert fails with version-conflict and latest payload equals current payload (ignoring version), mark `skipped` as idempotent.
- If insert fails with version-conflict and latest payload still matches local base semantics (or latest is unreadable under current key scope), retry with sequential `+1` versions up to `--version-conflict-retries`.
- If retries are exhausted or base semantics are confirmed drifted, keep `conflict` and require rerun/remediation.

Recommended default:

- `--mode dry-run`
- keep base-version check enabled (do not pass `--skip-base-check`)

## Product Flow Regeneration

Use `regen-product-flow` when patching is insufficient (for example classification/category or canonical name changes).

This subcommand rebuilds payloads by reusing:

- `process-automated-builder/tiangong_lca_spec/product_flow_creation/service.py`

## Safety Gates

- No direct database access in the skill.
- UUID scope is externally curated.
- Remediation actions and manifest are persisted before publish.
- Publish is append-only (`insert`).
- Schema-valid gate is required for publish.
- Base version drift check is enabled by default.
