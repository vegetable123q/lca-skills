# Output Schemas

## `review/findings.jsonl`

Produced by `lifecycleinventory-review --profile flow` and consumed by `flow-remediator-publisher llm-remediate`.

One JSON object per finding.

Common fields:

- `flow_uuid`
- `base_version`
- `severity`: `error|warning|info`
- `rule_id`
- `message`
- `fixability`: `auto|manual|review-needed`
- `evidence` (optional object)
- `suggested_action` (optional)
- `source` (optional, e.g. `rule` or `llm`)
- `confidence` (optional, mainly for LLM findings)

## `fix/remediation_actions.jsonl`

One row per finding remediation attempt (append-only log for orchestration/debugging).

Key fields:

- `flow_uuid`
- `base_version`
- `finding_index`
- `issue` (normalized finding payload)
- `input_contract`:
- `flow_uuid`
- `base_version`
- `original_flow_json` (full flow JSON at remediation time)
- `issue`
- `constraints`
- `llm_output`:
- `modified`: `true|false`
- `reason`: `string`
- `patched_flow_json`: `object|null`
- `changes`: `[{path,before,after,rationale}]`
- `needs_regen_service`: `true|false`
- `status`: `applied|no_change|no_effect|llm_error|llm_unavailable|invalid_response_missing_patch|regen_failed|schema_failed`
- `modified_requested`: `bool`
- `modified_applied`: `bool`
- `schema_valid` (optional bool)
- `schema_error` (optional string)
- `regen_meta` (optional object)

## `fix/modified_flags.jsonl`

One row per flow UUID.

Key fields:

- `flow_uuid`
- `base_version`
- `modified`: `true|false`
- `finding_count`
- `applied_issue_count`
- `schema_valid`
- `schema_error` (optional)
- `patched_file` (optional; present when file emitted)

## `fix/version_bump_log.jsonl`

One row per manifest entry handled by `bump-version-if-needed`.

Key fields:

- `flow_uuid`
- `base_version`
- `before_version`
- `expected_version`
- `after_version`
- `patched_file`
- `status`: `ok|skipped_unchanged|error`
- `bumped`: `true|false`

## `fix/patch_manifest.jsonl`

One row per patched/copied flow file used by downstream stages.

Key fields:

- `flow_uuid`
- `base_version`
- `patched_version_before_publish`
- `source_file`
- `patched_file`
- `changed`
- `schema_valid`
- `schema_error`
- `version_bumped` (optional)
- `before_sha256`
- `after_sha256`

## `publish/publish_results.jsonl`

One row per publish attempt.

Status values:

- `dry-run`
- `inserted`
- `conflict`
- `error`
- `skipped` (for schema gate or explicit gating)

Important fields:

- `flow_uuid`
- `base_version`
- `latest_version_checked`
- `new_version`
- `mode`
- `status`
- `reason` (when skipped/conflict/error)
- `insert_result` (when `insert`)
- `version_retargeted` / `retarget_reason` (when base drift but semantic-equal auto-retarget applied)
- `latest_version_after_conflict` / `retry_version` / `retried_after_conflict` (when insert conflict retry path triggered)
- `conflict_retry_basis` / `retry_attempts` (when blind or semantic-guided sequential +1 retry is used)

## Alignment Target

Keep this skill compatible with evolving `lifecycleinventory-review --profile flow` outputs by:

- mapping external review findings into the same `findings.jsonl` shape
- preserving `patch_manifest.jsonl` publish contract
- keeping publish append-only and versioned
