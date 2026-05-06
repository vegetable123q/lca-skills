# Remote Cleanup Workflow

## Scope Collection

Collect the delete/replace set from local artifacts, not from loose name search:

- lifecyclemodels: `publish-run/publish-report.json` and `lifecyclemodel-run/models/*/tidas_bundle/lifecyclemodels/*.json`
- processes: `publish-run/publish-report.json`, `runs/*/exports/processes/*.json`, and lifecyclemodel `referenceToResultingProcess`
- flows: `uuids.json.flows` and `flow-publish-rows.json`
- sources: `uuids.json.srcs` and publish report source entries

Use source IDs as an additional filter in reports, but do not delete by name alone.

## Delete Path

Remote hard delete is valid only through an explicit supported CLI/runtime command. If the current CLI has no delete command, or RLS returns `permission denied`, record the blocker and move to replace-by-publish. Do not embed decoded API-key credentials, direct Supabase clients, or table-specific delete code in this repo.

Recommended report shape:

```json
{
  "sources": ["CN..."],
  "requested": { "lifecyclemodels": 0, "processes": 0, "flows": 0, "sources": 0 },
  "delete_supported": false,
  "blocker": "CLI has no supported hard-delete command or remote returned permission denied",
  "next_action": "republish corrected artifacts with stable IDs"
}
```

## Local Rerun Cleanup

Keep:

- `plan.json`
- source data and references
- `uuids.json` unless a stable `--seed` will reproduce identical IDs

Remove stale generated directories:

- `lifecyclemodel-run`
- `orchestrator-run`
- `publish-run`
- `flow-publish-run`
- `runs`
- `manifests`
- `flows`
- `processes`
- `artifacts/process_from_flow`

If only one stage failed, clean the exact run root named in the error instead of sweeping the whole source directory.

## Replace-by-Publish Path

Regenerate corrected artifacts with the owner skill. For patent routes:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --flow-scope-file output/<SCOPE>/flows.json \
  --all --json
```

Then publish:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --base output/<SOURCE> \
  --publish-only --commit --json
```

Expected publish behavior after failed hard delete:

- same lifecyclemodel/process IDs update existing rows
- unchanged generated flows may be skipped
- database-reused flows are not re-published
- old generated flows that are no longer referenced may remain until a supported delete capability exists

## Verification

Before commit:

- `flow-resolution.json` shows reused database flows where possible
- process exports contain names, exchanges, reference units, and real flow references
- lifecyclemodel summary has distinct process count and edges
- `tiangong flow scan-process-flow-refs` reports all exchanges as `exists_in_target`

After commit:

- `publish-report.json` has no failed lifecyclemodels/processes
- `flow-publish-run/publish-report.json` has no failed generated flows
- any undeleted stale rows are explicitly listed as blocked residuals, not silently ignored
