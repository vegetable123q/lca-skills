---
name: tiangong-lca-remote-cleanup
description: Use when TianGong LCA remote lifecyclemodel, process, flow, or source records must be removed, replaced, or republished after bad data was committed, especially when cleanup must preserve deterministic IDs, remove current-user owned rows, and use supported CLI-backed remote writes.
---

# TianGong LCA Remote Cleanup

Use this skill for remote cleanup plans, deletion attempts, and corrected republish runs after bad TianGong data was committed.

## Contract

- Treat remote cleanup as production data maintenance.
- Use native `tiangong` CLI capabilities first. Do not add Supabase auth, raw REST deletes, MCP transports, or private runtime logic inside this skill.
- For current-user owned lifecyclemodel/process/flow deletion, use `tiangong-lca admin cleanup-owned`; it must filter by the authenticated user's `user_id`.
- If hard delete is missing from the CLI or the authenticated account gets `permission denied`, stop the direct-table delete path, record the blocker, and republish corrected artifacts with the same deterministic IDs when possible.
- If the missing capability is required, add it to `tiangong-lca-cli` first, then update this wrapper skill.

## Workflow

1. Build an exact local scope from committed artifacts: `publish-report.json`, `flow-publish-rows.json`, `uuids.json`, lifecyclemodel bundle files, and process exports.
2. Dry-run remote cleanup before any write. Report counts by table and source.
3. For user-wide owned deletion, run:

   ```bash
   tiangong-lca admin cleanup-owned --out-dir <cleanup-dir> --json
   tiangong-lca admin cleanup-owned --out-dir <cleanup-dir> --commit --json
   ```

   The CLI should route lifecyclemodels through `delete_lifecycle_model_bundle` and process/flow drafts through `cmd_dataset_delete`.
4. Preserve identity files. Do not delete `uuids.json` during local cleanup unless the rerun passes the same explicit seed and you have verified IDs remain stable.
5. Clean only generated run directories before rebuild: `lifecyclemodel-run`, `orchestrator-run`, `publish-run`, `flow-publish-run`, `runs`, `manifests`, `flows`, `processes`, and `artifacts/process_from_flow`.
6. Regenerate with the owning skill, normally `$patent-to-lifecyclemodel`; let that skill fetch or reuse its repo-level remote database flow scope so existing flows are reused.
7. Verify process-flow references with `tiangong-lca flow scan-process-flow-refs`; every exchange should classify as `exists_in_target`.
8. Publish through `tiangong-lca flow publish-reviewed-data` and `tiangong-lca publish run`, never through ad hoc remote writes.
9. After publish, fetch or scan the remote result and report deleted, overwritten, skipped, and blocked rows separately.

## Failure Rules

- `permission denied for table ...`: do not retry with a hand-written Supabase delete script. Use same-ID overwrite if available and state that hard delete needs a CLI/runtime capability.
- `Only draft datasets can be deleted`: report the `state_code` residuals. The reusable cleanup command can delete draft process/flow rows, but server-side policy may block published or locked rows such as `state_code=20`.
- `Lifecycle models must use bundle create and delete commands`: use the lifecyclemodel bundle delete path, not direct table deletion.
- `run root already exists and is not empty`: remove the stale local run directory named in the error, then rerun.
- Changed UUIDs after cleanup: restore the previous `uuids.json` from git or rerun with the original seed before publishing.
- Ambiguous flow reuse: do not create a duplicate by default. Use reviewed `existing_flow_ref` values or keep the unresolved flow in the publish set with explicit review notes.

## Reference

Read [references/workflow.md](references/workflow.md) when executing a multi-source cleanup or when a delete attempt fails.
