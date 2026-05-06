# Workflow — Patent → Lifecyclemodel

Two paths:
- **Plan-driven (preferred).** LLM writes one compact `plan.json`; the driver first normalizes that file and then generates everything else. Used in 95% of cases.
- **Manual.** LLM authors flows, UUIDs, ILCD datasets, and orchestrator-request by hand. Only needed when the plan format can't express the process (rare).

Every stage shells out to an existing skill's published script.

## Plan-driven path

### Stage A — Parse source (once)

Read the source document ONCE and produce `output/<SOURCE>/plan.json` from `assets/plan.template.json`. Capture:
- source metadata (id, title, assignee)
- goal (functional unit, boundary)
- every distinct flow that appears as an input, output, or intermediate
- one entry in `processes[]` per unit operation, with `inputs`, `outputs`, `reference_output_flow`, `classification`, `technology`, `comment`, `step_id`
- keep processes non-black-box when exchanges can be measured, calculated, or estimated from the patent and estimator scripts
- set `black_box: true` only when critical material, product, or operation data remain missing and the specific unit operation cannot form a defensible inventory

**Edge convention:** reuse the same `flow_key` as upstream Output and downstream Input. That is the ONLY thing that produces edges downstream.

**Black-box convention:** `black_box: true` is a last-resort semantic fallback, not a topology change. Do not mark a whole patent route black-box because some exchanges are missing; split the route and black-box only the unit operation with the critical data gap. Edges still come only from shared `flow_key`. The generated ILCD dataset will stay structurally valid, but its comments will state that the process is item-based and black-box because critical quantitative inventory data are missing.

Do not re-read the source after `plan.json` is committed. Everything downstream reads only `plan.json`.

### Stage B — One command

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

When Supabase read env is set, the materializer automatically delegates to
`tiangong flow list --state-code 0 --state-code 100 --all --page-size 1000 --json`
and writes `output/<SOURCE>/flow-scope.json` before resolving exchanges. Pass
`--flow-scope-file <file>` to freeze a reviewed scope; use `--no-remote-flow-scope`
only for offline tests.

What it runs:
1. `normalize-plan.mjs`
   - fills defaults
   - validates `reference_output_flow`
   - enforces `black_box -> unit:item`
   - rewrites the authored plan in place so later stages read one canonical file
2. `materialize-from-plan.mjs`
   - `flow-scope.json` from remote database rows when env is present and no explicit scope file was supplied
   - `flow-resolution.json`, reusing unique existing DB matches by English name, Chinese name, aliases, or `existing_flow_ref`
   - `flows/NN-<proc_key>.json` per process
   - `uuids.json` (one UUID per `flow_key` + one per `proc_key` + one source UUID)
   - `runs/<NN>-<proc_key>/` via `process-automated-builder auto-build`
   - `runs/<SOURCE>-combined/exports/processes/<proc_uuid>_<ver>.json` per process (ILCD)
   - `runs/<SOURCE>-combined/{cache,manifests}/*` copied from the first scaffold run
   - `manifests/lifecyclemodel-manifest.json`
3. `lifecyclemodel-automated-builder build` → `lifecyclemodel-run/…/tidas_bundle/lifecyclemodels/<model_uuid>_<ver>.json`
4. Driver reads Stage 3 output + normalized `plan.json` + `uuids.json` → writes `orchestrator-request.json`
5. `lifecyclemodel-recursive-orchestrator` plan → execute → publish → `orchestrator-run/publish-summary.json`
6. Optional publish execution: driver writes `publish-request.json` from `orchestrator-run/publish-bundle.json` and delegates to `tiangong publish run`.

### Stage C — Verify

```bash
jq '{process_count, edge_count, multiplication_factors}' \
   output/<SOURCE>/lifecyclemodel-run/models/<SOURCE>-combined/summary.json
cat output/<SOURCE>/orchestrator-run/publish-summary.json
```

Success: `edge_count == processes-1` (linear chain) or higher (branched), `publish-summary.lifecyclemodel_count >= 1`.

Also inspect `flow-resolution.json`: raw materials, utilities, elementary flows, electricity, water, oxygen, fuels, wastes, and common reagents should reuse existing DB flows where unique matches exist; generated flows should be limited to unresolved patent-specific intermediates/results.
If one process is black-box, inspect the generated process dataset comment and confirm it includes the black-box note.

### Stage D — Optional database publish

After reviewing Stage C outputs, publish the generated bundle through the unified CLI:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --base output/<SOURCE> \
  --publish-only --commit --json
```

For a single full command from `plan.json` through publish:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --publish-to-db --commit --json
```

Without `--commit`, Stage D creates a dry-run publish request. Stage D never writes directly; it delegates `publish-request.json` to `tiangong publish run`.

## Manual path (fallback)

Use only if the plan format cannot express the process (unusual structures, side-streams with complex allocation, etc.). Otherwise prefer the plan path — it is faster, less error-prone, and the driver is tested.

1. `output/<SOURCE>/flows/NN-<layer>.json` from `assets/flow.template.json`
2. Scaffold runs:
   ```bash
   for f in output/<SOURCE>/flows/*.json; do
     node process-automated-builder/scripts/run-process-automated-builder.mjs auto-build \
       --flow-file "$(pwd)/$f" --operation produce \
       --out-dir  "$(pwd)/output/<SOURCE>/runs/$(basename "$f" .json)" --json
   done
   ```
3. Allocate UUIDs:
   ```bash
   node patent-to-lifecyclemodel/scripts/allocate-uuids.mjs \
     --flows mofs,ncm_oxide,... --processes mofs_proc,... > output/<SOURCE>/uuids.json
   ```
4. Author `runs/<SOURCE>-combined/exports/processes/<uuid>_<ver>.json` from `assets/processDataSet.template.json` for each layer; consolidate all datasets into the single source-specific combined dir. Copy `cache/process_from_flow_state.json` and `manifests/*.json` from any scaffold run into `runs/<SOURCE>-combined/`.
5. Write `manifests/lifecyclemodel-manifest.json` from `assets/lifecyclemodel-manifest.template.json` pointing at `runs/<SOURCE>-combined/`.
6. Write `orchestrator-request.json` from `assets/orchestrator-request.template.json`.
7. Run Stages 5 and 6, then optional Stage D:
   ```bash
   node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs --base output/<SOURCE> --all --json
   node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs --base output/<SOURCE> --publish-only --commit --json
   ```
