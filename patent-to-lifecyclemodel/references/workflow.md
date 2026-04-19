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
- if a process exists but its material quantities are not defensible, set `black_box: true`, switch that process's flows to `unit: "item"`, and state the black-box rationale in `comment`

**Edge convention:** reuse the same `flow_key` as upstream Output and downstream Input. That is the ONLY thing that produces edges downstream.

**Black-box convention:** `black_box: true` is a semantic fallback, not a topology change. Edges still come only from shared `flow_key`. The generated ILCD dataset will stay structurally valid, but its comments will state that the process is item-based and black-box because the source omitted quantitative inventory detail.

Do not re-read the source after `plan.json` is committed. Everything downstream reads only `plan.json`.

### Stage B — One command

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

What it runs:
1. `normalize-plan.mjs`
   - fills defaults
   - validates `reference_output_flow`
   - enforces `black_box -> unit:item`
   - rewrites the authored plan in place so later stages read one canonical file
2. `materialize-from-plan.mjs`
   - `flows/NN-<proc_key>.json` per process
   - `uuids.json` (one UUID per `flow_key` + one per `proc_key` + one source UUID)
   - `runs/<NN>-<proc_key>/` via `process-automated-builder auto-build`
   - `runs/combined/exports/processes/<proc_uuid>_<ver>.json` per process (ILCD)
   - `runs/combined/{cache,manifests}/*` copied from the first scaffold run
   - `manifests/lifecyclemodel-manifest.json`
3. `lifecyclemodel-automated-builder build` → `lifecyclemodel-run/…/tidas_bundle/lifecyclemodels/<model_uuid>_<ver>.json`
4. Driver reads Stage 3 output + normalized `plan.json` + `uuids.json` → writes `orchestrator-request.json`
5. `lifecyclemodel-recursive-orchestrator` plan → execute → publish → `orchestrator-run/publish-summary.json`

### Stage C — Verify

```bash
jq '{process_count, edge_count, multiplication_factors}' \
   output/<SOURCE>/lifecyclemodel-run/models/combined/summary.json
cat output/<SOURCE>/orchestrator-run/publish-summary.json
```

Success: `edge_count == processes-1` (linear chain) or higher (branched), `publish-summary.lifecyclemodel_count >= 1`.

If one process is black-box, also inspect the generated process dataset comment and confirm it includes the black-box note.

### Stage D — (Optional) Remote publish

Out of scope; see `lca-publish-executor`.

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
4. Author `runs/combined/exports/processes/<uuid>_<ver>.json` from `assets/processDataSet.template.json` for each layer; consolidate all datasets into the single `runs/combined/` dir. Copy `cache/process_from_flow_state.json` and `manifests/*.json` from any scaffold run into `runs/combined/`.
5. Write `manifests/lifecyclemodel-manifest.json` from `assets/lifecyclemodel-manifest.template.json` pointing at `runs/combined/`.
6. Write `orchestrator-request.json` from `assets/orchestrator-request.template.json`.
7. Run Stages 5 and 6:
   ```bash
   node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs --base output/<SOURCE> --all --json
   ```
