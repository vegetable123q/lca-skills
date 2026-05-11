# Workflow — Patent → Lifecyclemodel

Two paths:
- **Plan-driven (preferred).** LLM writes one compact `plan.json`; the driver first normalizes that file and then generates everything else. Used in 95% of cases.
- **Manual.** LLM authors flows, UUIDs, ILCD datasets, and orchestrator-request by hand. Only needed when the plan format can't express the process (rare).

Every stage shells out to an existing skill's published script.

## Plan-driven path

### Stage A — Parse source (once)

Read the source document ONCE and produce `output/<SOURCE>/plan.json` from `assets/plan.template.json`. Capture:
- source metadata (id, title, assignee/company, inventor when available, priority/publication/grant dates, best patent year, and extra patent metadata such as URL or family members)
- goal (functional unit, boundary)
- every distinct flow that appears as an input, output, or intermediate
- one entry in `processes[]` per unit operation, with `inputs`, `outputs`, `reference_output_flow`, `classification`, `technology`, `comment`, `step_id`
- keep processes non-black-box when exchanges can be measured, calculated, or estimated from the patent and estimator scripts
- set `black_box: true` only when critical material, product, or operation data remain missing and the specific unit operation cannot form a defensible inventory

**Edge convention:** reuse the same `flow_key` as upstream Output and downstream Input. That is the ONLY thing that produces edges downstream.

**Input flow convention:** process inputs must resolve to product flows whenever the database has a suitable product flow. Do not put emission-side elementary flows on inputs, including categories like `Emissions > Emissions to soil > Emissions to non-agricultural soil`. Elementary/basic flows are allowed on inputs only for explicit gases such as oxygen, nitrogen, argon, hydrogen, air, or CO2.

**Black-box convention:** `black_box: true` is a last-resort semantic fallback, not a topology change. Do not mark a whole patent route black-box because some exchanges are missing; split the route and black-box only the unit operation with the critical data gap. Edges still come only from shared `flow_key`. The generated ILCD dataset will stay structurally valid, but its comments will state that the process is item-based and black-box because critical quantitative inventory data are missing.

Do not re-read the source after `plan.json` is committed. Everything downstream reads only `plan.json`.

### Stage B — One command

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

The materializer requires remote database flow resolution. Before pulling the full remote scope, it
checks `output/patent-to-lifecyclemodel-used-flows.json`, a minimal direct-reuse cache containing
only existing database flow `id`, `version`, `name`, `unit`, and exact reusable names from previous
successful resolutions. A cache hit is valid only for exact name + exact unit and only when it maps
to one database flow. If any plan flow remains unresolved, the materializer loads credentials from
the current environment or the adjacent TianGong CLI checkout's `.env` (`tiangong-cli/.env` or
`TIANGONG_LCA_CLI_DIR/.env`), delegates to
`tiangong flow list --state-code 0 --state-code 100 --all --page-size 1000 --json`, and writes
one repo-level `output/patent-to-lifecyclemodel-flow-scope.json` before resolving exchanges. Later
examples reuse that same repo-level scope; do not write full remote scope rows inside each
`output/<SOURCE>/`. `--no-remote-flow-scope` and `--flow-scope-file` are forbidden.

What it runs:
1. `normalize-plan.mjs`
   - fills defaults
   - validates `reference_output_flow`
   - enforces `black_box -> unit:item`
   - rewrites the authored plan in place so later stages read one canonical file
2. `materialize-from-plan.mjs`
   - exact direct cache reuse from `output/patent-to-lifecyclemodel-used-flows.json` when every reused name/unit maps to one existing database flow
   - `output/patent-to-lifecyclemodel-flow-scope.json` from live remote database rows fetched through the TianGong CLI when any plan flow is not directly cached or explicitly referenced
   - `flow-resolution.json`, automatically reusing the best existing DB match by English name, Chinese name, aliases, normalized names, or `existing_flow_ref`
   - input-side flow filtering that rejects non-gas elementary/emission candidates and prefers product flows
   - `flows/NN-<proc_key>.json` per process
   - `uuids.json` (one UUID per `flow_key` + one per `proc_key` + one source UUID)
   - `runs/<NN>-<proc_key>/` via `process-automated-builder auto-build`
   - `runs/<SOURCE>-combined/exports/processes/<proc_uuid>_<ver>.json` per process (ILCD)
   - `runs/<SOURCE>-combined/{cache,manifests}/*` copied from the first scaffold run
   - `manifests/lifecyclemodel-manifest.json`, including `basic_info.source` prefilled from patent metadata and `basic_info.source.extra_metadata` for additional source fields so downstream CLI manifests can distinguish company/year/source variants
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

Also inspect `flow-resolution.json`: raw materials, utilities, electricity, water, oxygen, fuels, wastes, and common reagents should reuse existing DB flows where candidates exist. Input-only non-gas materials should use product-flow candidates and must not resolve to emission-side elementary categories. Explicit gas inputs may use elementary/basic flows. Input-only materials should use the nearest compatible database flow when exact matching fails. Multiple candidates and normalized candidates are resolved automatically by exactness, state code, version, modified time, and id order. Nearest input matching remains substance-specific: authored names and aliases are tokenized, process-only modifiers are ignored, common metal symbols such as `Ni`, `Co`, and `Mn` plus Chinese metal names such as `金属镍`/`金属钴`/`金属锰` are normalized to element names, a bare generic `Metal` candidate is rejected when the query names a specific metal, slag/residue/remediation/waste-like rows are rejected for metal feedstock queries, and element-specific product-flow hits such as nickel metal, electrodeposit cobalt, or metallic manganese rank ahead of broad material rows. A flow produced by one patent process and consumed by another in the same plan is an internal patent intermediate and remains generated unless the plan explicitly supplies `existing_flow_ref`. Generated flows should be limited to patent-specific intermediates/results/wastes or rows where no compatible database flow can be found, and publish rows should include only generated flows actually used by process exchanges. The used-flow cache must stay minimal: never store generated flows, unresolved rows, or full remote rows.
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
