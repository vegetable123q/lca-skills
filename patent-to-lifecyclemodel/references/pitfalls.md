# Pitfalls observed during the reference run

Every item below was actually hit while building `output/CN110980817B/`. Read this before deviating from `workflow.md`.

## 1. `edge_count: 0` despite N processes

**Symptom:** `lifecyclemodel auto-build` completes, reports `built_model_count: 1` and `process_count: N`, but `edge_count: 0`.

**Root cause:** the builder infers edges by scanning exchanges for `referenceToFlowDataSet.@refObjectId` values that appear as Output in one process and Input in another. If you generated fresh UUIDs independently for each dataset, none match → no edges.

**Fix:** centralize UUID allocation **before** authoring datasets. Use `scripts/allocate-uuids.mjs` (or equivalent) to produce one `uuids.json`, then reference only those UUIDs in every dataset.

## 2. `built_model_count: N` (one model per run) instead of 1

**Symptom:** three processes, three runs, output has three single-node models with `edge_count: 0` each.

**Root cause:** `lifecyclemodel auto-build` groups processes **by run directory**. It does not look across runs. Three runs ⇒ three models.

**Fix:** consolidate. Create one `runs/combined/exports/processes/` dir and put every ILCD dataset JSON in there. Point the manifest's `local_runs[0]` at that combined dir. The original per-layer scaffold runs from Stage 2 are kept for audit, not consumed by Stage 5.

## 3. `LIFECYCLEMODEL_AUTO_BUILD_STATE_NOT_FOUND`

**Symptom:** auto-build refuses the combined run with "missing state file".

**Root cause:** the CLI expects `<run>/cache/process_from_flow_state.json` to exist as a marker of a valid process-build run. The combined dir has `exports/processes/` but nothing else.

**Fix:** copy any one scaffold run's `cache/process_from_flow_state.json` and `manifests/*.json` into the combined run. Those files are only sanity-checks for this stage; the actual graph inference reads `exports/processes/` only.

## 4. `LIFECYCLEMODEL_AUTO_BUILD_REFERENCE_FLOW_MISSING`

**Symptom:** auto-build rejects a dataset citing missing `referenceToReferenceFlow`.

**Root cause:** `quantitativeReference.referenceToReferenceFlow` is missing, empty, or points at an `@dataSetInternalID` that no exchange has.

**Fix:** set it to the `@dataSetInternalID` of the **output** exchange that represents the step's reference product. Keep `@dataSetInternalID` values unique within a dataset.

## 5. Process auto-build only scaffolds — no datasets come out

**Symptom:** `process auto-build` reports `status: prepared_local_process_auto_build_run` but `exports/processes/` is empty.

**Root cause:** the CLI's `auto-build` command sets up stages 01 → 10 but does not execute them. Stages 03-09 rely on LLM / KB / unstructured-parser modules that are out of scope for this skill family.

**Fix:** accept that scaffolding is as far as Stage 2 goes. Hand-author ILCD datasets in Stage 4.

## 6. Amounts are estimates, not measurements

The patent text rarely specifies per-kg-of-product amounts — it gives batch recipes ("weigh 3.10 kg NCM…") and ranges ("15-40 h"). Author with `"dataDerivationTypeStatus": "Estimated"` and note any unit conversions. Do not mark `"Measured"` unless the source really supplies per-functional-unit numbers.

## 6a. Truly missing inputs should become `item`, not fake kilograms

If a process is clearly present in the source but the material input quantities are not defensible even as normalized estimates, do not invent a kilogram inventory. Mark the process with `"black_box": true`, switch every flow used by that process to `unit: "item"`, and explain why in `comment`.

## 7. `data/` is gitignored

The source patent lives in `data/`, which is in `.gitignore`. That's fine — the `output/<SOURCE>/` directory carries the full audit trail (flows + scaffolds + datasets + manifests). `output/` is also gitignored; if you want to share a run, archive the folder or publish via `lca-publish-executor`.

## 8. Cache side-effect: unexpected `artifacts/` subdir

When `lifecyclemodel auto-build` runs with a relative CLI dir, it may create a CWD-local `artifacts/process_from_flow/` as a hand-off cache. Ignore it — the canonical outputs live under `--out-dir`.

## 9. Orchestrator `execute` fails with "run root already exists and is not empty"

**Symptom:** second `orchestrate execute` fails with `process auto-build run root already exists`, pointing at `<base>/artifacts/process_from_flow/...`.

**Root cause:** the underlying `tiangong process auto-build` refuses to overwrite a non-empty run root. If a prior run created `<base>/artifacts/process_from_flow/<node>/`, a subsequent re-run collides.

**Fix:** remove `<base>/artifacts/`, `<base>/lifecyclemodel-run/`, and `<base>/orchestrator-run/` before re-running. The driver script does not auto-clean; do it explicitly:

```bash
rm -rf output/<SOURCE>/{artifacts,lifecyclemodel-run,orchestrator-run}
```

Authored inputs (`flows/`, `runs/combined/exports/processes/`, `manifests/`, `uuids.json`, `orchestrator-request.json`) are preserved.

**Symptom:** you divide `1 kg` of the functional unit by the MW of the pure parent phase (e.g. `LiNi0.8Co0.1Mn0.1O2 = 97.28 g/mol`) to get upstream reagent moles, and the numbers end up 2–10% off with no clear reason.

**Root cause:** the functional-unit product often carries a coating (B₂O₃, Al₂O₃), dopant, or binder. It is not a single pure phase, so it does not have a single MW. Any stoichiometry done on it is mass-balance-wrong by the coating fraction.

**Fix:** run stoichiometry on the **last uncoated intermediate** (e.g. the NCM matrix before boric-acid coating), then mass-balance the coating mass separately. For CN110980817B this means: S2 reagents are sized per kg of `Ni0.8Co0.1Mn0.1O2` (MW 90.34) and S3's NCM input is `1 kg cathode − m_coating` kg of the matrix; the coating (H₃BO₃ → B₂O₃) goes in its own exchange.

## 11. Do not invent utility numbers — call `estimate-utilities.mjs`

**Symptom:** one run says electricity = 18 kWh/kg, another similar process says 9 kWh/kg, with no defensible basis for the 2× gap.

**Root cause:** LLM guessed kW × h without normalizing to the functional unit or accounting for process type.

**Fix:** call `patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode electricity|water|oxygen` with the patent-reported temperature, time, and batch mass. Copy the returned `kWh_per_kg` / `kg_per_kg` as the exchange amount; copy the `formula_ref` into the process `comment` so reviewers can reproduce the number. Different patents driven by the same estimator are then directly comparable.
