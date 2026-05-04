# Pitfalls

Observed while building `output/CN110980817B/`. Keep this file as a quick troubleshooting index; see `conversion-guide.zh-CN.md` for the full method, formulae, and references.

| # | Symptom | Cause | Fix |
| ---: | --- | --- | --- |
| 1 | `edge_count: 0` despite multiple processes | Upstream outputs and downstream inputs do not share the same flow UUID. | Reuse the same `flow_key` in `plan.json`; `allocate-uuids` then assigns one UUID. |
| 2 | `built_model_count: N` for N processes | `lifecyclemodel auto-build` grouped separate run dirs into separate single-node models. | Use the driver so all ILCD datasets land in one `runs/combined/` dir. |
| 3 | `LIFECYCLEMODEL_AUTO_BUILD_STATE_NOT_FOUND` | Combined dir has exports but lacks the process-build state marker. | Let `materialize-from-plan` copy `cache/process_from_flow_state.json` and manifests from the first scaffold run. |
| 4 | `LIFECYCLEMODEL_AUTO_BUILD_REFERENCE_FLOW_MISSING` | `quantitativeReference.referenceToReferenceFlow` is missing or points at no exchange. | Ensure each process output includes `reference_output_flow`; the materializer points to that output exchange. |
| 5 | Process auto-build produces no process datasets | `process auto-build` only scaffolds local stages in this workflow. | Treat scaffold runs as audit/support folders; ILCD datasets are materialized from `plan.json`. |
| 6 | Amounts are marked `Measured` without direct source quantities | Patent recipes often give batch or range data, not per-FU inventory. | Use `Measured` only for exact quantities; use `Calculated` with `calc_note` or `Estimated` with `formula_ref`. |
| 7 | Missing material quantities are filled with fake kg values | The patent step exists, but inventory is not defensible. | Mark the process `black_box: true`, use `unit:item`, amount `1`, and explain the missing data. |
| 8 | Source patent or run outputs are not tracked by git | `data/` and `output/` are gitignored. | Archive the run folder or hand off through `lca-publish-executor` when sharing is needed. |
| 9 | Unexpected CWD-local `artifacts/` appears | Builder cache side effect from relative CLI paths. | Ignore it; canonical outputs live under `--out-dir`. |
| 10 | Re-run fails because run root already exists | Prior `artifacts/`, `lifecyclemodel-run/`, or `orchestrator-run/` collides with a new execute. | Remove generated dirs before re-running. |
| 11 | Coated/doped final product stoichiometry is off | Final product is not a single pure phase. | Size reagents on the last uncoated matrix, then mass-balance coating/doping separately. |
| 12 | Utility values vary without basis | LLM guessed electricity, water, O2, or waste factors. | Call `estimate-utilities.mjs` and copy returned amount plus `formula_ref`/`source_ref` into the exchange. |
| 13 | EIA replaces patent recipe | Plant-level EIA factors were used as if they were patent unit-operation data. | Patent first, EIA second. Use EIA only as an auxiliary anchor for missing utility/waste quantities on named operations. |

Generated folders that are safe to remove for a clean plan-driven rerun:

```bash
rm -rf output/<SOURCE>/{artifacts,flows,runs,manifests,lifecyclemodel-run,orchestrator-run,orchestrator-request.json,uuids.json}
```
