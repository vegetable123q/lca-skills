---
name: patent-to-lifecyclemodel
description: Turn a patent or SOP text into a complete TianGong TIDAS lifecyclemodel. Use when the user hands you descriptive text ("extract a Lifecyclemodel from this patent", "将 <document> 抽取为一个完整的 Lifecyclemodel") and has no pre-existing flow or process datasets. Composes process-automated-builder, lifecyclemodel-automated-builder, and lifecyclemodel-recursive-orchestrator through a local-only pipeline.
---

# Patent → Lifecyclemodel SOP

Composition skill. Reuses three existing builder skills; does not re-implement them.

## The only file the LLM authors

After ONE pass over the source document, write `output/<SOURCE>/plan.json` from `assets/plan.template.json`. Then run the driver. The driver normalizes the plan first, so the authored file can stay minimal and unambiguous.

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

The driver: `materialize-from-plan` (flows, UUIDs, ILCD datasets, combined run, lifecyclemodel manifest) → Stage 5 `lifecyclemodel-automated-builder` → auto-generate `orchestrator-request.json` → Stage 6 `lifecyclemodel-recursive-orchestrator` (plan → execute → publish).

Reference: `output/CN110980817B/plan.json` — a 3-process lithium battery cathode example.
Reference fallback: `patent-to-lifecyclemodel/assets/example-black-box-plan.json` — mixed detailed + black-box example derived from `CN110980817B`.

## Plan contract (minimum)

```jsonc
{
  "source":    { "id": "<PATENT-ID>", "title": "...", "assignee": "..." },
  "goal":      { "name": "...", "functional_unit": {"amount": 1, "unit": "kg"}, "boundary": "..." },
  "geography": "CN",
  "reference_year": "2019",
  "flows":     { "<flow_key>": { "name_en": "...", "name_zh": "...", "unit": "kg" } },
  "processes": [
    {
      "key": "<proc_key>", "step_id": "S1", "name_en": "...", "name_zh": "...",
      "classification": ["..."], "technology": "...", "comment": "...",
      "black_box": false,
      "reference_output_flow": "<flow_key of the reference product OUTPUT>",
      "inputs":  [ {"flow": "<flow_key>", "amount": 0, "derivation": "Measured|Estimated"} ],
      "outputs": [ {"flow": "<flow_key>", "amount": 1, "derivation": "Measured"} ]
    }
  ]
}
```

**Edge rule:** use the same `flow_key` as upstream Output and downstream Input. No other glue is required — `edge_count` == number of inter-process shared flows.

## Derivation vocabulary

Every exchange has one of three `derivation` values. Pick based on the source, not convenience.

| Value | When | Required fields |
| --- | --- | --- |
| `Measured` | The source text gives the exact quantity (e.g. `"weigh 3.10 kg NCM oxide"`). | — |
| `Calculated` | Derived from source-given molar ratios / chemical formulas / recipe fractions / concentration-flow-time. | `calc_note` (one line: formula + numbers) |
| `Estimated` | Source is silent; value comes from an engineering default. | For electricity/water/O₂ the default MUST come from `scripts/estimate-utilities.mjs` (see below). |

## Defaults from `estimate-utilities.mjs`

For utilities (electricity, water wash, pure-O₂ consumption) do not invent numbers. Call the estimator and copy its `formula_ref` into the flow's `comment`:

```bash
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode electricity \
  --params '{"process_type":"muffle_lab_large","T_C":800,"duration_h":20,"batch_charge_kg":4.98,"product_mass_kg":3.34,"phase":"solid"}'
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode water \
  --params '{"solid_mass_kg":3.1,"wash_regime":"coprecipitate","product_mass_kg":3.1}'
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode oxygen \
  --params '{"pure_oxygen":true,"duration_h":20,"furnace_volume_m3":0.05,"product_mass_kg":3.34}'
```

Rules:
- **Pure-O₂**: only declare an `o2` input when the source names a pure-O₂ atmosphere. Set the process's `pure_oxygen: true`. `normalize-plan` rejects an `o2` input without this flag.
- **Water wash**: always also declare a `wastewater` output of the same magnitude (mass balance).
- **Electricity breakdown**: batch reactor + subsequent calcination go in two estimator calls; sum the `kWh_per_kg` values for the single `electricity` exchange of that step.

## Canonical flows (hydrate → anhydrous)

If a reagent is a hydrate in the patent but the database has only the anhydrous form (or vice-versa), declare both flows and point the less canonical one at the canonical one:

```json
"flows": {
  "coso4_7h2o": {"name_en": "Cobalt sulfate heptahydrate", "unit": "kg",
                  "canonical_flow_key": "coso4", "conversion_factor": 0.5513},
  "coso4":      {"name_en": "Cobalt sulfate",              "unit": "kg"}
}
```

`conversion_factor = MW(anhydrous) / MW(hydrate) = 154.99 / 281.10 ≈ 0.5513`. `materialize-from-plan` emits the ILCD exchange pointing at the canonical flow, multiplies the amount by `conversion_factor`, and records the conversion in the exchange `generalComment`.

## Black-box fallback

When a process exists in the patent but the material input amounts are not disclosed clearly enough to defend a mass-based inventory:

- mark that process with `"black_box": true`
- set every flow used by that process to `"unit": "item"`
- use `amount: 1` unless the source gives a defensible item count
- keep `derivation: "Estimated"` for those exchanges
- explain in `comment` that the process is a black-box step and item-based because the source omits defensible quantities

The script will carry the black-box marker into the generated ILCD comments. Do not mix `kg` and `item` within one black-box process.

## Hard rules

- One combined run. The builder infers the graph from ONE `runs/combined/` dir; the driver handles this.
- Shared flow UUIDs are the edges. `allocate-uuids.mjs` derives them from `plan.flows`.
- ILCD contract: every `processDataSet` needs `common:UUID`, `quantitativeReference.referenceToReferenceFlow` pointing at an existing `@dataSetInternalID`, and matching exchanges. `materialize-from-plan.mjs` handles this.
- For `"black_box": true`, every input/output flow used by that process must declare `unit: "item"`, every exchange amount must be 1, and any downstream process consuming one of those item-unit flows automatically inherits a `Missing important data:` marker in its comment.
- `O2` inputs require `"pure_oxygen": true` on the owning process. `normalize-plan` fails otherwise.
- `derivation: "Calculated"` requires a non-empty `calc_note` on the same exchange.
- Composite / coated / doped final products: do NOT back-compute upstream reagent masses by dividing the functional-unit mass by the final product's MW; the final product is not a single pure phase. Use the un-coated / pre-doping matrix MW to run stoichiometry, then mass-balance the coating/doping mass separately (see `references/pitfalls.md` #10).
- Use the driver path instead of calling sub-scripts manually when you want the lowest-token, lowest-ambiguity workflow: it now runs `normalize-plan.mjs` before materialization.
- Local-only. No remote writes. For remote publish, hand off to `lca-publish-executor` after review.

## Read on demand

- `references/workflow.md` — stage-by-stage recipe (plan-first + manual fallback)
- `references/pitfalls.md` — 9 real traps from the reference run
- `references/artifacts.md` — file ownership map

## Fast triage

| Symptom | Fix |
| --- | --- |
| `edge_count: 0` | Missing shared `flow_key` between process inputs/outputs — fix `plan.json` |
| `built_model_count: N` for N processes | Driver was bypassed; use `--plan` |
| `run root already exists` on re-run | `rm -rf output/<SOURCE>/{artifacts,flows,runs,manifests,lifecyclemodel-run,orchestrator-run,orchestrator-request.json,uuids.json}` then re-run |
| `relation_count: 0` at publish | Stage 5 did not produce a model — inspect `lifecyclemodel-run/` logs |
| `derivation=Calculated but no calc_note` | Add a one-line `calc_note` to that exchange or switch to `Estimated` |
| `lists an O2 input but pure_oxygen!=true` | Set `pure_oxygen: true` on the process (only if source says pure O₂) or remove the O2 exchange |
| `unit=item requires amount=1` | Black-box/item flow has a non-unity amount somewhere — set every exchange of that flow to 1 |
| `canonical_flow_key … not found` | Declare both flows in `plan.flows{}` (hydrate and anhydrous) |

## When NOT to use

- ILCD process datasets already exist → `lifecyclemodel-automated-builder` directly
- Lifecyclemodel exists, need resulting process → `lifecyclemodel-resulting-process-builder`
- Remote publish → `lca-publish-executor`
