---
name: patent-to-lifecyclemodel
description: Turn a patent or SOP text into a complete TianGong TIDAS lifecyclemodel. Use when the user hands you descriptive text ("extract a Lifecyclemodel from this patent", "将 <document> 抽取为一个完整的 Lifecyclemodel") and has no pre-existing flow or process datasets. Composes process-automated-builder, lifecyclemodel-automated-builder, and lifecyclemodel-recursive-orchestrator through a local-only pipeline.
---

# Patent -> Lifecyclemodel SOP

Composition skill. Reuses existing builder skills; it does not re-implement their logic.

## Default path

After one pass over the source document, author only:

```text
output/<SOURCE>/plan.json
```

Use `assets/plan.template.json`, then run:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

The driver runs `normalize-plan` -> `materialize-from-plan` -> `lifecyclemodel-automated-builder build` -> auto-generated `orchestrator-request.json` -> `lifecyclemodel-recursive-orchestrator` plan/execute/publish.

## Minimum plan shape

```jsonc
{
  "source": { "id": "<PATENT-ID>", "title": "...", "assignee": "..." },
  "goal": { "name": "...", "functional_unit": {"amount": 1, "unit": "kg"}, "boundary": "..." },
  "geography": "CN",
  "reference_year": "2019",
  "flows": { "<flow_key>": { "name_en": "...", "name_zh": "...", "unit": "kg" } },
  "processes": [
    {
      "key": "<proc_key>",
      "step_id": "S1",
      "name_en": "...",
      "name_zh": "...",
      "classification": ["..."],
      "technology": "...",
      "comment": "...",
      "black_box": false,
      "pure_oxygen": false,
      "reference_output_flow": "<product_output_flow_key>",
      "inputs": [{ "flow": "<flow_key>", "amount": 0, "derivation": "Measured|Calculated|Estimated" }],
      "outputs": [{ "flow": "<flow_key>", "amount": 1, "derivation": "Measured" }]
    }
  ]
}
```

Edge rule: use the same `flow_key` as upstream Output and downstream Input. No other glue creates lifecyclemodel edges.

## Authoring rules

- `Measured`: source text gives the exact quantity.
- `Calculated`: derived from source-given ratios, formulas, concentrations, flow rates, or times. Add `calc_note`.
- `Estimated`: source is silent. Electricity, water, O2, waste, and emissions must come from `scripts/estimate-utilities.mjs`; copy `formula_ref` and any `source_ref` to the exchange.
- O2 inputs require `pure_oxygen: true` on the process, and only when the source explicitly names a pure-O2 atmosphere.
- Water wash normally needs a paired `wastewater` output of the same magnitude.
- Hydrate/anhydrous aliases use `canonical_flow_key` plus `conversion_factor`; declare both flows in `flows`.
- Composite, coated, or doped final products are not single pure phases. Do stoichiometry on the last uncoated intermediate, then mass-balance coating/doping separately.
- If quantities are not defensible, set `black_box: true`, use only `unit: "item"` flows in that process, set every exchange amount to `1`, and explain why in `comment`.
- Local-only. Do not remote publish from this skill; hand off to `lca-publish-executor` after review.

## Utility estimator

```bash
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode electricity --params '<json>'
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode water --params '<json>'
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode oxygen --params '<json>'
node patent-to-lifecyclemodel/scripts/estimate-utilities.mjs --mode waste --params '<json>'
```

Use patent data first. EIA auxiliary modes are only for missing utility/waste quantities on operations already named by the patent.

## Verify

```bash
jq '{process_count, edge_count, multiplication_factors}' \
  output/<SOURCE>/lifecyclemodel-run/models/combined/summary.json
cat output/<SOURCE>/orchestrator-run/publish-summary.json
```

Success: `edge_count == processes - 1` for a linear chain, or higher for branched systems; `publish-summary.lifecyclemodel_count >= 1`.

## Read on demand

- `references/conversion-guide.zh-CN.md` - detailed Chinese explanation, formulas, references, and improvement list.
- `references/workflow.md` - stage-by-stage recipe and manual fallback.
- `references/pitfalls.md` - compact troubleshooting table.
- `references/artifacts.md` - output ownership map.

## Fast triage

| Symptom | Fix |
| --- | --- |
| `edge_count: 0` | Reuse the same `flow_key` between upstream output and downstream input. |
| `built_model_count: N` | Driver was bypassed; use `--plan` and the combined run. |
| `run root already exists` | Remove generated dirs: `output/<SOURCE>/{artifacts,flows,runs,manifests,lifecyclemodel-run,orchestrator-run,orchestrator-request.json,uuids.json}`. |
| `relation_count: 0` | Stage 5 did not produce a model; inspect `lifecyclemodel-run/` logs. |
| `derivation=Calculated but no calc_note` | Add `calc_note` or switch to `Estimated`. |
| O2 validation failure | Set `pure_oxygen: true` only if the source says pure O2; otherwise remove O2. |
| `unit=item requires amount=1` | Set every exchange of that item flow to amount `1`. |
| `canonical_flow_key ... not found` | Declare both alias and canonical flows in `plan.flows`. |

## When not to use

- ILCD process datasets already exist -> `lifecyclemodel-automated-builder`.
- Lifecyclemodel exists and needs a resulting process -> `lifecyclemodel-resulting-process-builder`.
- Remote publish -> `lca-publish-executor`.
