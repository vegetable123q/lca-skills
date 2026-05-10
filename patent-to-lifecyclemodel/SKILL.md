---
name: patent-to-lifecyclemodel
description: Convert a patent or SOP into a TianGong TIDAS lifecyclemodel by authoring one plan, generating process datasets, building the lifecyclemodel, and optionally publishing through the unified tiangong CLI.
---

# Patent -> Lifecyclemodel

Thin wrapper. Author `output/<SOURCE>/plan.json`; the driver delegates build and publish work to existing CLI-backed skills.

## Run

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --plan output/<SOURCE>/plan.json \
  --base output/<SOURCE> \
  --all --json
```

Publish only when explicitly requested:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --base output/<SOURCE> \
  --publish-only --commit --json
```

Materialization requires a database flow scope by default. With Supabase read env present, it delegates to `tiangong flow list --state-code 0 --state-code 100 --all` and writes `output/<SOURCE>/flow-scope.json`; pass `--flow-scope-file` to use a reviewed frozen scope. If neither is available, the run fails instead of silently creating all flows locally. Use `--no-remote-flow-scope` only for explicit offline tests.
Stage 7 first publishes generated flow datasets through `tiangong flow publish-reviewed-data`, then writes `publish-request.json` and calls `tiangong publish run`; do not add remote-write logic inside this skill.
Reruns preserve `uuids.json` when present; keep that file when correcting previously published data so remote rows can be overwritten with stable IDs.

## Plan Rules

- Split the patent route into one process per defensible unit operation.
- Reuse the same `flow_key` for an upstream output and downstream input; this creates lifecyclemodel edges.
- Resolve flows against remote database scope first. Raw materials, elementary flows, utilities, electricity, water, oxygen, fuels, waste, and common reagents must prefer existing database flows; patent-specific intermediates, coated/composite products, and final results may be generated when unresolved.
- Use `name_en`, `name_zh`, and `aliases`/`match_names` to expose database-matchable names such as generic reagent names or grid electricity names; unique exact matches across those names are reused and only unresolved patent-specific flows are generated.
- If search returns multiple defensible candidates, or only normalized candidate matches such as hydrate-to-anhydrous name variants, inspect `flow-resolution.json.review` and add an audited `existing_flow_ref` plus any required `canonical_flow_key`/`conversion_factor` instead of creating a duplicate.
- Use normal physical units such as `kg`, `L`, `mol`, `kWh`, and `m3`; reserve `item` for unavoidable black-box processes.
- `Measured` means directly stated. `Calculated` means source-derived; add `calc_note`. `Estimated` means source missing; add `formula_ref` or `source_ref`.
- Use patent masses, volumes, concentrations, ratios, yields, residence times, temperatures, and flow rates before estimating.
- Use `scripts/estimate-utilities.mjs` for estimated electricity, water, O2, waste, and emissions.
- Set `pure_oxygen: true` only when the source explicitly names pure O2.
- Default to `black_box: false`. Use `black_box: true` only when critical material, product, or operation data are still missing after measured/calculated/estimated modeling.
- Never mark a whole patent route black-box because some exchanges are missing. Split the route and black-box only the specific step with the critical gap.
- If black-box is unavoidable, every flow used by that process must have `unit: "item"`, every exchange amount must be `1`, and `comment` must name the missing critical data.
- Hydrate and valence name variants are only surfaced as candidates; the resolver does not hard-code reagent-specific chemistry or conversion factors. Declare audited conversions with `canonical_flow_key` and `conversion_factor`.
- Keep coated, doped, and composite products as composites; do not collapse them into pure phases.

## Minimum Plan

```jsonc
{
  "source": { "id": "<PATENT-ID>", "title": "...", "assignee": "..." },
  "goal": { "name": "...", "functional_unit": {"amount": 1, "unit": "kg"}, "boundary": "..." },
  "geography": "CN",
  "reference_year": "2019",
  "flows": {
    "<flow_key>": {
      "name_en": "...",
      "name_zh": "...",
      "unit": "kg",
      "existing_flow_ref": {
        "id": "<DB-FLOW-UUID>",
        "version": "01.00.000",
        "name": "...",
        "unit": "kg"
      }
    }
  },
  "processes": [{
    "key": "<proc_key>",
    "step_id": "S1",
    "name_en": "...",
    "classification": ["..."],
    "technology": "...",
    "black_box": false,
    "pure_oxygen": false,
    "reference_output_flow": "<product_flow_key>",
    "inputs": [{ "flow": "<flow_key>", "amount": 0, "derivation": "Measured|Calculated|Estimated" }],
    "outputs": [{ "flow": "<flow_key>", "amount": 1, "derivation": "Measured" }]
  }]
}
```

## Verify

```bash
jq '{process_count, edge_count}' \
  output/<SOURCE>/lifecyclemodel-run/models/<SOURCE>-combined/summary.json
cat output/<SOURCE>/orchestrator-run/publish-summary.json
cat output/<SOURCE>/publish-run/publish-report.json
```

Expected: process count matches the plan, edges connect shared flows, no publish failures, and no black-box process unless the plan documents a critical data gap.
Also verify `flow-scope.json` was generated from the remote database or intentionally supplied, `flow-resolution.json` reuses database flows where possible, the flow publish report only prepares or commits unresolved generated flows, the lifecyclemodel `json_tg.submodels` has one `primary` reference process, and process exchange references scan as `exists_in_target`.
For a clean rerun, remove generated run directories but keep `plan.json` and `uuids.json`.

## References

- `references/conversion-guide.zh-CN.md`
- `references/workflow.md`
- `references/pitfalls.md`
- `references/artifacts.md`
