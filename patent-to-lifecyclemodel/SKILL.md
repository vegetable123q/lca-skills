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

The driver runs `tiangong-lca lifecyclemodel validate-build --run-dir output/<SOURCE>/lifecyclemodel-run --engine sdk --json` after Stage 5 by default. Treat that validation report as the local gate for the remote web "数据校验" function: do not continue to orchestration or publish when the report is not OK. Use `--skip-validation` only for debugging incomplete local artifacts, never for accepted output.

Publish only when explicitly requested:

```bash
node patent-to-lifecyclemodel/scripts/run-patent-to-lifecyclemodel.mjs \
  --base output/<SOURCE> \
  --publish-only --commit --json
```

Materialization requires remote database flow resolution. The wrapper first checks the minimal direct-reuse cache at `output/patent-to-lifecyclemodel-used-flows.json`; that cache may only contain database flow `id`, `version`, `name`, `unit`, and exact reusable names from previous successful resolutions. A cache hit is valid only for exact name + exact unit and only when it maps to one database flow. If any plan flow is not directly covered by `existing_flow_ref` or this exact cache, the wrapper loads remote flow credentials from the current environment or the adjacent TianGong CLI checkout's `.env` (`tiangong-cli/.env` or `TIANGONG_LCA_CLI_DIR/.env`), delegates to `tiangong flow list --state-code 0 --state-code 100 --all --page-size 1000 --json`, and writes one repo-level scope file at `output/patent-to-lifecyclemodel-flow-scope.json`. Reuse that repo-level scope for later examples; do not write full remote scope rows into each `output/<SOURCE>/`. Do not pass `--no-remote-flow-scope` or `--flow-scope-file`; both are forbidden.
Stage 7 first publishes generated flow datasets through `tiangong flow publish-reviewed-data`, then writes `publish-request.json` and calls `tiangong publish run`; do not add remote-write logic inside this skill.
Reruns preserve `uuids.json` when present; keep that file when correcting previously published data so remote rows can be overwritten with stable IDs.

## Plan Rules

- Inputs may include a patent metadata CSV row such as `data/combined_patents_20250825_true.csv` plus the patent full text. Use the CSV/meta row to populate bibliographic fields, dedupe/filter signals, source URLs, family/citation metadata, assignee, dates, jurisdiction, title, abstract, CPC/IPC, and product/query context. Use the patent text to populate process route, technical parameters, material quantities, examples, claims-derived route constraints, and LCI source quotes. Do not let CSV metadata replace the technical extraction from the full patent text.
- Split the patent route into one process per defensible unit operation.
- Fill plan-level fields before materialization: `source.id`, `source.title`, `source.assignee` when available, best available patent dates/year, `goal.name`, `goal.functional_unit`, `goal.boundary`, `geography`, `reference_year`, and every referenced `flows[flow_key].name_en`, `name_zh` when available, and `unit`.
- Preserve CSV/meta columns that do not map to first-class fields under `source.extra_metadata` using stable snake_case keys; code propagates source metadata into lifecyclemodel manifest/publish payloads.
- Treat every `processes[]` entry as a reviewable TianGong process. Fill all required process fields before running the driver: `key`, `step_id`, `name_en`, `name_zh` when available, `classification`, `scale`, `technology`, `comment`, `black_box`, `pure_oxygen`, `reference_output_flow`, non-empty `inputs[]`, and non-empty `outputs[]`.
- The process `technology` field is the source for the UI "处理、标准、路线" information through `processInformation.technology.technologyDescriptionAndIncludedProcesses`. It must not be generic. Put the LCI source text that supports the inventory and technical parameters there, including route sequence, equipment or operation, temperatures, times, concentrations, ratios, pH, pressure, atmosphere, flow rates, yields, washing/drying/calcination conditions, and any standard or engineering basis used. Quote or closely paraphrase the patent/SOP parameter text and name the source section/example when possible.
- Use `comment` for the audit summary: patent/SOP source, what is measured/calculated/estimated, missing-data limitations, and why the process remains non-black-box or becomes black-box.
- Code fills deterministic scaffolding only: missing `step_id`, `name_en`, `scale`, `comment`, referenced flow `name_en`, and exchange `source_ref` defaults. AI must still author real `technology`, flow names, amounts, derivations, calculation notes, source quotes, and estimator parameters from the CSV metadata and patent text.
- Reuse the same `flow_key` for an upstream output and downstream input; this creates lifecyclemodel edges.
- Resolve flows against remote database scope first. Raw materials, elementary flows, utilities, electricity, water, oxygen, fuels, waste, and common reagents must prefer existing database flows; patent-specific intermediates, coated/composite products, and final results may be generated when unresolved.
- Process inputs must prefer product flows. Only explicit gas inputs such as oxygen, nitrogen, argon, hydrogen, air, or CO2 may use elementary/basic flows. Never use emission or elementary flow categories such as `Emissions > Emissions to soil > Emissions to non-agricultural soil` on the input side.
- Reuse `output/patent-to-lifecyclemodel-used-flows.json` only for exact repeat substances that were already resolved to one existing database flow with the same unit. Do not add fuzzy names, candidate matches, converted hydrate/anhydrous variants, generated flows, or full database rows to this cache.
- Use `name_en`, `name_zh`, and `aliases`/`match_names` to expose database-matchable names such as generic reagent names or grid electricity names; unique exact matches across those names are reused and only unresolved patent-specific flows are generated.
- If search returns multiple candidates, resolve automatically using the resolver order: exact name before normalized name, higher state code, latest version, latest modified time, then stable id order. Do not leave flow candidates for manual review.
- A flow produced by one patent process and consumed by another process in the same plan is a patent-specific internal intermediate; keep it generated even if it also appears as a downstream input. Only `existing_flow_ref` may override this.
- Input-only raw materials, reagents, utilities, fuels, and common consumables must prefer the nearest database flow when exact matching fails; use compatible units and converter factors where available. Nearest matching must still be substance-specific: tokenize `name_en`, `name_zh`, `aliases`, and database names after removing process-only modifiers, normalize common metal symbols such as `Ni`, `Co`, and `Mn` and Chinese metal names such as `金属镍`/`金属钴`/`金属锰` to their element names, reject a bare generic `Metal` hit when the query names a specific metal element, reject slag/residue/remediation/waste-like product rows for metal feedstock queries, and prefer product-flow candidates that share the specific element or salt/oxide family before applying state code, version, modified time, and id ordering.
- Simple salt nearest matches must preserve the counterion/cation. Do not reuse a chloride, sulfate, nitrate, hydroxide, carbonate, or fluoride row solely because it shares the anion when the query and candidate name different counterions, such as sodium chloride resolving to lithium chloride. Keep unresolved rows generated unless an audited `existing_flow_ref` or exact compatible database flow exists.
- Patent-specific complex salts, solid electrolytes, dopants, coated/composite products, and final products must not collapse to broad database rows such as `Zirconium-based compound` just because they share one element token.
- Generated flow publish rows must be limited to unresolved flows actually referenced by process inputs or outputs; do not publish unused plan-declared placeholder flows.
- Use normal physical units such as `kg`, `L`, `mol`, `kWh`, and `m3`; reserve `item` for unavoidable black-box processes.
- `Measured` means directly stated. `Calculated` means source-derived; add `calc_note`. `Estimated` means source missing; add `formula_ref` or `source_ref`.
- For every exchange, fill `flow`, `amount`, and `derivation`; for `Calculated`, fill `calc_note`; for `Estimated`, fill `formula_ref` and `source_ref` whenever an estimator or auxiliary source is used; for source-supported `Measured`/`Calculated` values, fill `source_ref` or `source_quote` with the patent/SOP location or short source phrase.
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
  "source": {
    "id": "<PATENT-ID>",
    "title": "...",
    "assignee": "<company / patent owner>",
    "priority_date": "<YYYY-MM-DD if available>",
    "publication_date": "<YYYY-MM-DD if available>",
    "grant_date": "<YYYY-MM-DD if available>",
    "year": "<preferred patent year if needed>"
  },
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
    "name_zh": "...",
    "classification": ["..."],
    "scale": "lab|pilot|industrial",
    "technology": "<LCI source text for the UI 处理、标准、路线 field: route, operation, equipment, conditions, technical parameters, standards/basis>",
    "comment": "<audit summary: source, measured/calculated/estimated basis, missing data limits>",
    "black_box": false,
    "pure_oxygen": false,
    "reference_output_flow": "<product_flow_key>",
    "inputs": [{
      "flow": "<flow_key>",
      "amount": 0,
      "derivation": "Measured|Calculated|Estimated",
      "calc_note": "<required when Calculated>",
      "formula_ref": "<required when Estimated from estimator/formula>",
      "source_ref": "<patent/SOP section, example, table, or auxiliary source>",
      "source_quote": "<short source phrase for technical parameters when useful>",
      "comment": "<exchange audit note>"
    }],
    "outputs": [{
      "flow": "<flow_key>",
      "amount": 1,
      "derivation": "Measured|Calculated|Estimated",
      "source_ref": "<patent/SOP section, example, table, or auxiliary source>"
    }]
  }]
}
```

Preserve patent source metadata in `source` whenever it is available. Company/assignee and the best patent year are copied into `manifests/lifecyclemodel-manifest.json.basic_info.source`, and additional source metadata such as patent URLs or family members is retained under `basic_info.source.extra_metadata`, so the downstream lifecyclemodel builder and publish manifests can distinguish otherwise similar patent-derived models.

When `$product-to-patent` is used first, read `patents/<PUBLICATION>/plan-source.json` from the combined workflow output and copy its `source` object directly into `plan.source`. That object is produced by `buildPatentSourceFromGooglePatentsResult`, the dedicated metadata handoff interface for Google Patents rows. It normalizes publication number, title, company/assignee, inventor, priority/filing/publication/grant dates, best year, Google Patents URL, PDF URL, family members, citation signals, source query, and product name before lifecyclemodel materialization.

## Verify

```bash
jq '{process_count, edge_count}' \
  output/<SOURCE>/lifecyclemodel-run/models/<SOURCE>-combined/summary.json
cat output/<SOURCE>/orchestrator-run/publish-summary.json
cat output/<SOURCE>/publish-run/publish-report.json
```

Expected: process count matches the plan, edges connect shared flows, no publish failures, and no black-box process unless the plan documents a critical data gap.
Also verify `output/<SOURCE>/lifecyclemodel-run/reports/lifecyclemodel-validate-build-report.json` exists and has `ok: true`; this is the acceptance standard for the remote web "数据校验" expectation. Verify `output/patent-to-lifecyclemodel-flow-scope.json` was generated from the remote database through the TianGong CLI unless every plan flow was directly covered by `existing_flow_ref` or the exact used-flow cache, `flow-resolution.json` reuses database flows automatically where candidates exist, inputs do not resolve to emission-side elementary flows, simple salt nearest matches do not change the cation/counterion, broad compound rows are not reused for patent-specific complex salts or solid electrolytes, every exported process has non-empty name, classification, reference year, geography, quantitative reference, technology, comments, inputs, outputs, and exchange derivation/source notes, each process `technologyDescriptionAndIncludedProcesses` contains the LCI source text and technical parameters intended for the UI "处理、标准、路线" field, the flow publish report only prepares or commits unresolved generated flows used by process exchanges and has `failure_count: 0`, lifecyclemodel `json_tg.xflow.nodes[*].data.label` is a TIDAS localized name object rather than a plain string, each node has unique `ports.items` for process inputs and outputs, lifecyclemodel edges connect the intermediate flow with `source.port = OUTPUT:<flowUUID>` and `target.port = INPUT:<flowUUID>` when available, the native TIDAS `lifeCycleModelInformation.quantitativeReference.referenceToReferenceProcess` points to the final-product process instance and is projected to that node's existing `data.quantitativeReference = "1"` UI field, and process exchange references scan as `exists_in_target`.
If an existing `output/<SOURCE>/` run or database row was already generated with incomplete process fields, revise `output/<SOURCE>/plan.json`, keep `output/<SOURCE>/uuids.json`, rerun the driver from the corrected plan so generated process datasets and lifecyclemodel payloads are rebuilt, then run publish with `--publish-only --commit` only after both flow and lifecyclemodel publish reports have zero failures. Stable UUIDs let the corrected rows overwrite the prior incomplete data instead of creating duplicates.
For a clean rerun, remove generated run directories but keep `plan.json` and `uuids.json`.

```bash
node --test test/patent-to-lifecyclemodel-source-metadata.test.mjs
node --test test/product-to-patent-lifecyclemodel-workflow.test.mjs
```

## References

- `references/conversion-guide.zh-CN.md`
- `references/workflow.md`
- `references/pitfalls.md`
- `references/artifacts.md`
