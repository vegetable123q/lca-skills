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
- For `"black_box": true`, every input/output flow used by that process must declare `unit: "item"`.
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

## When NOT to use

- ILCD process datasets already exist → `lifecyclemodel-automated-builder` directly
- Lifecyclemodel exists, need resulting process → `lifecyclemodel-resulting-process-builder`
- Remote publish → `lca-publish-executor`
