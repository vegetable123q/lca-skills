# Artifact map

Canonical layout for a run under `output/<SOURCE>/`. Shapes of each file are defined by the producing skill; this doc only names them and what stage made them, so a reader can trace artifacts back to their owners.

```
output/<SOURCE>/
├── flows/                                    [Stage 1, authored here]
│   ├── 01-<layer>.json
│   ├── 02-<layer>.json
│   └── 03-<layer>.json
├── uuids.json                                [Stage 3, authored here]
├── runs/                                     [Stage 2 + Stage 4]
│   ├── 01-<layer>/                           scaffold — owned by process-automated-builder
│   │   ├── request/        pff-request.json, request.normalized.json, source-policy.json
│   │   ├── input/          input_manifest.json, <flow>.json
│   │   ├── manifests/      flow-summary, assembly-plan, lineage-manifest, invocation-index, run-manifest
│   │   ├── cache/          process_from_flow_state.json, agent_handoff_summary.json
│   │   ├── stage_outputs/  01_route … 10_publish (empty / placeholder — scaffold only)
│   │   └── reports/        process-auto-build-report.json
│   ├── 02-<layer>/                           (same shape)
│   ├── 03-<layer>/                           (same shape)
│   └── <SOURCE>-combined/                    [Stage 4, authored here]
│       ├── exports/
│       │   └── processes/  <PROC_UUID>_<version>.json × N   ← hand-authored ILCD datasets
│       ├── cache/          process_from_flow_state.json     ← copied from 01-<layer>/cache/
│       └── manifests/      *.json                            ← copied from 01-<layer>/manifests/
├── manifests/
│   └── lifecyclemodel-manifest.json          [Stage 5, authored here]
├── lifecyclemodel-run/                       [Stage 5] — owned by lifecyclemodel-automated-builder
│   ├── request/            lifecyclemodel-auto-build.request.json, request.normalized.json
│   ├── run-plan.json
│   ├── resolved-manifest.json
│   ├── selection/          selection-brief.md
│   ├── discovery/          reference-model-summary.json
│   ├── manifests/          invocation-index.json, run-manifest.json
│   ├── models/<SOURCE>-combined/
│   │   ├── tidas_bundle/lifecyclemodels/<MODEL_UUID>_<version>.json   ← the json_ordered
│   │   ├── summary.json
│   │   ├── connections.json       ← read this to verify edge inference
│   │   └── process-catalog.json
│   └── reports/            lifecyclemodel-auto-build-report.json
├── orchestrator-request.json                 [Stage 6, authored here]
├── orchestrator-run/                         [Stage 6] — owned by lifecyclemodel-recursive-orchestrator
│   ├── request.normalized.json
│   ├── assembly-plan.json
│   ├── graph-manifest.json
│   ├── lineage-manifest.json
│   ├── boundary-report.json
│   ├── invocations/         <per-node invocation logs>
│   ├── publish-bundle.json
│   └── publish-summary.json                   ← local publish-prep success marker
├── publish-request.json                      [Stage 7, authored here] — tiangong publish request
└── publish-run/                              [Stage 7] — owned by tiangong publish run
    ├── request.normalized.json
    ├── publish-report.json
    └── relation-manifest.json                  ← when relation_mode stays local_manifest_only
```

## Quick verification checklist

After Stage 5:
```bash
jq '.local_build_reports[0].summary | {process_count, edge_count, reference_process_uuid, multiplication_factors}' \
   output/<SOURCE>/lifecyclemodel-run/reports/lifecyclemodel-auto-build-report.json
```

After Stage 6:
```bash
cat output/<SOURCE>/orchestrator-run/publish-summary.json
```

After Stage 7:
```bash
cat output/<SOURCE>/publish-run/publish-report.json
```

## What belongs to whom

| Dir / file | Owner skill | Editable by hand? |
| --- | --- | --- |
| `flows/*.json` | this skill | yes (Stage 1) |
| `uuids.json` | this skill | yes, but regenerate via helper |
| `runs/<layer>/` (scaffold) | `process-automated-builder` | no (regenerate via Stage 2) |
| `runs/<SOURCE>-combined/exports/processes/*.json` | this skill | yes (Stage 4) |
| `runs/<SOURCE>-combined/{cache,manifests}` | copied from scaffold | no |
| `lifecyclemodel-run/**` | `lifecyclemodel-automated-builder` | no (regenerate via Stage 5) |
| `orchestrator-request.json` | this skill | yes (Stage 6) |
| `orchestrator-run/**` | `lifecyclemodel-recursive-orchestrator` | no (regenerate via Stage 6) |
| `publish-request.json` | this skill | yes (Stage 7) |
| `publish-run/**` | `tiangong publish run` | no (regenerate via Stage 7) |
