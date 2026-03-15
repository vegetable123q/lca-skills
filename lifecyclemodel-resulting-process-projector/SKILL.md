---
name: lifecyclemodel-resulting-process-projector
description: Project a lifecycle model into reviewable local resulting-process payloads plus lifecyclemodel/resulting-process relation metadata. Use when a lifecycle model already exists and you need a local projection bundle, relation contract, and publish handoff artifacts without routing through `process-automated-builder`.
---

# Lifecycle Model Resulting Process Projector

Use this skill when the source of truth is already a lifecycle model `json_ordered` file and the next step is to prepare resulting-process artifacts, not to synthesize a process from external flow evidence.

## What The Implementation Does

- validates `assets/request.schema.json`
- loads a lifecycle model from `source_model.json_ordered` or `source_model.json_ordered_path`
- extracts process instances and graph edges from model topology
- derives:
  - `process-projection-bundle.json`
  - `projection-report.json`
  - `publish-bundle.json` via `publish`
- emits relation payloads containing:
  - `generated_from_lifecyclemodel_id`
  - `generated_from_lifecyclemodel_version`
  - `projection_role`
  - `projection_signature`
- supports `primary-only` and `all-subproducts`
- keeps all work local; no remote write path is executed here

## Inputs

Required top-level fields:

- `source_model`
- `projection`
- `publish`

The source model may be provided as:

- `source_model.id`
- `source_model.json_ordered_path`
- `source_model.json_ordered`

## Outputs

- `request.normalized.json`
- `source-model.normalized.json`
- `source-model.summary.json`
- `projection-report.json`
- `process-projection-bundle.json`
- `publish-bundle.json` from `publish`

## Commands

```bash
python3 scripts/lifecyclemodel_resulting_process_projector.py project \
  --request assets/example-request.json \
  --out-dir /abs/path/run-001

python3 scripts/lifecyclemodel_resulting_process_projector.py project \
  --model-file assets/example-model.json \
  --projection-role all \
  --out-dir /abs/path/run-001

python3 scripts/lifecyclemodel_resulting_process_projector.py publish \
  --run-dir /abs/path/run-001 \
  --publish-processes \
  --publish-relations
```

## Separation Rule

- use `process-automated-builder` for flow-to-process synthesis
- use this skill for lifecyclemodel-to-resulting-process projection

They are different pipelines and should stay separate.

## Load References On Demand

- `references/projection-workflow.md`: intake to publish-handoff stages
- `references/projection-contract.md`: bundle shape and relation semantics
- `references/projector-invocation-contract.md`: caller/callee contract
- `references/integration-notes.md`: current cross-project architecture notes
