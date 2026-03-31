# Source Analysis

## TianGong App Behavior

Reference repo: `tiangong-lca-next`

- `src/services/lifeCycleModels/util.ts`
  - `genLifeCycleModelJsonOrdered` converts graph nodes and edges into native lifecycle model data.
  - Core native fields are:
    - `lifeCycleModelInformation.quantitativeReference.referenceToReferenceProcess`
    - `technology.processes.processInstance`
    - `connections.outputExchange.downstreamProcess`
- `src/services/lifeCycleModels/api.ts`
  - The application stores extra platform fields such as `json_tg` and `rule_verification`.
  - Those fields are outside this skill's scope after the current redesign.

## TIDAS SDK

Reference repo: `tidas-sdk`

- `createLifeCycleModel(data?, config?)` provides strict validation.
- The SDK is the native schema gate for the `json_ordered` artifact this skill emits.

## TIDAS Tools

Reference repo: `tidas-tools`

- Historical upstream note: `tidas-tools` contains a `validate.py` implementation for lifecycle model classification hierarchy checks.
- Current skill execution must not route callers back into that Python entrypoint. Keep the active path on CLI / SDK-backed validation, and treat any `validate.py` mention here as background context only.
- Classification still has to pass even if the JSON passes strict `tidas-sdk` validation.

## Downstream Publish Boundary

Implication:

- this skill should stop at native `json_ordered`
- if a remote write is approved later, the downstream publish layer is the correct place to derive any platform-specific fields such as `json_tg`
