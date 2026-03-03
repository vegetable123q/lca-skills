# ILCD Method Guardrails for Process Automated Builder

> Purpose: provide lightweight, executable method guardrails derived from ILCD LCA guidance for `process_from_flow` automation.
> Scope: only rules that directly affect automated decisions (FU/reference flow, comparability, boundary consistency, and interpretation safety).

## 0) Default Assumption for This Project (Dataset/Database Building)

- Primary intent is long-term dataset/database consistency and downstream comparability.
- Default modelling stance: prefer normalized and comparable bases (e.g., mass/performance) unless a per-item scenario is explicitly required.
- Do not mix per-item and per-mass semantics inside one dataset; split scenarios/datasets when both are required.

## 1) Functional Unit (FU) and Reference Flow Consistency

### Rule G1 — FU must match comparison intent
- If goal is cross-product/component comparability across heterogeneous items, prefer normalized FU (often mass-based, e.g. `1 kg`) or an explicit performance FU.
- Do **not** default to per-item FU when item sizes/masses vary significantly.

### Rule G2 — Reference flow must be quantitatively aligned with FU
- The process quantitative reference and the flow quantitative reference must be in the same basis.
- Reject/flag combinations that imply mixed bases (e.g. process exchanges implicitly per-item while reference output is `1 kg`).

### Rule G3 — Unit/flow-property pairing must be coherent
- Validate that `flowProperty` and reference unit are coherent and intentionally chosen (e.g. `Mass + kg` or `Number of items + item`).
- Treat silent basis switches as blocking issues for publish.

## 2) Comparability Guardrails (for batch/benchmark usage)

### Rule C1 — Comparison must be based on relevant FU
- For comparative outputs, ensure all alternatives are normalized to the same FU definition and assumptions.
- If normalization is not possible with available evidence, mark as non-comparable and require scenario split.

### Rule C2 — Separate scenario types instead of mixing semantics
- If both per-item and per-mass views are needed, produce separate scenarios/datasets; do not mix in one dataset.
- Require explicit comments indicating scenario basis and intended use.

## 3) Process Inventory Integrity

### Rule I1 — Exchange magnitudes must be basis-consistent
- Before publish, run basis consistency check:
  - output reference amount basis
  - major input/output exchange basis
  - balance check basis
- If inconsistent, emit `basis_mismatch` and stop strict publish.

### Rule I2 — Balance warnings are interpretation signals, not auto-proof of correctness
- Large imbalance may be acceptable only with explicit documented rationale (cut-off, by-products, excluded flows, auxiliary handling).
- Without rationale, keep status as `check` and require revision.

## 4) Documentation Minimums (must be auto-populated)

For each generated process/flow intended for publish, include:
- Intended application (comparison/accounting/decision-support context)
- FU statement (quantitative + qualitative)
- Reference flow statement
- Boundary notes (what is included/excluded)
- Basis declaration (`per kg`, `per item`, or performance unit)
- Key assumptions and known limitations

## 5) Operational Integration Points in `process_from_flow`

### At flow/process construction time
- Apply G1/G2/G3 and C1/C2 as preflight checks.

### At balance review stage
- Apply I1/I2 and annotate reports with basis diagnostics.

### At publish gate
- If `basis_mismatch` exists, fail strict publish or require explicit override flag.

## 6) Recommended Output Flags

Add these flags to state/report for downstream automation:
- `fu_basis`: `mass|item|performance|other`
- `comparability_ready`: `true|false`
- `basis_mismatch`: `true|false`
- `normalization_required`: `true|false`
- `publish_blockers`: list of blocking rule ids

## 7) Situation Mapping Hint (ILCD A/B/C)

- For database-building / accounting-style accumulation, treat the work as closer to ILCD Situation C unless a decision-support comparative study explicitly requires Situation A/B assumptions.
- If data may later be used in public comparisons, raise documentation and consistency requirements at build time (do not defer to downstream users).

## 8) Source Note

This guardrail file is a distilled operational subset from ILCD general LCA guidance topics, especially:
- Function, functional unit, and reference flow
- Comparisons between systems and functional unit relevance
- Frequent error: comparisons not based on relevant FU
- Iterative refinement and consistency checks

Use this file as an automation policy layer; consult full ILCD text only when ambiguity remains.
