# flow profile

Status: **implemented (initial LLM-driven version)**.

## Purpose

Flow-level review for a batch of flow JSON files, with:

- deterministic evidence extraction (name/classification/flow property/quantitative reference)
- optional local reference-context enrichment (via `process-automated-builder` flow property registry) for flowproperty + unitgroup context
- LLM semantic review on structured summaries
- machine-readable `findings.jsonl` for downstream remediation

## Entry

Use the unified entrypoint:

```bash
python scripts/run_review.py --profile flow --flows-dir /path/to/flows --out-dir /path/to/review
```

or:

```bash
python scripts/run_review.py --profile flow --run-root /path/to/run --out-dir /path/to/review
```

When `--flows-dir` is omitted, the script tries:

1. `<run-root>/cache/flows`
2. `<run-root>/exports/flows`

## Main Outputs

- `findings.jsonl` (merged rule-based + LLM findings)
- `rule_findings.jsonl`
- `llm_findings.jsonl`
- `flow_summaries.jsonl`
- `similarity_pairs.jsonl`
- `flow_review_summary.json`
- `flow_review_zh.md`
- `flow_review_en.md`
- `flow_review_timing.md`

## Notes

- Enable `--with-reference-context` to improve flow property/unitgroup rationality evidence (internally uses local `process-automated-builder` registry, not CRUD).
- LLM semantic review is enabled by default when `OPENAI_API_KEY` is set.
- Use `--disable-llm` to force rule-only review.
- Use `--enable-llm` to force LLM review in environments where the key may not be preloaded.
- By default, the profile auto-loads `profiles/flow/references/tidas_flows.yaml` when present.
- Use `--methodology-file /path/to/tidas_flows.yaml` to override; findings will include `rule_source` (default `tidas_flows.yaml`, overridable by `--methodology-id`).
