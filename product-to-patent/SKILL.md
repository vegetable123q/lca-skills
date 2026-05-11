---
name: product-to-patent
description: Find, download, and triage Google Patents candidates from a product or material description, then deduplicate patent families and prepare reviewed patent links for downstream patent-to-lifecyclemodel conversion. Use when starting from a product such as an NCM811 cathode, battery material, process target, or commercial/technical product and needing candidate patents, Google Patents metadata, family-aware source selection, or source handoff into patent-to-lifecyclemodel.
---

# Product -> Patent

Thin metadata-first workflow for turning a product description into reviewed patent source candidates.
Use this before `$patent-to-lifecyclemodel` when the patent source is not known yet.

## Workflow

1. Read `references/google-patents-workflow.zh-CN.md`.
2. Convert the product into 3-6 search queries: exact product names, aliases, chemistry/formula variants, function words, and process words.
3. Run the metadata helper for each query:

```bash
node product-to-patent/scripts/google-patents-metadata.mjs \
  --query '"NCM811" cathode "preparation method"' \
  --max-results 30 \
  --out-dir output/product-to-patent/ncm811-cathode/q1 \
  --json
```

The default `--fetcher auto` path uses Jina Reader first, avoiding local direct requests to `patents.google.com`; if Google still blocks the search endpoint, NCM811 queries fall back to curated Google Patents seed publications from `assets/ncm811-query-plan.json` and then enrich each public patent page.

Use an existing Google Patents search URL when a human has already tuned the query:

```bash
node product-to-patent/scripts/google-patents-metadata.mjs \
  --url 'https://patents.google.com/?q=%22NCM811%22+cathode&dups=language' \
  --max-results 30 \
  --out-dir output/product-to-patent/ncm811-cathode/from-url
```

4. Download full text for all identified publications with rate limiting and multiple strategies:

```bash
node product-to-patent/scripts/google-patents-download-fulltext.mjs \
  --metadata-file output/product-to-patent/ncm811-cathode/q1/google-patents-metadata.json \
  --out-dir output/raw \
  --delay 25 \
  --download-images \
  --image-mode flow \
  --skip-existing
```

Or use the seed plan directly:

```bash
node product-to-patent/scripts/google-patents-download-fulltext.mjs \
  --seed-plan product-to-patent/assets/ncm811-query-plan.json \
  --out-dir output/raw \
  --delay 25
```

The downloader tries four text strategies per publication (Jina Reader en, Jina Reader native language, direct fetch, Jina XHR), then PDF download. With `--download-images --image-mode flow`, it also extracts Google PatentImages figure URLs with process-flow, preparation-process, manufacturing-process, schematic, or Chinese flow-chart signals and stores them under `<out-dir>/<PUBNUM>-images/`; when Jina exposes both a 120 px thumbnail and a later full-size inline figure, the downloader prefers the full-size figure. Use `--image-mode all` only when manual review needs every patent drawing. It adds configurable delays between requests to avoid rate limiting. Output goes to one `.md`/`.html` file per patent plus `download-summary.json`.

5. Review the downloaded full-text files in `output/raw/` and `download-summary.json`.
6. Group candidates by patent family signals before ranking them. Treat family members as one source family unless they add materially different disclosure.
7. Pick patents that disclose process recipe data: masses, molar ratios, precursor concentrations, pH, drying/calcination temperatures, atmosphere, residence times, yields, and product composition.
8. For NCM811 cathode manufacturing patents, keep process-flow candidate figures when available; they can reveal unit-operation order, recycle loops, or coating/calcination routing that text extraction may flatten. Do not treat generic SEM photos, battery structure drawings, or decorative page images as lifecyclemodel-ready process evidence unless captions or surrounding text connect them to the manufacturing route.
9. Write a reviewed candidate file using `assets/reviewed-candidates.template.json`, then pass the selected source patent into `$patent-to-lifecyclemodel`.

## NCM811 Batch Pipeline

For large-scale NCM811 collection (800+ patents), use the batch pipeline:

```bash
node product-to-patent/scripts/google-patents-batch-pipeline.mjs \
  --out-dir output/raw \
  --target-count 800 \
  --max-depth 2 \
  --download-delay 6 \
  --download-images \
  --image-mode flow
```

The pipeline runs NCM811 seed and citation-chain discovery, deduplicates, then downloads full text for each unique publication via Jina Reader. When image download is enabled, process-flow candidate figures are saved under `<out-dir>/<PUBNUM>-images/` and indexed in `download-summary.json`. Output is one `.txt` file per patent (matching `data/` directory format) plus `download-summary.json` with per-patent metadata (title, assignee, dates, CPC codes, abstract).

To run discovery only (no download):

```bash
node product-to-patent/scripts/google-patents-batch-pipeline.mjs --no-download --target-count 800
```

To resume a previous run:

```bash
node product-to-patent/scripts/google-patents-batch-pipeline.mjs --no-discover
```

## NCM811 Starter

Use `assets/ncm811-query-plan.json` as the starting plan for NCM811 cathode searches. The first pass should include exact and alias queries:

```text
"NCM811" cathode "preparation method"
("NCM811" OR "NMC811") ("positive electrode" OR cathode) coating
"LiNi0.8Co0.1Mn0.1O2" "preparation method"
```

Expand only after reviewing the first metadata export. Add CPC terms such as `H01M4/525` only when Google Patents result groups or reviewed candidates confirm the classification is useful for the target chemistry.

## Product -> Patent -> Lifecyclemodel Loop

For a complete query-driven handoff, run the combined workflow script:

```bash
node product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs \
  --query '"NCM811" cathode "preparation method"' \
  --product-name "NCM811 cathode active material" \
  --max-results 10 \
  --out-dir output/product-to-patent-lifecyclemodel/ncm811-cathode \
  --download-images \
  --image-mode flow \
  --skip-existing
```

The workflow performs the Google Patents metadata search, saves `metadata/google-patents-metadata.json`, downloads each patent page/PDF candidate into `raw/`, extracts process-flow candidate figures when requested, and writes a structured `workflow-manifest.json`. Each patent also gets:

- `patents/<PUBLICATION>/source-metadata.json`: normalized patent source metadata, including company/assignee, priority/filing/publication/grant dates, links, family/citation signals, and download status.
- `patents/<PUBLICATION>/plan-source.json`: a compact handoff for authoring `patents/<PUBLICATION>/lifecyclemodel/plan.json`.
- `patents/<PUBLICATION>/lifecyclemodel/`: the base directory passed to `$patent-to-lifecyclemodel`.

After authoring a plan from the downloaded full text, rerun with `--run-lifecyclemodels` to execute every patent directory that already has `lifecyclemodel/plan.json`:

```bash
node product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs \
  --query '"NCM811" cathode "preparation method"' \
  --product-name "NCM811 cathode active material" \
  --out-dir output/product-to-patent-lifecyclemodel/ncm811-cathode \
  --skip-existing \
  --run-lifecyclemodels
```

When converting, copy the `source` object from `plan-source.json` into `plan.json`. The `$patent-to-lifecyclemodel` metadata interface preserves that source object into lifecyclemodel basic info, so company, publication year, patent URL, PDF URL, and family metadata remain attached to the generated model.

## Family Review Rules

- Do not count application and grant versions as separate technical sources by default.
- Prefer the family member with the clearest full text, examples, claims, legal status, and machine translation.
- Compare `detail.family_members`, same priority/application dates, `Other versions`, citations, and title/assignee overlap.
- Keep all family links in the reviewed file, but mark one `representative_publication_number`.
- If two family members contain different examples or jurisdiction-specific claim language useful for lifecycle modeling, keep both and explain why.

## Output Contract

For every selected source family, record:

- product target and query that found it
- representative publication number and Google Patents URL
- all observed family members and useful alternate links
- relevance rationale tied to product chemistry and process disclosure
- extraction readiness: `ready_for_lifecyclemodel`, `needs_manual_pdf_review`, or `reject`
- notes on missing process data, family ambiguity, and legal-status uncertainty

## Boundaries

- This skill uses Google Patents public metadata as triage support. It does not make legal-status claims.
- Do not add TianGong business logic, private runtimes, MCP transports, or a long-lived crawler service here.
- If the helper needs advanced crawling, pagination beyond the public metadata helper, proxying, or Scrapling-style adaptive crawling, add a native capability in `tiangong-lca-cli` first and keep this skill as the wrapper.
- Scrapling-style fetching was considered for local direct crawling, but this skill must not depend on bypassing Google from the local IP; use the default Jina/seed fallback path for reliable runs.
- Respect Google Patents rate limits and terms; keep runs small and auditable.

## Verify

```bash
node --test test/product-to-patent-google-patents.test.mjs
node --test test/product-to-patent-lifecyclemodel-workflow.test.mjs
node product-to-patent/scripts/google-patents-metadata.mjs --help
node product-to-patent/scripts/google-patents-download-fulltext.mjs --help
node product-to-patent/scripts/google-patents-batch-pipeline.mjs --help
node product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs --help
node scripts/validate-skills.mjs product-to-patent
```

## Resources

- `scripts/google-patents-metadata.mjs`: download search metadata and per-result detail signals from Google Patents.
- `scripts/google-patents-download-fulltext.mjs`: download full text for identified publications with rate limiting and multiple fetch strategies.
- `scripts/google-patents-batch-pipeline.mjs`: batch pipeline for large-scale discovery + download (800+ patents).
- `scripts/product-patent-lifecyclemodel-workflow.mjs`: query-driven metadata, patent download, source handoff, and optional authored-plan lifecyclemodel execution loop.
- `references/google-patents-workflow.zh-CN.md`: query, metadata, crawler, and family-review guidance.
- `assets/ncm811-query-plan.json`: example product-to-query plan for NCM811 cathode.
- `assets/reviewed-candidates.template.json`: reviewed handoff format for downstream conversion.
