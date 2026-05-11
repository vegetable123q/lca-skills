function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined && entry !== null && entry !== ''),
  );
}

function yearFromDate(value) {
  const match = String(value || '').match(/\b(19|20)\d{2}\b/u);
  return match?.[0] || '';
}

const knownSourceKeys = new Set([
  'type',
  'id',
  'title',
  'assignee',
  'inventor',
  'priority_date',
  'publication_date',
  'grant_date',
  'year',
  'publication_year',
]);

export function buildPatentSourceMetadata(plan) {
  const source = plan?.source || {};
  const extraMetadata = compactObject(
    Object.fromEntries(
      Object.entries(source).filter(([key]) => !knownSourceKeys.has(key)),
    ),
  );
  const year =
    source.year ||
    source.publication_year ||
    yearFromDate(source.publication_date) ||
    yearFromDate(source.priority_date) ||
    yearFromDate(source.grant_date) ||
    plan?.reference_year ||
    '';

  return compactObject({
    source_type: source.type || 'patent',
    source_id: source.id || '',
    title: source.title || '',
    assignee: source.assignee || '',
    inventor: source.inventor || '',
    priority_date: source.priority_date || '',
    publication_date: source.publication_date || '',
    grant_date: source.grant_date || '',
    year: year ? String(year) : '',
    reference_year: plan?.reference_year ? String(plan.reference_year) : '',
    extra_metadata: Object.keys(extraMetadata).length ? extraMetadata : undefined,
  });
}

function sourceDecisionFactor(sourceMetadata) {
  const parts = [
    sourceMetadata.source_id,
    sourceMetadata.assignee,
    sourceMetadata.year,
  ].filter(Boolean);
  if (!parts.length) return 'Patent source metadata is not available in the plan.';
  return `Patent source: ${parts.join(' / ')}`;
}

export function buildPatentLifecyclemodelManifest(plan, combinedDir) {
  const source = buildPatentSourceMetadata(plan);

  return {
    run_label: `${plan?.source?.id || 'source'}-lifecyclemodel`,
    allow_remote_write: false,
    basic_info: {
      name: plan?.goal?.name || plan?.source?.title || plan?.source?.id || '',
      functional_unit: plan?.goal?.functional_unit || { amount: 1, unit: 'kg' },
      boundary: plan?.goal?.boundary || 'cradle-to-gate',
      geography: plan?.geography || '',
      reference_year: plan?.reference_year ? String(plan.reference_year) : '',
      source,
    },
    selection: {
      mode: 'graph_first_local_inference',
      max_models: 1,
      max_processes_per_model: 12,
      decision_factors: [sourceDecisionFactor(source)],
    },
    output: { write_local_models: true, emit_validation_report: true },
    local_runs: [combinedDir],
  };
}
