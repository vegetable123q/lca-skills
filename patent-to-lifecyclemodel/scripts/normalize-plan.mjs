#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const argv = process.argv.slice(2);

function has(flag) {
  return argv.includes(flag);
}

function arg(flag, fallback = null) {
  const index = argv.indexOf(flag);
  return index === -1 ? fallback : (argv[index + 1] ?? fallback);
}

function printHelp() {
  console.log(`Usage:
  node patent-to-lifecyclemodel/scripts/normalize-plan.mjs --plan <plan.json> [--write] [--json]

Examples:
  node patent-to-lifecyclemodel/scripts/normalize-plan.mjs --plan output/CN111725499B/plan.json --write --json
  node patent-to-lifecyclemodel/scripts/normalize-plan.mjs --plan patent-to-lifecyclemodel/assets/example-black-box-plan.json --json
`.trim());
}

if (has('--help') || has('-h')) {
  printHelp();
  process.exit(0);
}

const planPathArg = arg('--plan');
const write = has('--write');
const jsonMode = has('--json');

if (!planPathArg) {
  console.error('normalize-plan: --plan is required');
  process.exit(2);
}

const planPath = path.resolve(process.cwd(), planPathArg);
const plan = JSON.parse(fs.readFileSync(planPath, 'utf8'));

function fail(message) {
  console.error(`normalize-plan: ${message}`);
  process.exit(2);
}

function normalizeDerivation(value, fallback) {
  const normalized = String(value || fallback || '').trim();
  if (normalized === 'Measured' || normalized === 'Estimated' || normalized === 'Calculated') {
    return normalized;
  }
  return fallback;
}

function normalizeText(value) {
  return String(value || '')
    .replace(/[^0-9a-zA-Z\u4e00-\u9fff]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim()
    .toLowerCase();
}

const INPUT_GAS_WORDS = new Set([
  'air',
  'argon',
  'co2',
  'dioxide',
  'gas',
  'gaseous',
  'hydrogen',
  'nitrogen',
  'oxygen',
]);

function flowLooksLikeGas(flowKey, flow) {
  const haystack = normalizeText(
    [
      flowKey,
      flow?.name_en,
      flow?.name,
      flow?.name_zh,
      ...(Array.isArray(flow?.aliases) ? flow.aliases : []),
      flow?.existing_flow_ref?.name,
    ].join(' '),
  );
  const tokens = haystack.split(' ').filter(Boolean);
  return tokens.some((token) => INPUT_GAS_WORDS.has(token));
}

function flowLooksLikeEmission(flowKey, flow) {
  const haystack = normalizeText(
    [
      flowKey,
      flow?.name_en,
      flow?.name,
      flow?.name_zh,
      flow?.existing_flow_ref?.name,
      flow?.existing_flow_ref?.classification,
      flow?.existing_flow_ref?.category,
    ].join(' '),
  );
  return (
    haystack.includes('emission') ||
    haystack.includes('elementary flow') ||
    haystack.includes('non agricultural soil') ||
    haystack.includes('nonagricultural soil') ||
    haystack.includes('排放') ||
    haystack.includes('非农业土壤')
  );
}

const MISSING_DATA_PREFIX = 'Missing important data:';
const BLACK_BOX_PREFIX = 'Black-box process.';

function prefixOnce(prefix, text) {
  const trimmed = (text || '').trim();
  if (!trimmed) return prefix;
  return trimmed.startsWith(prefix) ? trimmed : `${prefix} ${trimmed}`.trim();
}

if (!plan.source?.id) fail('source.id is required');
if (!Array.isArray(plan.processes) || plan.processes.length === 0) fail('processes[] is required');
if (!plan.flows || typeof plan.flows !== 'object') fail('flows{} is required');

plan.goal ||= {};
plan.goal.functional_unit ||= { amount: 1, unit: 'kg' };
plan.goal.boundary ||= 'cradle-to-gate';
plan.reference_year ||= 'unknown';
plan.geography ||= 'GLO';

const referencedFlowKeys = new Set();

for (const proc of plan.processes) {
  proc.black_box = proc.black_box === true;
  proc.pure_oxygen = proc.pure_oxygen === true;
  proc.inputs = Array.isArray(proc.inputs) ? proc.inputs : [];
  proc.outputs = Array.isArray(proc.outputs) ? proc.outputs : [];
  proc.classification = Array.isArray(proc.classification) && proc.classification.length
    ? proc.classification
    : ['Chemicals and chemical products'];
  proc.comment ||= '';
  proc.technology ||= '';
  proc.scale ||= '';

  if (!proc.reference_output_flow) {
    fail(`process ${proc.key || '<unknown>'} is missing reference_output_flow`);
  }

  const validateExchange = (entry, direction) => {
    if (!entry.flow) fail(`process ${proc.key} has an ${direction} without flow`);
    const flow = plan.flows?.[entry.flow];
    if (direction === 'input' && flowLooksLikeEmission(entry.flow, flow) && !flowLooksLikeGas(entry.flow, flow)) {
      fail(
        `process ${proc.key} input flow=${entry.flow} must not use emission or elementary flow references; use a product flow unless the input is a gas`,
      );
    }
    const fallbackDerivation =
      direction === 'output' && entry.flow === proc.reference_output_flow ? 'Measured' : 'Estimated';
    entry.derivation = normalizeDerivation(entry.derivation, fallbackDerivation);
    if (typeof entry.amount !== 'number') entry.amount = Number(entry.amount ?? 0);
    if (entry.derivation === 'Calculated') {
      const note = String(entry.calc_note ?? '').trim();
      if (!note) {
        fail(`process ${proc.key} ${direction} flow=${entry.flow} has derivation=Calculated but no calc_note`);
      }
      entry.calc_note = note;
    }
    referencedFlowKeys.add(entry.flow);
  };

  for (const input of proc.inputs) validateExchange(input, 'input');
  let hasReferenceOutput = false;
  for (const output of proc.outputs) {
    validateExchange(output, 'output');
    if (output.flow === proc.reference_output_flow) hasReferenceOutput = true;
  }

  if (!hasReferenceOutput) {
    fail(`process ${proc.key} has no output matching reference_output_flow=${proc.reference_output_flow}`);
  }

  // Rule: O2 may only appear when pure_oxygen is declared.
  const hasO2Input = proc.inputs.some((entry) => /\bo2\b|oxygen/i.test(entry.flow));
  if (hasO2Input && !proc.pure_oxygen) {
    fail(
      `process ${proc.key} lists an O2 input but pure_oxygen!=true. ` +
      'Either set pure_oxygen:true (only if source specifies pure-O2 atmosphere) or remove the O2 exchange.',
    );
  }

  if (proc.black_box) {
    const processFlowKeys = [
      ...proc.inputs.map((entry) => entry.flow),
      ...proc.outputs.map((entry) => entry.flow),
    ];
    for (const flowKey of processFlowKeys) {
      const flow = plan.flows[flowKey];
      if (!flow) fail(`process ${proc.key} references missing flow ${flowKey}`);
      if (flow.unit !== 'item') {
        fail(`black-box process ${proc.key} requires unit=item for flow ${flowKey}`);
      }
    }
    proc.comment = prefixOnce(BLACK_BOX_PREFIX, proc.comment);
    proc.comment = prefixOnce(MISSING_DATA_PREFIX, proc.comment);
  }
}

// Rule: any flow with unit=item globally enforces amount=1 in every exchange
// that references it, regardless of which process owns that exchange. This
// propagates black-box semantics to downstream consumers.
for (const proc of plan.processes) {
  for (const entry of [...proc.inputs, ...proc.outputs]) {
    const flow = plan.flows[entry.flow];
    if (flow?.unit === 'item' && entry.amount !== 1) {
      fail(
        `process ${proc.key} exchange for flow=${entry.flow} has amount=${entry.amount} but unit=item requires amount=1`,
      );
    }
  }
  // Processes consuming any item-unit flow inherit the missing-data marker.
  const consumesItem = [...proc.inputs, ...proc.outputs].some(
    (entry) => plan.flows[entry.flow]?.unit === 'item',
  );
  if (consumesItem && !proc.comment.includes(MISSING_DATA_PREFIX)) {
    proc.comment = prefixOnce(MISSING_DATA_PREFIX, proc.comment);
  }
}

for (const flowKey of referencedFlowKeys) {
  const flow = plan.flows[flowKey];
  if (!flow) fail(`missing flow definition for ${flowKey}`);
  flow.unit ||= 'kg';
}

// Rule: canonical_flow_key + conversion_factor. If a flow declares a canonical
// form (e.g. anhydrous version of a hydrate that already exists in the DB),
// both flows must exist in plan.flows and the conversion_factor must be a
// positive number. Materialize-from-plan swaps the exchange at write time.
for (const [flowKey, flow] of Object.entries(plan.flows)) {
  if (flow.canonical_flow_key == null) continue;
  const target = plan.flows[flow.canonical_flow_key];
  if (!target) {
    fail(`flow ${flowKey} canonical_flow_key=${flow.canonical_flow_key} not found in plan.flows`);
  }
  if (flow.unit !== target.unit) {
    fail(
      `flow ${flowKey} (unit=${flow.unit}) canonical_flow_key=${flow.canonical_flow_key} ` +
      `(unit=${target.unit}): units must match`,
    );
  }
  if (typeof flow.conversion_factor !== 'number' || !(flow.conversion_factor > 0)) {
    fail(`flow ${flowKey} has canonical_flow_key but conversion_factor is missing or non-positive`);
  }
}

if (write) {
  fs.writeFileSync(planPath, `${JSON.stringify(plan, null, 2)}\n`);
}

const summary = {
  schema_version: 1,
  status: 'normalized',
  plan: planPath,
  process_count: plan.processes.length,
  flow_count: Object.keys(plan.flows).length,
  black_box_processes: plan.processes.filter((proc) => proc.black_box).map((proc) => proc.key),
  pure_oxygen_processes: plan.processes.filter((proc) => proc.pure_oxygen).map((proc) => proc.key),
  canonical_aliases: Object.fromEntries(
    Object.entries(plan.flows)
      .filter(([, flow]) => flow.canonical_flow_key)
      .map(([key, flow]) => [key, { canonical: flow.canonical_flow_key, factor: flow.conversion_factor }]),
  ),
};

process.stdout.write(
  jsonMode ? `${JSON.stringify(summary)}\n` : `normalize-plan: ok ${JSON.stringify(summary)}\n`,
);
