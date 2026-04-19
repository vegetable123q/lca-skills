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
  if (normalized === 'Measured' || normalized === 'Estimated') return normalized;
  return fallback;
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

  for (const input of proc.inputs) {
    if (!input.flow) fail(`process ${proc.key} has an input without flow`);
    input.derivation = normalizeDerivation(input.derivation, 'Estimated');
    if (typeof input.amount !== 'number') input.amount = Number(input.amount ?? 0);
    referencedFlowKeys.add(input.flow);
  }

  let hasReferenceOutput = false;
  for (const output of proc.outputs) {
    if (!output.flow) fail(`process ${proc.key} has an output without flow`);
    output.derivation = normalizeDerivation(
      output.derivation,
      output.flow === proc.reference_output_flow ? 'Measured' : 'Estimated',
    );
    if (typeof output.amount !== 'number') output.amount = Number(output.amount ?? 0);
    if (output.flow === proc.reference_output_flow) hasReferenceOutput = true;
    referencedFlowKeys.add(output.flow);
  }

  if (!hasReferenceOutput) {
    fail(`process ${proc.key} has no output matching reference_output_flow=${proc.reference_output_flow}`);
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
    if (!/black-box/iu.test(proc.comment)) {
      proc.comment = `Black-box process. ${proc.comment}`.trim();
    }
  }
}

for (const flowKey of referencedFlowKeys) {
  const flow = plan.flows[flowKey];
  if (!flow) fail(`missing flow definition for ${flowKey}`);
  flow.unit ||= 'kg';
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
};

process.stdout.write(
  jsonMode ? `${JSON.stringify(summary)}\n` : `normalize-plan: ok ${JSON.stringify(summary)}\n`,
);
