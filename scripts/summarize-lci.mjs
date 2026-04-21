#!/usr/bin/env node
// Summarize per-process LCI tables from one or more local build runs under output/<run_id>/.
// Reads plan.json for flow unit metadata and process ordering, then reads each exported
// processDataSet under runs/combined/exports/processes/ (falling back to per-process runs).
//
// Usage:
//   node scripts/summarize-lci.mjs <run_dir> [<run_dir> ...]
//   node scripts/summarize-lci.mjs --help
//
// Example:
//   node scripts/summarize-lci.mjs output/CN110980817B output/CN111725499B

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const HELP = `Usage: node scripts/summarize-lci.mjs <run_dir> [<run_dir> ...]

Produces Markdown LCI tables (inputs / outputs with units) for every exported
processDataSet inside a run directory such as output/CN110980817B.

Options:
  -h, --help      Show this message and exit.
`;

function parseArgs(argv) {
  const runs = [];
  for (const a of argv) {
    if (a === '-h' || a === '--help') {
      process.stdout.write(HELP);
      process.exit(0);
    }
    runs.push(a);
  }
  if (!runs.length) {
    process.stderr.write(HELP);
    process.exit(1);
  }
  return runs;
}

function readJson(p) {
  return JSON.parse(readFileSync(p, 'utf8'));
}

function pickEnglishText(node) {
  if (!node) return '';
  if (Array.isArray(node)) {
    const en = node.find((x) => x?.['@xml:lang'] === 'en');
    return (en ?? node[0])?.['#text'] ?? '';
  }
  if (typeof node === 'object') return node['#text'] ?? '';
  return String(node);
}

function collectProcessFiles(runDir, plan) {
  const combined = path.join(runDir, 'runs', 'combined', 'exports', 'processes');
  const files = [];
  if (existsSync(combined) && statSync(combined).isDirectory()) {
    for (const f of readdirSync(combined)) {
      if (f.endsWith('.json')) files.push(path.join(combined, f));
    }
  }
  if (files.length) return files;
  // Fallback: per-process run dirs
  const runsRoot = path.join(runDir, 'runs');
  if (!existsSync(runsRoot)) return files;
  for (const entry of readdirSync(runsRoot)) {
    const procDir = path.join(runsRoot, entry, 'exports', 'processes');
    if (existsSync(procDir) && statSync(procDir).isDirectory()) {
      for (const f of readdirSync(procDir)) {
        if (f.endsWith('.json')) files.push(path.join(procDir, f));
      }
    }
  }
  return files;
}

function loadUnitIndex(plan) {
  // Index units by lowercase english flow name so we can look them up by
  // exchange shortDescription (which is the english name_en from plan).
  const byName = new Map();
  for (const [key, flow] of Object.entries(plan.flows ?? {})) {
    const names = [flow.name_en, flow.name_zh, key].filter(Boolean);
    for (const n of names) byName.set(String(n).toLowerCase(), flow.unit);
  }
  return byName;
}

function loadProcessOrder(plan) {
  // Preserve plan-declared order; map english process name_en → sort index + step_id.
  const order = new Map();
  (plan.processes ?? []).forEach((p, i) => {
    const name = (p.name_en ?? p.key ?? '').toLowerCase();
    order.set(name, { index: i, step_id: p.step_id, key: p.key });
  });
  return order;
}

function summarizeProcessFile(file, unitIndex) {
  const doc = readJson(file);
  const ds = doc.processDataSet ?? {};
  const info = ds.processInformation ?? {};
  const dsi = info.dataSetInformation ?? {};
  const name = pickEnglishText(dsi.name?.baseName);
  const nameZh = (() => {
    const arr = dsi.name?.baseName;
    if (Array.isArray(arr)) {
      const zh = arr.find((x) => x?.['@xml:lang'] === 'zh');
      return zh?.['#text'] ?? '';
    }
    return '';
  })();
  const location = pickEnglishText(dsi.name?.mixAndLocationTypes);
  const refYear = info.time?.['common:referenceYear'] ?? '';
  const refFlowInternalId = info.quantitativeReference?.referenceToReferenceFlow;
  const exchanges = ds.exchanges?.exchange ?? [];
  const inputs = [];
  const outputs = [];
  for (const ex of exchanges) {
    const flowName = pickEnglishText(ex.referenceToFlowDataSet?.['common:shortDescription']);
    const unit = unitIndex.get(String(flowName).toLowerCase()) ?? '';
    const row = {
      flow: flowName,
      amount: ex.meanAmount ?? ex.resultingAmount,
      unit,
      derivation: ex.dataDerivationTypeStatus ?? '',
      isRef: String(ex['@dataSetInternalID']) === String(refFlowInternalId),
    };
    if (ex.exchangeDirection === 'Input') inputs.push(row);
    else outputs.push(row);
  }
  return { file, name, nameZh, location, refYear, inputs, outputs };
}

function renderTable(rows, title) {
  if (!rows.length) return `_No ${title.toLowerCase()}._`;
  const header = '| Flow | Amount | Unit | Derivation |';
  const sep = '| --- | ---: | --- | --- |';
  const body = rows
    .map((r) => {
      const label = r.isRef ? `**${r.flow}** _(reference)_` : r.flow;
      return `| ${label} | ${r.amount} | ${r.unit || '—'} | ${r.derivation} |`;
    })
    .join('\n');
  return [`**${title}**`, '', header, sep, body].join('\n');
}

function renderProcess(p, order) {
  const meta = order.get(String(p.name).toLowerCase());
  const stepTag = meta?.step_id ? ` (step ${meta.step_id})` : '';
  const lines = [];
  lines.push(`### ${p.name}${stepTag}`);
  if (p.nameZh) lines.push(`_${p.nameZh}_`);
  const metaBits = [];
  if (p.location) metaBits.push(p.location);
  if (p.refYear) metaBits.push(`reference year ${p.refYear}`);
  if (metaBits.length) lines.push(metaBits.join(' · '));
  lines.push('');
  lines.push(renderTable(p.inputs, 'Inputs'));
  lines.push('');
  lines.push(renderTable(p.outputs, 'Outputs'));
  return lines.join('\n');
}

function summarizeRun(runDir) {
  const planPath = path.join(runDir, 'plan.json');
  if (!existsSync(planPath)) {
    return `## ${path.basename(runDir)}\n\n_No plan.json found under ${runDir}._`;
  }
  const plan = readJson(planPath);
  const unitIndex = loadUnitIndex(plan);
  const order = loadProcessOrder(plan);
  const files = collectProcessFiles(runDir, plan);
  if (!files.length) {
    return `## ${path.basename(runDir)}\n\n_No exported processes under ${runDir}/runs/**/exports/processes/._`;
  }
  const summaries = files.map((f) => summarizeProcessFile(f, unitIndex));
  summaries.sort((a, b) => {
    const ai = order.get(String(a.name).toLowerCase())?.index ?? 999;
    const bi = order.get(String(b.name).toLowerCase())?.index ?? 999;
    return ai - bi;
  });
  const source = plan.source ?? {};
  const goal = plan.goal ?? {};
  const fu = goal.functional_unit ?? {};
  const header = [
    `## ${path.basename(runDir)} — ${source.title ?? source.id ?? ''}`,
    '',
    `- Patent / source: **${source.id ?? ''}**`,
    `- Functional unit: **${fu.amount ?? ''} ${fu.unit ?? ''}** ${goal.name ? `(${goal.name})` : ''}`.trim(),
    `- Boundary: ${goal.boundary ?? ''}`,
    `- Geography / year: ${plan.geography ?? ''} / ${plan.reference_year ?? ''}`,
    '',
  ].join('\n');
  return [header, ...summaries.map((s) => renderProcess(s, order))].join('\n\n');
}

function main() {
  const runs = parseArgs(process.argv.slice(2));
  const blocks = runs.map((r) => summarizeRun(path.resolve(r)));
  process.stdout.write(`# LCI Summary\n\n${blocks.join('\n\n---\n\n')}\n`);
}

main();
