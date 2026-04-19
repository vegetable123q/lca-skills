#!/usr/bin/env node
// materialize-from-plan.mjs
//
// One compact plan.json (authored once by the LLM from the source doc) is
// expanded into every artifact that Stages 1, 3, 4, and 5-manifest need:
//   flows/NN-<proc_key>.json
//   runs/<proc_key>/                              (via process-automated-builder auto-build)
//   uuids.json
//   runs/combined/exports/processes/<uuid>_<ver>.json
//   runs/combined/cache/process_from_flow_state.json          (copied)
//   runs/combined/manifests/*.json                            (copied)
//   manifests/lifecyclemodel-manifest.json
//
// The LLM never touches the ILCD template, allocate-uuids flags, or manifest
// boilerplate after writing plan.json.
//
// Usage:
//   node scripts/materialize-from-plan.mjs \
//     --plan output/<SOURCE>/plan.json \
//     --base output/<SOURCE> \
//     [--seed <hex>]    # deterministic UUIDs
//     [--json]

import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const skillDir = path.dirname(path.dirname(__filename));
const projectRoot = path.dirname(skillDir);

const argv = process.argv.slice(2);
const arg = (f, d = null) => { const i = argv.indexOf(f); return i === -1 ? d : (argv[i + 1] ?? d); };

function printHelp() {
  console.log(`Usage:
  node patent-to-lifecyclemodel/scripts/materialize-from-plan.mjs --plan <plan.json> --base <output-dir> [--seed <seed>] [--json]

Examples:
  node patent-to-lifecyclemodel/scripts/materialize-from-plan.mjs --plan output/CN111725499B/plan.json --base output/CN111725499B --json
  node patent-to-lifecyclemodel/scripts/materialize-from-plan.mjs --plan output/CN111725499B/plan.json --base output/CN111725499B --seed cn111725499b
`.trim());
}

if (argv.includes('--help') || argv.includes('-h')) {
  printHelp();
  process.exit(0);
}

const planPath = arg('--plan');
const baseArg = arg('--base');
const seed = arg('--seed', '');
const jsonMode = argv.includes('--json');

if (!planPath || !baseArg) {
  console.error('materialize-from-plan: --plan and --base are required');
  process.exit(2);
}

const plan = JSON.parse(fs.readFileSync(planPath, 'utf8'));
const base = path.resolve(process.cwd(), baseArg);

function seededUuid(key) {
  if (!seed) return crypto.randomUUID();
  const h = crypto.createHash('sha256').update(`${seed}|${key}`).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function ensureDir(p) { fs.mkdirSync(p, { recursive: true }); }
function writeJson(p, obj) { ensureDir(path.dirname(p)); fs.writeFileSync(p, JSON.stringify(obj, null, 2) + '\n'); }

// ---------- 1. Allocate UUIDs from plan ----------
const flowKeys = Object.keys(plan.flows || {});
const procKeys = (plan.processes || []).map((p) => p.key);
const uuids = {
  flows: Object.fromEntries(flowKeys.map((k) => [k, seededUuid(`flow:${k}`)])),
  procs: Object.fromEntries(procKeys.map((k) => [k, seededUuid(`proc:${k}`)])),
  srcs: { patent: seededUuid(`src:${plan.source?.id || 'source'}`) },
};
writeJson(path.join(base, 'uuids.json'), uuids);

// ---------- 2. Author flow files ----------
(plan.processes || []).forEach((proc, idx) => {
  const refFlowKey = proc.reference_output_flow;
  const refFlow = plan.flows?.[refFlowKey] || {};
  const flowFile = {
    flow: {
      name: refFlow.name_en || refFlowKey,
      name_zh: refFlow.name_zh,
      unit: refFlow.unit || 'kg',
      reference_amount: 1,
      description: proc.comment || '',
      source_document: plan.source?.id || '',
    },
    operation: 'produce',
    process: {
      name: proc.name_en,
      type: 'unit process',
      technology: { description: proc.technology || '' },
    },
    references: { patent_id: plan.source?.id || '', step_id: proc.step_id || '' },
  };
  const nn = String(idx + 1).padStart(2, '0');
  writeJson(path.join(base, 'flows', `${nn}-${proc.key}.json`), flowFile);
});

// ---------- 3. Scaffold one process-automated-builder run per flow ----------
const procBuilderScript = path.join(projectRoot, 'process-automated-builder', 'scripts', 'run-process-automated-builder.mjs');
(plan.processes || []).forEach((proc, idx) => {
  const nn = String(idx + 1).padStart(2, '0');
  const flowFile = path.join(base, 'flows', `${nn}-${proc.key}.json`);
  const runDir = path.join(base, 'runs', `${nn}-${proc.key}`);
  if (fs.existsSync(runDir)) return;  // idempotent
  const res = spawnSync(process.execPath, [
    procBuilderScript, 'auto-build',
    '--flow-file', flowFile,
    '--operation', 'produce',
    '--out-dir', runDir,
    '--json',
  ], { stdio: jsonMode ? 'pipe' : 'inherit' });
  if (res.status !== 0) {
    console.error(`materialize-from-plan: process auto-build failed for ${proc.key}`);
    process.exit(res.status ?? 1);
  }
});

// ---------- 4. Build ILCD processDataSet files into runs/combined ----------
const combinedDir = path.join(base, 'runs', 'combined');
const combinedExports = path.join(combinedDir, 'exports', 'processes');
ensureDir(combinedExports);
ensureDir(path.join(combinedDir, 'cache'));
ensureDir(path.join(combinedDir, 'manifests'));

// copy state + manifests from the first scaffold run
const firstRunDir = path.join(base, 'runs', `01-${plan.processes[0].key}`);
const stateSrc = path.join(firstRunDir, 'cache', 'process_from_flow_state.json');
if (fs.existsSync(stateSrc)) fs.copyFileSync(stateSrc, path.join(combinedDir, 'cache', 'process_from_flow_state.json'));
const firstManifestsDir = path.join(firstRunDir, 'manifests');
if (fs.existsSync(firstManifestsDir)) {
  for (const name of fs.readdirSync(firstManifestsDir)) {
    if (name.endsWith('.json')) {
      fs.copyFileSync(path.join(firstManifestsDir, name), path.join(combinedDir, 'manifests', name));
    }
  }
}

const VERSION = '00.00.001';
const sourceUuid = uuids.srcs.patent;

function buildIlcd(proc) {
  const procUuid = uuids.procs[proc.key];
  const refFlowKey = proc.reference_output_flow;
  let internalCounter = 0;
  const nextId = () => String(internalCounter++);
  const exchange = [];
  // inputs first, outputs second; refOutput internal id noted
  let refInternalId = null;

  (proc.inputs || []).forEach((x) => {
    const flow = plan.flows[x.flow] || {};
    exchange.push({
      '@dataSetInternalID': nextId(),
      referenceToFlowDataSet: {
        '@type': 'flow data set',
        '@refObjectId': uuids.flows[x.flow],
        '@version': '01.00.000',
        'common:shortDescription': [{ '@xml:lang': 'en', '#text': flow.name_en || x.flow }],
      },
      exchangeDirection: 'Input',
      meanAmount: x.amount ?? 0,
      resultingAmount: x.amount ?? 0,
      dataDerivationTypeStatus: x.derivation || 'Estimated',
    });
  });
  (proc.outputs || []).forEach((x) => {
    const flow = plan.flows[x.flow] || {};
    const id = nextId();
    if (x.flow === refFlowKey) refInternalId = id;
    exchange.push({
      '@dataSetInternalID': id,
      referenceToFlowDataSet: {
        '@type': 'flow data set',
        '@refObjectId': uuids.flows[x.flow],
        '@version': '01.00.000',
        'common:shortDescription': [{ '@xml:lang': 'en', '#text': flow.name_en || x.flow }],
      },
      exchangeDirection: 'Output',
      meanAmount: x.amount ?? 0,
      resultingAmount: x.amount ?? 0,
      dataDerivationTypeStatus: x.derivation || 'Measured',
    });
  });

  if (refInternalId === null) {
    console.error(`materialize-from-plan: process ${proc.key} has no output matching reference_output_flow=${refFlowKey}`);
    process.exit(2);
  }

  return {
    processDataSet: {
      '@xmlns': 'http://lca.jrc.it/ILCD/Process',
      '@xmlns:common': 'http://lca.jrc.it/ILCD/Common',
      '@version': '1.1',
      processInformation: {
        dataSetInformation: {
          'common:UUID': procUuid,
          name: {
            baseName: [
              { '@xml:lang': 'en', '#text': proc.name_en || proc.key },
              ...(proc.name_zh ? [{ '@xml:lang': 'zh', '#text': proc.name_zh }] : []),
            ],
            mixAndLocationTypes: [{ '@xml:lang': 'en', '#text': proc.scale || '' }],
          },
          classificationInformation: {
            'common:classification': {
              'common:class': (proc.classification || ['Chemicals and chemical products']).map((c, i) => ({ '@level': String(i), '#text': c })),
            },
          },
          'common:generalComment': [{ '@xml:lang': 'en', '#text': proc.comment || '' }],
        },
        quantitativeReference: { '@type': 'Reference flow(s)', referenceToReferenceFlow: refInternalId },
        time: { 'common:referenceYear': String(plan.reference_year || ''), 'common:dataSetValidUntil': '2030' },
        geography: { locationOfOperationSupplyOrProduction: { '@location': plan.geography || '' } },
        technology: { technologyDescriptionAndIncludedProcesses: [{ '@xml:lang': 'en', '#text': proc.technology || '' }] },
      },
      modellingAndValidation: {
        LCIMethodAndAllocation: {
          typeOfDataSet: 'Unit process, single operation',
          LCIMethodPrinciple: 'Attributional',
          deviationsFromLCIMethodPrinciple: [{ '@xml:lang': 'en', '#text': 'None' }],
          LCIMethodApproaches: ['Allocation - mass'],
          deviationsFromLCIMethodApproaches: [{ '@xml:lang': 'en', '#text': 'None' }],
        },
        dataSourcesTreatmentAndRepresentativeness: {
          referenceToDataSource: [{
            '@type': 'source data set',
            '@refObjectId': sourceUuid,
            '@version': '01.00.000',
            '@uri': plan.source?.id || '',
            'common:shortDescription': [{ '@xml:lang': 'en', '#text': plan.source?.title || plan.source?.id || '' }],
          }],
        },
      },
      administrativeInformation: {
        dataEntryBy: { 'common:timeStamp': new Date().toISOString() },
        publicationAndOwnership: {
          'common:dataSetVersion': VERSION,
          'common:referenceToOwnershipOfDataSet': {
            '@refObjectId': sourceUuid,
            '@type': 'contact data set',
            '@version': '01.00.000',
            'common:shortDescription': [{ '@xml:lang': 'en', '#text': plan.source?.assignee || plan.source?.id || '' }],
          },
        },
      },
      exchanges: { exchange },
    },
  };
}

(plan.processes || []).forEach((proc) => {
  const dataset = buildIlcd(proc);
  const fname = `${uuids.procs[proc.key]}_${VERSION}.json`;
  writeJson(path.join(combinedExports, fname), dataset);
});

// ---------- 5. Emit lifecyclemodel manifest ----------
writeJson(path.join(base, 'manifests', 'lifecyclemodel-manifest.json'), {
  run_label: `${plan.source?.id || 'source'}-lifecyclemodel`,
  allow_remote_write: false,
  selection: { mode: 'graph_first_local_inference', max_models: 1, max_processes_per_model: 12 },
  output: { write_local_models: true, emit_validation_report: true },
  local_runs: [combinedDir],
});

const summary = {
  schema_version: 1,
  status: 'materialized',
  base,
  plan: planPath,
  processes: procKeys.length,
  flows: flowKeys.length,
  uuids_file: path.join(base, 'uuids.json'),
  combined_run: combinedDir,
  lifecyclemodel_manifest: path.join(base, 'manifests', 'lifecyclemodel-manifest.json'),
};

if (jsonMode) process.stdout.write(JSON.stringify(summary) + '\n');
else console.error(`materialize-from-plan: ok — ${summary.processes} processes, ${summary.flows} flows → ${base}`);
