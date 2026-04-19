#!/usr/bin/env node
// run-patent-to-lifecyclemodel.mjs
//
// End-to-end driver. Composition only — no builder logic.
//
// Modes:
//   1. Plan-driven (preferred — minimises LLM work):
//        --plan <plan.json>  --base <output/SOURCE>
//      runs materialize-from-plan (Stages 1, 3, 4), then Stage 5, then
//      auto-generates orchestrator-request.json, then Stage 6.
//
//   2. Manual (when flows/ + runs/combined/ + orchestrator-request.json are
//      already authored):
//        --base <output/SOURCE>  [--stage5-only | --stage6-only | --all]
//
// Usage examples:
//   node scripts/run-patent-to-lifecyclemodel.mjs --plan output/X/plan.json --base output/X --json
//   node scripts/run-patent-to-lifecyclemodel.mjs --base output/X --all --json

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const skillDir = path.dirname(path.dirname(__filename));
const projectRoot = path.dirname(skillDir);

const argv = process.argv.slice(2);
const arg = (f, d = null) => { const i = argv.indexOf(f); return i === -1 ? d : (argv[i + 1] ?? d); };
const has = (f) => argv.includes(f);

const baseArg = arg('--base');
if (!baseArg) { console.error('run-patent-to-lifecyclemodel: --base is required'); process.exit(2); }
const base = path.resolve(process.cwd(), baseArg);
const planPath = arg('--plan');
const jsonMode = has('--json');

const stage5Only = has('--stage5-only');
const stage6Only = has('--stage6-only');
const runStage5 = stage5Only || (!stage5Only && !stage6Only) || has('--all');
const runStage6 = stage6Only || (!stage5Only && !stage6Only) || has('--all');

function run(label, args) {
  if (!jsonMode) console.error(`[${label}]`);
  const res = spawnSync(process.execPath, args, { stdio: jsonMode ? 'pipe' : 'inherit' });
  if (res.status !== 0) {
    if (jsonMode && res.stdout) process.stderr.write(res.stdout.toString());
    if (jsonMode && res.stderr) process.stderr.write(res.stderr.toString());
    console.error(`run-patent-to-lifecyclemodel: ${label} failed`);
    process.exit(res.status ?? 1);
  }
  return res;
}

// ---------- Optional Stage 1/3/4: plan-driven materialization ----------
const materializeScript = path.join(skillDir, 'scripts', 'materialize-from-plan.mjs');
if (planPath) {
  run('materialize-from-plan', [
    materializeScript,
    '--plan', path.resolve(process.cwd(), planPath),
    '--base', base,
    '--json',
  ]);
}

// ---------- Stage 5 ----------
const manifestPath = path.join(base, 'manifests', 'lifecyclemodel-manifest.json');
const lifecyclemodelRunDir = path.join(base, 'lifecyclemodel-run');
if (runStage5) {
  if (!fs.existsSync(manifestPath)) {
    console.error(`run-patent-to-lifecyclemodel: missing ${manifestPath} (author it or pass --plan)`);
    process.exit(2);
  }
  run('stage5:lifecyclemodel auto-build', [
    path.join(projectRoot, 'lifecyclemodel-automated-builder', 'scripts', 'run-lifecyclemodel-automated-builder.mjs'),
    'build',
    '--manifest', manifestPath,
    '--out-dir', lifecyclemodelRunDir,
    '--json',
  ]);
}

// ---------- Auto-generate orchestrator-request.json if plan given ----------
const orchestratorRequestPath = path.join(base, 'orchestrator-request.json');
if (planPath && runStage6) {
  const plan = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), planPath), 'utf8'));
  const uuids = JSON.parse(fs.readFileSync(path.join(base, 'uuids.json'), 'utf8'));
  // find the built model
  const modelsDir = path.join(lifecyclemodelRunDir, 'models', 'combined', 'tidas_bundle', 'lifecyclemodels');
  if (!fs.existsSync(modelsDir)) {
    console.error(`run-patent-to-lifecyclemodel: missing built model dir ${modelsDir}; run Stage 5 first`);
    process.exit(2);
  }
  const modelFile = fs.readdirSync(modelsDir).find((f) => f.endsWith('.json'));
  if (!modelFile) { console.error('run-patent-to-lifecyclemodel: no built lifecyclemodel found'); process.exit(2); }
  const [modelUuid, versionWithExt] = modelFile.split('_');
  const modelVersion = versionWithExt.replace(/\.json$/, '');
  const modelFileAbs = path.join(modelsDir, modelFile);

  const sourceSlug = (plan.source?.id || 'source').toLowerCase();
  const processNodes = plan.processes.map((p) => ({
    node_id: `${p.key}-node`,
    kind: 'process',
    label: `${p.step_id || ''} ${p.name_en || p.key}`.trim(),
    requested_action: 'build_process',
    process: { id: uuids.procs[p.key], version: '00.00.001', name: p.name_en || p.key },
    process_builder: {
      flow_file: path.join(base, 'flows', `${String(plan.processes.indexOf(p) + 1).padStart(2, '0')}-${p.key}.json`),
    },
  }));

  // build edges: for each process, for each input flow that is also an output of another process, add an edge
  const edges = [];
  for (const downstream of plan.processes) {
    for (const inp of downstream.inputs || []) {
      const upstream = plan.processes.find((p) => (p.outputs || []).some((o) => o.flow === inp.flow));
      if (upstream && upstream.key !== downstream.key) {
        edges.push({ from: `${upstream.key}-node`, to: `${downstream.key}-node`, relation: `supplies ${inp.flow}` });
      }
    }
  }
  const rootNodeId = `${sourceSlug}-model`;
  // the final process (reference) also feeds the root model
  const finalProc = plan.processes[plan.processes.length - 1];
  edges.push({ from: `${finalProc.key}-node`, to: rootNodeId, relation: 'reference-producing process' });

  const request = {
    request_id: `${sourceSlug}-orchestration-001`,
    goal: {
      name: plan.goal?.name || plan.source?.title || plan.source?.id || '',
      functional_unit: plan.goal?.functional_unit || { amount: 1, unit: 'kg' },
      boundary: plan.goal?.boundary || 'cradle-to-gate',
    },
    root: {
      node_id: rootNodeId,
      kind: 'lifecyclemodel',
      lifecyclemodel: { id: modelUuid, version: modelVersion, name: `${plan.source?.id} lifecycle model` },
      requested_action: 'build_submodel',
      depends_on: processNodes.map((n) => n.node_id),
      submodel_builder: { manifest: manifestPath },
      projector: { command: 'build', model_file: modelFileAbs, projection_role: 'primary' },
    },
    orchestration: {
      mode: 'expanded', max_depth: 3, reuse_resulting_process_first: false,
      allow_process_build: true, allow_submodel_build: true, pin_child_versions: true,
      stop_at_elementary_flow: false, cutoff_policy: 'default', fail_fast: false,
    },
    candidate_sources: { my_processes: false, team_processes: false, public_processes: false, existing_lifecyclemodels: false, existing_resulting_processes: false },
    nodes: processNodes,
    edges,
    publish: { intent: 'prepare_only', prepare_lifecyclemodel_payload: true, prepare_resulting_process_payload: true, prepare_relation_payload: true },
    notes: [
      `Source: ${plan.source?.id || ''}`,
      'Generated by patent-to-lifecyclemodel; exchange amounts from the plan are estimates until verified.',
    ],
  };
  fs.writeFileSync(orchestratorRequestPath, JSON.stringify(request, null, 2) + '\n');
  if (!jsonMode) console.error(`[auto] wrote ${orchestratorRequestPath}`);
}

// ---------- Stage 6 ----------
const orchestratorRunDir = path.join(base, 'orchestrator-run');
if (runStage6) {
  if (!fs.existsSync(orchestratorRequestPath)) {
    console.error(`run-patent-to-lifecyclemodel: missing ${orchestratorRequestPath} (author it or pass --plan)`);
    process.exit(2);
  }
  const orchScript = path.join(projectRoot, 'lifecyclemodel-recursive-orchestrator', 'scripts', 'run-lifecyclemodel-recursive-orchestrator.mjs');
  run('stage6:plan', [orchScript, 'plan', '--request', orchestratorRequestPath, '--out-dir', orchestratorRunDir, '--json']);
  run('stage6:execute', [orchScript, 'execute', '--request', orchestratorRequestPath, '--out-dir', orchestratorRunDir,
    '--allow-process-build', '--allow-submodel-build', '--json']);
  run('stage6:publish', [orchScript, 'publish', '--run-dir', orchestratorRunDir,
    '--publish-lifecyclemodels', '--publish-resulting-process-relations', '--json']);
}

if (jsonMode) {
  process.stdout.write(JSON.stringify({
    schema_version: 1, status: 'completed', base,
    lifecyclemodel_run: lifecyclemodelRunDir, orchestrator_run: orchestratorRunDir,
  }) + '\n');
}
