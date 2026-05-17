import fs from 'node:fs';
import path from 'node:path';
import { buildPatentSourceDataset } from './publish-metadata.mjs';

function listProcessExportFiles(base) {
  const runsDir = path.join(base, 'runs');
  if (!fs.existsSync(runsDir)) return [];

  const files = [];
  for (const runName of fs.readdirSync(runsDir).sort()) {
    const exportsDir = path.join(runsDir, runName, 'exports', 'processes');
    if (!fs.existsSync(exportsDir)) continue;
    for (const fileName of fs.readdirSync(exportsDir).sort()) {
      if (fileName.endsWith('.json')) {
        files.push(path.join(exportsDir, fileName));
      }
    }
  }
  return files;
}

function readJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writePatentSourceInput(base) {
  const plan = readJsonIfExists(path.join(base, 'plan.json'));
  const uuids = readJsonIfExists(path.join(base, 'uuids.json'));
  const sourceUuid = uuids?.srcs?.patent;
  if (!plan?.source || typeof sourceUuid !== 'string' || !sourceUuid.trim()) {
    return null;
  }

  const sourceDir = path.join(base, 'publish-metadata', 'sources');
  const sourcePath = path.join(sourceDir, `${sourceUuid}_01.00.000.json`);
  fs.mkdirSync(sourceDir, { recursive: true });
  fs.writeFileSync(
    sourcePath,
    `${JSON.stringify(
      buildPatentSourceDataset({
        source: plan.source,
        sourceUuid,
      }),
      null,
      2,
    )}\n`,
  );
  return sourcePath;
}

export function buildPatentPublishRequest(base, options = {}) {
  const resolvedBase = path.resolve(base);
  const commit = options.commit === true;
  const outDir = options.outDir ? path.resolve(options.outDir) : path.join(resolvedBase, 'publish-run');
  const maxAttempts = Number.isInteger(options.maxAttempts) ? options.maxAttempts : 5;
  const retryDelaySeconds =
    typeof options.retryDelaySeconds === 'number' ? options.retryDelaySeconds : 2;

  const inputs = {
    bundle_paths: [path.join(resolvedBase, 'orchestrator-run', 'publish-bundle.json')],
  };
  const processFiles = listProcessExportFiles(resolvedBase);
  if (processFiles.length > 0) {
    inputs.processes = processFiles;
  }

  return {
    inputs: {
      ...inputs,
    },
    publish: {
      commit,
      publish_lifecyclemodels: true,
      publish_processes: true,
      publish_sources: true,
      publish_relations: true,
      publish_process_build_runs: false,
      relation_mode: 'local_manifest_only',
      max_attempts: maxAttempts,
      retry_delay_seconds: retryDelaySeconds,
      process_build_forward_args: [],
    },
    out_dir: outDir,
  };
}

export function writePatentPublishRequest(filePath, base, options = {}) {
  const request = buildPatentPublishRequest(base, options);
  const sourcePath = writePatentSourceInput(path.resolve(base));
  if (sourcePath) {
    request.inputs.sources = [sourcePath, ...(request.inputs.sources || [])];
  }
  const resolvedPath = path.resolve(filePath);
  fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });
  fs.writeFileSync(resolvedPath, `${JSON.stringify(request, null, 2)}\n`);
  return { request, path: resolvedPath };
}
