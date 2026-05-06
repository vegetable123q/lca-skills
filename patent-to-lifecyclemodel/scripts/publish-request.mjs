import fs from 'node:fs';
import path from 'node:path';

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
  const resolvedPath = path.resolve(filePath);
  fs.mkdirSync(path.dirname(resolvedPath), { recursive: true });
  fs.writeFileSync(resolvedPath, `${JSON.stringify(request, null, 2)}\n`);
  return { request, path: resolvedPath };
}
