import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { buildPatentPublishRequest } from '../patent-to-lifecyclemodel/scripts/publish-request.mjs';

test('buildPatentPublishRequest points at orchestrator publish bundle and defaults to dry-run', () => {
  const base = path.resolve('/workspace/output/CN111725499B');

  const request = buildPatentPublishRequest(base);

  assert.deepEqual(request.inputs, {
    bundle_paths: [path.join(base, 'orchestrator-run', 'publish-bundle.json')],
  });
  assert.deepEqual(request.publish, {
    commit: false,
    publish_lifecyclemodels: true,
    publish_processes: true,
    publish_sources: true,
    publish_relations: true,
    publish_process_build_runs: false,
    relation_mode: 'local_manifest_only',
    max_attempts: 5,
    retry_delay_seconds: 2,
    process_build_forward_args: [],
  });
  assert.equal(request.out_dir, path.join(base, 'publish-run'));
});

test('buildPatentPublishRequest accepts commit and custom publish output directory', () => {
  const base = path.resolve('/workspace/output/CN111725499B');
  const outDir = path.resolve('/workspace/output/CN111725499B/db-publish');

  const request = buildPatentPublishRequest(base, { commit: true, outDir });

  assert.equal(request.publish.commit, true);
  assert.equal(request.out_dir, outDir);
});

test('buildPatentPublishRequest includes canonical process exports for remote publish', () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-publish-request-'));
  const processFile = path.join(
    base,
    'runs',
    'CN111725499B-combined',
    'exports',
    'processes',
    'process-id_00.00.001.json',
  );
  fs.mkdirSync(path.dirname(processFile), { recursive: true });
  fs.writeFileSync(processFile, '{}\n');

  const request = buildPatentPublishRequest(base);

  assert.deepEqual(request.inputs.processes, [processFile]);
});
