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

test('writePatentPublishRequest emits patent source dataset for remote validation', async () => {
  const { writePatentPublishRequest } = await import(
    '../patent-to-lifecyclemodel/scripts/publish-request.mjs'
  );
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-publish-request-source-'));
  fs.writeFileSync(
    path.join(base, 'plan.json'),
    `${JSON.stringify(
      {
        source: {
          id: 'CN108123128A',
          title: 'Surface-layer Al-doped NCM cathode material',
          assignee: 'Example Battery Co',
          publication_date: '2018-07-01',
          extra_metadata: {
            url: 'https://patents.google.com/patent/CN108123128A/en',
          },
        },
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(base, 'uuids.json'),
    `${JSON.stringify({ srcs: { patent: 'f549fef9-eb86-40a9-846e-2e95854971d1' } }, null, 2)}\n`,
  );

  const result = writePatentPublishRequest(path.join(base, 'publish-request.json'), base);

  assert.equal(result.request.inputs.sources.length, 1);
  const sourcePath = result.request.inputs.sources[0];
  const sourcePayload = JSON.parse(fs.readFileSync(sourcePath, 'utf8'));
  const sourceInfo = sourcePayload.sourceDataSet.sourceInformation.dataSetInformation;
  const admin = sourcePayload.sourceDataSet.administrativeInformation;

  assert.equal(sourceInfo['common:UUID'], 'f549fef9-eb86-40a9-846e-2e95854971d1');
  assert.match(sourceInfo.sourceCitation, /CN108123128A/u);
  assert.equal(
    admin.dataEntryBy['common:referenceToDataSetFormat']['@version'],
    '03.00.003',
  );
  assert.equal(
    admin.publicationAndOwnership['common:referenceToOwnershipOfDataSet']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
});
