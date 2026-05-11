import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  applyPatentPublishMetadataToBundle,
  buildPatentLifecyclemodelJsonTg,
} from '../patent-to-lifecyclemodel/scripts/publish-metadata.mjs';

const desc = (text) => [{ '@xml:lang': 'en', '#text': text }];

function processInstance(id, processId, label, extra = {}) {
  return {
    '@dataSetInternalID': id,
    referenceToProcess: {
      '@refObjectId': processId,
      '@version': '00.00.001',
      'common:shortDescription': desc(label),
    },
    ...extra,
  };
}

function modelPayload(processInstance, extraInfo = {}) {
  return {
    lifeCycleModelDataSet: {
      lifeCycleModelInformation: {
        ...extraInfo,
        technology: { processes: { processInstance } },
      },
    },
  };
}

function exchange(id, direction, flowId, label, extra = {}) {
  return {
    '@dataSetInternalID': id,
    exchangeDirection: direction,
    referenceUnit: 'kg',
    meanAmount: 1,
    referenceToFlowDataSet: {
      '@refObjectId': flowId,
      '@version': '00.00.001',
      'common:shortDescription': desc(label),
    },
    ...extra,
  };
}

function processPayload(processId, exchanges) {
  return {
    processDataSet: {
      processInformation: {
        dataSetInformation: { 'common:UUID': processId },
      },
      exchanges: { exchange: exchanges },
    },
  };
}

function writeBundleFixture({ payload, manifest = null, prefix = 'ptl-publish-' }) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const baseDir = manifest ? path.join(root, 'CN123') : root;
  const bundleDir = manifest ? path.join(baseDir, 'orchestrator-run') : baseDir;
  fs.mkdirSync(bundleDir, { recursive: true });
  if (manifest) {
    const manifestDir = path.join(baseDir, 'manifests');
    fs.mkdirSync(manifestDir, { recursive: true });
    fs.writeFileSync(
      path.join(manifestDir, 'lifecyclemodel-manifest.json'),
      `${JSON.stringify(manifest, null, 2)}\n`,
    );
  }
  fs.writeFileSync(path.join(bundleDir, 'model.json'), `${JSON.stringify(payload, null, 2)}\n`);
  const bundlePath = path.join(bundleDir, 'publish-bundle.json');
  fs.writeFileSync(
    bundlePath,
    `${JSON.stringify({ lifecyclemodels: [{ node_id: 'model', file: 'model.json' }] }, null, 2)}\n`,
  );
  return bundlePath;
}

test('json_tg exposes process names, unique ports, edge flow labels, and reference process', () => {
  const payload = modelPayload(
    [
      processInstance('1', 'process-a', 'Mix slurry', {
        connections: {
          outputExchange: {
            '@flowUUID': 'flow-a',
            downstreamProcess: { '@id': '2', '@flowUUID': 'flow-a' },
          },
        },
      }),
      processInstance('2', 'process-b', 'Dry electrode'),
    ],
    { quantitativeReference: { referenceToReferenceProcess: '2' } },
  );
  const processPayloads = [
    processPayload('process-a', [
      exchange('0', 'Output', 'flow-a', 'Wet precursor', { quantitativeReference: true }),
    ]),
    processPayload('process-b', [exchange('3', 'Input', 'flow-a', 'Wet precursor')]),
  ];

  const jsonTg = buildPatentLifecyclemodelJsonTg(payload, { processPayloads });

  assert.deepEqual(jsonTg.xflow.nodes[0].data.label, { baseName: desc('Mix slurry') });
  assert.deepEqual(jsonTg.xflow.nodes[1].data.label, { baseName: desc('Dry electrode') });
  assert.equal(jsonTg.xflow.nodes[0].data.quantitativeReference, '0');
  assert.equal(jsonTg.xflow.nodes[1].data.quantitativeReference, '1');
  assert.equal(jsonTg.xflow.nodes[0].ports.items[0].id, 'OUTPUT:flow-a');
  assert.equal(jsonTg.xflow.nodes[1].ports.items[0].id, 'INPUT:flow-a');
  assert.equal(jsonTg.xflow.edges[0].labels[0].text, 'Wet precursor');
  assert.deepEqual(jsonTg.xflow.edges[0].source, { cell: '1', port: 'OUTPUT:flow-a' });
  assert.deepEqual(jsonTg.xflow.edges[0].target, { cell: '2', port: 'INPUT:flow-a' });
  assert.equal(jsonTg.xflow.edges[0].data.flow.output.exchangeInternalId, '0');
  assert.equal(jsonTg.xflow.edges[0].data.flow.input.exchangeInternalId, '3');
  assert.equal(jsonTg.submodels[1].type, 'primary');
});

test('json_tg derives primary from native quantitativeReference', () => {
  const jsonTg = buildPatentLifecyclemodelJsonTg(
    modelPayload(
      [
        processInstance('1', 'process-a', 'Upstream step'),
        processInstance('2', 'process-b', 'Reference step'),
      ],
      {
        dataSetInformation: {
          referenceToResultingProcess: {
            '@refObjectId': 'resulting-process',
            '@version': '01.01.000',
          },
        },
        quantitativeReference: { referenceToReferenceProcess: '2' },
      },
    ),
  );

  assert.equal(jsonTg.submodels[0].type, 'secondary');
  assert.equal(jsonTg.submodels[1].type, 'primary');
  assert.equal(jsonTg.xflow.nodes[1].data.name, 'Reference step');
});

test('json_tg keeps duplicate same-flow ports unique', () => {
  const jsonTg = buildPatentLifecyclemodelJsonTg(
    modelPayload(processInstance('1', 'process-a', 'Repeated input step')),
    {
      processPayloads: [
        processPayload('process-a', [
          exchange('1', 'Input', 'flow-a', 'Shared reagent'),
          exchange('2', 'Input', 'flow-a', 'Shared reagent'),
        ]),
      ],
    },
  );

  assert.deepEqual(
    jsonTg.xflow.nodes[0].ports.items.map((port) => port.id),
    ['INPUT:flow-a', 'INPUT:flow-a:2'],
  );
});

test('publish metadata inlines file entries and writes json_tg', () => {
  const payload = modelPayload(processInstance('1', 'process-a', 'Named process'));
  const bundlePath = writeBundleFixture({ payload, prefix: 'ptl-publish-metadata-' });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  assert.equal(entry.file, undefined);
  assert.deepEqual(entry.json_ordered.lifeCycleModelDataSet, payload.lifeCycleModelDataSet);
  assert.deepEqual(entry.json_tg.xflow.nodes[0].data.label, { baseName: desc('Named process') });
});

test('publish metadata carries patent source into lifecyclemodel basic info', () => {
  const bundlePath = writeBundleFixture({
    payload: modelPayload(processInstance('1', 'process-a', 'Named process'), {
      dataSetInformation: { 'common:UUID': 'model-id' },
    }),
    manifest: {
      basic_info: {
        name: 'NCM cathode model',
        reference_year: '2023',
        source: {
          source_type: 'patent',
          source_id: 'CN123',
          assignee: 'Example Battery Co',
          inventor: '张三 李四',
          priority_date: '2021-01-01',
          publication_date: '2023-02-03',
          year: '2023',
        },
      },
    },
    prefix: 'ptl-publish-basic-info-',
  });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  const dataSetInformation =
    entry.json_ordered.lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation;
  assert.equal(entry.basic_info.source.assignee, 'Example Battery Co');
  assert.equal(dataSetInformation.referenceYear, '2023');
  assert.equal(dataSetInformation.patentSource.source_id, 'CN123');
  assert.match(
    dataSetInformation['common:generalComment'][0]['#text'],
    /CN123.*Example Battery Co.*2023/u,
  );
});
