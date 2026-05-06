import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  applyPatentPublishMetadataToBundle,
  buildPatentLifecyclemodelJsonTg,
} from '../patent-to-lifecyclemodel/scripts/publish-metadata.mjs';

test('buildPatentLifecyclemodelJsonTg gives xflow nodes string labels and connected edges', () => {
  const payload = {
    lifeCycleModelDataSet: {
      lifeCycleModelInformation: {
        technology: {
          processes: {
            processInstance: [
              {
                '@dataSetInternalID': '1',
                referenceToProcess: {
                  '@refObjectId': 'process-a',
                  '@version': '00.00.001',
                  'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Mix slurry' }],
                },
                connections: {
                  outputExchange: {
                    '@flowUUID': 'flow-a',
                    downstreamProcess: { '@id': '2' },
                  },
                },
              },
              {
                '@dataSetInternalID': '2',
                referenceToProcess: {
                  '@refObjectId': 'process-b',
                  '@version': '00.00.001',
                  'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Dry electrode' }],
                },
              },
            ],
          },
        },
      },
    },
  };

  const jsonTg = buildPatentLifecyclemodelJsonTg(payload);

  assert.equal(jsonTg.xflow.nodes[0].data.label, 'Mix slurry');
  assert.equal(jsonTg.xflow.nodes[1].data.label, 'Dry electrode');
  assert.equal(jsonTg.submodels[0].name, 'Mix slurry');
  assert.deepEqual(jsonTg.xflow.edges[0].source, { cell: '1' });
  assert.deepEqual(jsonTg.xflow.edges[0].target, { cell: '2' });
  assert.equal(jsonTg.xflow.edges[0].data.connection.outputExchange['@flowUUID'], 'flow-a');
});

test('applyPatentPublishMetadataToBundle inlines file entries so tiangong publish can read json_tg', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-publish-metadata-'));
  const modelPath = path.join(dir, 'model.json');
  const bundlePath = path.join(dir, 'publish-bundle.json');
  const payload = {
    lifeCycleModelDataSet: {
      lifeCycleModelInformation: {
        technology: {
          processes: {
            processInstance: {
              '@dataSetInternalID': '1',
              referenceToProcess: {
                '@refObjectId': 'process-a',
                'common:shortDescription': [{ '@xml:lang': 'en', '#text': 'Named process' }],
              },
            },
          },
        },
      },
    },
  };
  fs.writeFileSync(modelPath, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(
    bundlePath,
    `${JSON.stringify({ lifecyclemodels: [{ node_id: 'model', file: 'model.json' }] }, null, 2)}\n`,
  );

  applyPatentPublishMetadataToBundle(bundlePath);

  const bundle = JSON.parse(fs.readFileSync(bundlePath, 'utf8'));
  assert.equal(bundle.lifecyclemodels[0].file, undefined);
  assert.deepEqual(
    bundle.lifecyclemodels[0].json_ordered.lifeCycleModelDataSet,
    payload.lifeCycleModelDataSet,
  );
  assert.equal(bundle.lifecyclemodels[0].json_tg.xflow.nodes[0].data.label, 'Named process');
});
