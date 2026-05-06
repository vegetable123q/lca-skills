import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildPatentFlowRowsFromPlan,
  isExistingFlowPreflightOnlyReport,
} from '../patent-to-lifecyclemodel/scripts/flow-datasets.mjs';

test('buildPatentFlowRowsFromPlan emits publishable flow datasets with reference properties', () => {
  const plan = {
    source: { id: 'CN123', assignee: 'Example Assignee' },
    flows: {
      cathode: { name_en: 'Composite cathode', name_zh: '复合正极', unit: 'kg' },
      solvent: { name_en: 'Solvent mixture', unit: 'L' },
      operation: { name_en: 'Black-box operation', unit: 'item' },
    },
  };
  const uuids = {
    flows: {
      cathode: 'flow-cathode',
      solvent: 'flow-solvent',
      operation: 'flow-operation',
    },
  };

  const rows = buildPatentFlowRowsFromPlan(plan, uuids);

  assert.equal(rows.length, 3);
  assert.deepEqual(
    rows.map((row) => [row.id, row.version]),
    [
      ['flow-cathode', '01.00.000'],
      ['flow-operation', '01.00.000'],
      ['flow-solvent', '01.00.000'],
    ],
  );

  const cathode = rows.find((row) => row.id === 'flow-cathode').json_ordered.flowDataSet;
  assert.equal(
    cathode.flowInformation.dataSetInformation.name.baseName[0]['#text'],
    'Composite cathode',
  );
  assert.equal(
    cathode.flowProperties.flowProperty.referenceToFlowPropertyDataSet['@refObjectId'],
    '93a60a56-a3c8-11da-a746-0800200b9a66',
  );

  const solvent = rows.find((row) => row.id === 'flow-solvent').json_ordered.flowDataSet;
  assert.equal(
    solvent.flowProperties.flowProperty.referenceToFlowPropertyDataSet['@refObjectId'],
    '93a60a56-a3c8-22da-a746-0800200c9a66',
  );

  const operation = rows.find((row) => row.id === 'flow-operation').json_ordered.flowDataSet;
  assert.equal(
    operation.flowProperties.flowProperty.referenceToFlowPropertyDataSet['@refObjectId'],
    '01846770-4cfe-4a25-8ad9-919d8d378345',
  );
});

test('isExistingFlowPreflightOnlyReport recognizes already-visible flow preflight failures', () => {
  assert.equal(
    isExistingFlowPreflightOnlyReport({
      flow_reports: [
        {
          status: 'failed',
          error: [{ code: 'target_user_id_required' }],
        },
        {
          status: 'failed',
          error: [{ code: 'exact_version_visible_not_owned' }],
        },
      ],
    }),
    true,
  );

  assert.equal(
    isExistingFlowPreflightOnlyReport({
      flow_reports: [{ status: 'failed', error: [{ code: 'REMOTE_INVALID_JSON' }] }],
    }),
    false,
  );
});
