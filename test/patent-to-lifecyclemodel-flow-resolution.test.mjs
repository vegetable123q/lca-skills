import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildFlowResolution,
  applyFlowResolutionToExchange,
} from '../patent-to-lifecyclemodel/scripts/flow-resolution.mjs';

function flowRow({ id, version, name, property = 'Mass' }) {
  const propertyIds = {
    Mass: '93a60a56-a3c8-11da-a746-0800200b9a66',
    Volume: '93a60a56-a3c8-22da-a746-0800200c9a66',
    'Number of items': '01846770-4cfe-4a25-8ad9-919d8d378345',
  };
  return {
    id,
    version,
    json_ordered: {
      flowDataSet: {
        flowInformation: {
          dataSetInformation: {
            'common:UUID': id,
            name: {
              baseName: [{ '@xml:lang': 'en', '#text': name }],
            },
          },
        },
        administrativeInformation: {
          publicationAndOwnership: {
            'common:dataSetVersion': version,
          },
        },
        flowProperties: {
          flowProperty: {
            '@dataSetInternalID': '0',
            referenceToFlowPropertyDataSet: {
              '@refObjectId': propertyIds[property],
              'common:shortDescription': { '@xml:lang': 'en', '#text': property },
            },
          },
        },
      },
    },
  };
}

test('buildFlowResolution reuses unique existing DB flow latest version and leaves patent-specific flow new', () => {
  const plan = {
    source: { id: 'CN123' },
    flows: {
      oxygen: { name_en: 'Oxygen', unit: 'kg' },
      product: { name_en: 'Patent-specific cathode matrix', unit: 'kg' },
    },
  };
  const uuids = { flows: { oxygen: 'new-oxygen', product: 'new-product' } };
  const scopeRows = [
    flowRow({ id: 'db-oxygen', version: '01.01.000', name: 'oxygen' }),
    flowRow({ id: 'db-oxygen', version: '01.01.002', name: 'Oxygen' }),
  ];

  const resolution = buildFlowResolution(plan, uuids, scopeRows);

  assert.equal(resolution.flows.oxygen.decision, 'reuse_existing');
  assert.equal(resolution.flows.oxygen.id, 'db-oxygen');
  assert.equal(resolution.flows.oxygen.version, '01.01.002');
  assert.equal(resolution.flows.product.decision, 'create_new');
  assert.equal(resolution.summary.reuse_existing, 1);
  assert.equal(resolution.summary.create_new, 1);
});

test('buildFlowResolution does not auto-pick ambiguous distinct DB flows', () => {
  const plan = {
    source: { id: 'CN123' },
    flows: {
      electricity: { name_en: 'Electricity, low voltage', unit: 'kWh' },
    },
  };
  const uuids = { flows: { electricity: 'new-electricity' } };
  const scopeRows = [
    flowRow({ id: 'db-electricity-cn', version: '01.01.000', name: 'Electricity, low voltage' }),
    flowRow({ id: 'db-electricity-glo', version: '01.01.000', name: 'Electricity, low voltage' }),
  ];

  const resolution = buildFlowResolution(plan, uuids, scopeRows);

  assert.equal(resolution.flows.electricity.decision, 'create_new');
  assert.equal(resolution.flows.electricity.reason, 'ambiguous_exact_name_match');
  assert.equal(resolution.review.length, 1);
  assert.equal(resolution.review[0].candidate_count, 2);
});

test('buildFlowResolution honors explicit existing DB flow refs from the plan', () => {
  const plan = {
    source: { id: 'CN123' },
    flows: {
      electricity: {
        name_en: 'Electricity, low voltage',
        unit: 'MJ',
        existing_flow_ref: {
          id: 'db-electricity-cn',
          version: '02.00.000',
          name: 'Electricity, low voltage',
          unit: 'kWh',
        },
      },
    },
  };
  const uuids = { flows: { electricity: 'new-electricity' } };
  const scopeRows = [
    flowRow({ id: 'db-electricity-cn', version: '01.01.000', name: 'Electricity, low voltage' }),
    flowRow({ id: 'db-electricity-glo', version: '01.01.000', name: 'Electricity, low voltage' }),
  ];

  const resolution = buildFlowResolution(plan, uuids, scopeRows);

  assert.equal(resolution.flows.electricity.decision, 'reuse_existing');
  assert.equal(resolution.flows.electricity.reason, 'plan_existing_flow_ref');
  assert.equal(resolution.flows.electricity.id, 'db-electricity-cn');
  assert.equal(resolution.flows.electricity.version, '02.00.000');
  assert.equal(resolution.flows.electricity.unit, 'kWh');
  assert.equal(resolution.flows.electricity.amount_factor, 0.2777777777777778);
  assert.equal(resolution.summary.reuse_existing, 1);
  assert.equal(resolution.review.length, 0);
});

test('applyFlowResolutionToExchange writes existing DB flow refs and unit conversions', () => {
  const exchange = {
    flow: 'water',
    amount: 1000,
  };
  const plan = {
    flows: {
      water: { name_en: 'Water', unit: 'g' },
    },
  };
  const resolution = {
    flows: {
      water: {
        decision: 'reuse_existing',
        id: 'db-water',
        version: '03.00.004',
        name: 'water',
        unit: 'kg',
        amount_factor: 0.001,
      },
    },
  };

  const result = applyFlowResolutionToExchange(exchange, 'Input', plan, resolution, {
    generatedFlowId: 'new-water',
    exchangeId: '0',
  });

  assert.equal(result.referenceToFlowDataSet['@refObjectId'], 'db-water');
  assert.equal(result.referenceToFlowDataSet['@version'], '03.00.004');
  assert.equal(result.referenceUnit, 'kg');
  assert.equal(result.meanAmount, 1);
  assert.match(result['common:generalComment'][0]['#text'], /converted from g to kg/u);
});
