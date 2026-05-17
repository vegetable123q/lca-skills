import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  applyPatentAdministrativeMetadataToDataset,
  applyPatentPublishMetadataToBundle,
  buildPatentSourceDataset,
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
  assert.deepEqual(dataSetInformation.name.baseName, desc('NCM cathode model'));
  assert.equal(dataSetInformation.referenceYear, '2023');
  assert.equal(dataSetInformation.patentSource.source_id, 'CN123');
  assert.match(
    dataSetInformation['common:generalComment'][0]['#text'],
    /CN123.*Example Battery Co.*2023/u,
  );
});

test('publish metadata preserves lifecyclemodel name details and fills intended applications', () => {
  const bundlePath = writeBundleFixture({
    payload: {
      lifeCycleModelDataSet: {
        lifeCycleModelInformation: {
          dataSetInformation: {
            'common:UUID': 'model-id',
            name: {
              baseName: desc('Old model name'),
              treatmentStandardsRoutes: desc('Sol-gel and calcination route'),
              functionalUnitFlowProperties: desc('1 kg product'),
            },
          },
          technology: { processes: { processInstance: processInstance('1', 'process-a', 'Named process') } },
        },
        administrativeInformation: {
          'common:commissionerAndGoal': {
            'common:referenceToCommissioner': { '@refObjectId': 'contact-id' },
          },
        },
        modellingAndValidation: {
          dataSourcesTreatmentEtc: {
            useAdviceForDataSet: desc('Generic model advice'),
          },
        },
      },
    },
    manifest: {
      basic_info: {
        name: 'NCM cathode model',
        geography: 'CN',
        boundary: 'cradle-to-gate',
        source: { source_type: 'patent', source_id: 'CN123' },
      },
    },
    prefix: 'ptl-publish-required-fields-',
  });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  const model = entry.json_ordered.lifeCycleModelDataSet;
  const name = model.lifeCycleModelInformation.dataSetInformation.name;
  const commissionerAndGoal = model.administrativeInformation['common:commissionerAndGoal'];

  assert.deepEqual(name.baseName, desc('NCM cathode model'));
  assert.deepEqual(name.treatmentStandardsRoutes, desc('Sol-gel and calcination route'));
  assert.deepEqual(name.functionalUnitFlowProperties, desc('1 kg product'));
  assert.match(name.mixAndLocationTypes[0]['#text'], /CN/u);
  assert.equal(
    commissionerAndGoal['common:intendedApplications'][0]['#text'],
    '基于专利的生命周期建模',
  );
  assert.equal(
    model.modellingAndValidation.dataSourcesTreatmentEtc.useAdviceForDataSet[0]['#text'],
    '基于专利的生命周期建模',
  );
  assert.equal(
    commissionerAndGoal['common:referenceToCommissioner']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
});

test('publish metadata fills missing lifecyclemodel name details', () => {
  const bundlePath = writeBundleFixture({
    payload: modelPayload(processInstance('1', 'process-a', 'Named process'), {
      dataSetInformation: {
        'common:UUID': 'model-id',
        name: {
          baseName: desc('Old model name'),
          treatmentStandardsRoutes: [],
          mixAndLocationTypes: [],
        },
      },
    }),
    manifest: {
      basic_info: {
        name: 'NCM cathode model',
        source: { source_type: 'patent', source_id: 'CN123' },
      },
    },
    prefix: 'ptl-publish-required-field-defaults-',
  });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  const name =
    entry.json_ordered.lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation.name;

  assert.equal(name.treatmentStandardsRoutes[0]['#text'], '基于专利的生命周期建模路线');
  assert.equal(name.mixAndLocationTypes[0]['#text'], '专利路线');
});

test('publish metadata replaces scale-only lifecyclemodel mix and location', () => {
  const bundlePath = writeBundleFixture({
    payload: modelPayload(processInstance('1', 'process-a', 'Named process'), {
      dataSetInformation: {
        'common:UUID': 'model-id',
        name: {
          baseName: desc('Old model name'),
          mixAndLocationTypes: desc('lab'),
        },
      },
    }),
    manifest: {
      basic_info: {
        name: 'NCM cathode model',
        geography: 'CN',
        boundary: 'cradle-to-gate',
        source: { source_type: 'patent', source_id: 'CN123' },
      },
    },
    prefix: 'ptl-publish-mix-location-defaults-',
  });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  const name =
    entry.json_ordered.lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation.name;

  assert.equal(name.mixAndLocationTypes[0]['#text'], 'CN；cradle-to-gate；专利路线');
  assert.equal(name.mixAndLocationTypes[1]['#text'], 'CN; cradle-to-gate; patent-derived route');
});

test('publish metadata applies Wang Boxiang administrative contact to lifecyclemodels', () => {
  const payload = modelPayload(processInstance('1', 'process-a', 'Named process'), {
    dataSetInformation: { 'common:UUID': 'model-id' },
  });
  payload.lifeCycleModelDataSet.modellingAndValidation = {
    validation: {
      review: {
        'common:referenceToNameOfReviewerAndInstitution': {
          '@refObjectId': 'f549fef9-eb86-40a9-846e-2e95854971d1',
          '@type': 'contact data set',
          '@uri': '../contacts/f549fef9-eb86-40a9-846e-2e95854971d1_01.00.000.xml',
          '@version': '01.00.000',
        },
      },
    },
  };

  const bundlePath = writeBundleFixture({
    payload,
    manifest: {
      basic_info: {
        name: 'NCM cathode model',
        source: { source_type: 'patent', source_id: 'CN123' },
      },
    },
    prefix: 'ptl-publish-admin-contact-',
  });

  applyPatentPublishMetadataToBundle(bundlePath);

  const entry = JSON.parse(fs.readFileSync(bundlePath, 'utf8')).lifecyclemodels[0];
  const admin = entry.json_ordered.lifeCycleModelDataSet.administrativeInformation;
  const commissioner =
    admin['common:commissionerAndGoal']['common:referenceToCommissioner'];
  const dataEntry =
    admin.dataEntryBy['common:referenceToPersonOrEntityEnteringTheData'];
  const generator =
    admin.dataGenerator['common:referenceToPersonOrEntityGeneratingTheDataSet'];
  const owner = admin.publicationAndOwnership['common:referenceToOwnershipOfDataSet'];
  const reviewer =
    entry.json_ordered.lifeCycleModelDataSet.modellingAndValidation.validation.review[
      'common:referenceToNameOfReviewerAndInstitution'
    ];

  assert.equal(commissioner['@refObjectId'], '1ed5e71c-3ec3-4666-b0fc-9167b60c8056');
  assert.equal(dataEntry['@refObjectId'], '1ed5e71c-3ec3-4666-b0fc-9167b60c8056');
  assert.equal(generator['@refObjectId'], '1ed5e71c-3ec3-4666-b0fc-9167b60c8056');
  assert.equal(owner['@refObjectId'], '1ed5e71c-3ec3-4666-b0fc-9167b60c8056');
  assert.equal(reviewer['@refObjectId'], '1ed5e71c-3ec3-4666-b0fc-9167b60c8056');
  assert.equal(admin.publicationAndOwnership['common:licenseType'], 'Other');
});

test('patent source dataset uses publishable source identity and canonical references', () => {
  const payload = buildPatentSourceDataset({
    source: {
      id: 'CN108123128A',
      title: 'Surface-layer Al-doped NCM cathode material',
      assignee: 'Example Battery Co',
      publication_date: '2018-07-01',
      extra_metadata: {
        url: 'https://patents.google.com/patent/CN108123128A/en',
      },
    },
    sourceUuid: 'f549fef9-eb86-40a9-846e-2e95854971d1',
    now: new Date('2026-05-13T00:00:00Z'),
  });

  const source = payload.sourceDataSet;
  const info = source.sourceInformation.dataSetInformation;
  const admin = source.administrativeInformation;

  assert.equal(info['common:UUID'], 'f549fef9-eb86-40a9-846e-2e95854971d1');
  assert.equal(info.referenceToDigitalFile['@uri'], 'https://patents.google.com/patent/CN108123128A/en');
  assert.equal(
    admin.dataEntryBy['common:referenceToDataSetFormat']['@refObjectId'],
    'a97a0155-0234-4b87-b4ce-a45da52f2a40',
  );
  assert.equal(admin.dataEntryBy['common:referenceToDataSetFormat']['@version'], '03.00.003');
  assert.equal(
    admin.publicationAndOwnership['common:referenceToOwnershipOfDataSet']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
});

test('administrative metadata normalization supports process and flow datasets', () => {
  const processPayload = {
    processDataSet: {
      processInformation: {
        dataSetInformation: {
          name: {
            baseName: desc('Calcination process'),
            mixAndLocationTypes: desc('lab'),
          },
        },
        quantitativeReference: { '@type': 'Reference flow(s)', referenceToReferenceFlow: '1' },
        geography: { locationOfOperationSupplyOrProduction: { '@location': 'CN' } },
      },
      administrativeInformation: {
        dataEntryBy: {
          'common:referenceToDataSetFormat': {
            '@refObjectId': 'a97a0155-0234-4b87-b4ce-a45da52f2a40',
            '@type': 'source data set',
            '@uri': '../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_01.00.000.xml',
            '@version': '01.00.000',
          },
        },
        publicationAndOwnership: { 'common:dataSetVersion': '00.00.001' },
      },
      modellingAndValidation: {
        complianceDeclarations: {
          compliance: {
            'common:referenceToComplianceSystem': {
              '@refObjectId': 'patent-based-lifecycle-modeling-compliance',
              '@type': 'source data set',
              '@uri': '../sources/patent-based-lifecycle-modeling-compliance_01.00.000.xml',
              '@version': '01.00.000',
            },
          },
        },
      },
      exchanges: {
        exchange: [
          exchange('1', 'Output', 'flow-product', 'Calcined product', {
            quantitativeReference: true,
          }),
        ],
      },
    },
  };
  const flowPayload = {
    flowDataSet: {
      flowInformation: {
        dataSetInformation: {
          name: {
            baseName: desc('Calcined product'),
            treatmentStandardsRoutes: [],
            mixAndLocationTypes: [],
          },
          classificationInformation: {
            'common:classification': {
              'common:class': [
                { '@level': '0', '#text': 'Patent-derived flows' },
                { '@level': '1', '#text': 'CN123' },
              ],
            },
          },
        },
      },
      administrativeInformation: {
        dataEntryBy: {
          'common:referenceToDataSetFormat': {
            '@refObjectId': 'a97a0155-0234-4b87-b4ce-a45da52f2a40',
            '@version': '01.00.000',
          },
        },
        publicationAndOwnership: { 'common:dataSetVersion': '01.00.000' },
      },
    },
  };

  applyPatentAdministrativeMetadataToDataset(processPayload, { commissioner: true });
  applyPatentAdministrativeMetadataToDataset(flowPayload);

  const processAdmin = processPayload.processDataSet.administrativeInformation;
  const flowAdmin = flowPayload.flowDataSet.administrativeInformation;
  const processName = processPayload.processDataSet.processInformation.dataSetInformation.name;
  const flowName = flowPayload.flowDataSet.flowInformation.dataSetInformation.name;

  assert.equal(
    processAdmin['common:commissionerAndGoal']['common:referenceToCommissioner']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(
    processAdmin.dataEntryBy['common:referenceToPersonOrEntityEnteringTheData']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(
    processAdmin.dataGenerator['common:referenceToPersonOrEntityGeneratingTheDataSet'][
      '@refObjectId'
    ],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(
    processAdmin.publicationAndOwnership['common:referenceToOwnershipOfDataSet']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(processAdmin.publicationAndOwnership['common:licenseType'], 'Other');
  assert.equal(processName.treatmentStandardsRoutes[0]['#text'], '基于专利的工艺路线');
  assert.equal(processName.mixAndLocationTypes[0]['#text'], 'CN；lab；专利工艺');
  assert.equal(processName.functionalUnitFlowProperties[0]['#text'], '参考流：Calcined product');
  assert.equal(processPayload.processDataSet['@xmlns'], 'http://lca.jrc.it/ILCD/Process');
  assert.equal(processPayload.processDataSet['@xmlns:common'], 'http://lca.jrc.it/ILCD/Common');
  assert.equal(processPayload.processDataSet['@xmlns:xsi'], 'http://www.w3.org/2001/XMLSchema-instance');
  assert.equal(processPayload.processDataSet['@version'], '1.1');
  assert.equal(processPayload.processDataSet['@locations'], '../ILCDLocations.xml');
  assert.equal(
    processPayload.processDataSet.processInformation.time['common:referenceYear'],
    new Date().getUTCFullYear(),
  );
  assert.equal(
    processPayload.processDataSet.modellingAndValidation.validation.review['@type'],
    'Not reviewed',
  );
  assert.equal(
    processPayload.processDataSet.modellingAndValidation.complianceDeclarations.compliance[
      'common:approvalOfOverallCompliance'
    ],
    'Not defined',
  );
  assert.equal(
    processPayload.processDataSet.modellingAndValidation.complianceDeclarations.compliance[
      'common:referenceToComplianceSystem'
    ]['@refObjectId'],
    'd92a1a12-2545-49e2-a585-55c259997756',
  );
  assert.equal(
    processPayload.processDataSet.modellingAndValidation.complianceDeclarations.compliance[
      'common:referenceToComplianceSystem'
    ]['@version'],
    '20.20.002',
  );
  assert.equal(
    processAdmin.dataEntryBy['common:referenceToDataSetFormat']['@version'],
    '03.00.003',
  );
  assert.equal(
    processPayload.processDataSet.exchanges.exchange[0].referenceToFlowDataSet['@uri'],
    '../flows/flow-product_00.00.001.xml',
  );
  assert.equal(processPayload.processDataSet.exchanges.exchange[0].meanAmount, '1');
  assert.equal(
    flowAdmin.dataEntryBy['common:referenceToPersonOrEntityEnteringTheData']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(
    flowAdmin.dataEntryBy['common:referenceToDataSetFormat']['@version'],
    '03.00.003',
  );
  assert.equal(
    flowAdmin.dataGenerator['common:referenceToPersonOrEntityGeneratingTheDataSet'][
      '@refObjectId'
    ],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(
    flowAdmin.publicationAndOwnership['common:referenceToOwnershipOfDataSet']['@refObjectId'],
    '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  );
  assert.equal(flowAdmin.publicationAndOwnership['common:licenseType'], 'Other');
  assert.equal(flowName.treatmentStandardsRoutes[0]['#text'], '基于专利的流数据');
  assert.equal(flowName.mixAndLocationTypes[0]['#text'], '专利派生流');
  assert.equal(
    flowPayload.flowDataSet.flowInformation.dataSetInformation.classificationInformation[
      'common:classification'
    ]['common:class'][0]['@classId'],
    'patent-derived-flows',
  );
  assert.equal(
    flowPayload.flowDataSet.flowInformation.dataSetInformation.classificationInformation[
      'common:classification'
    ]['common:class'][1]['@classId'],
    'cn123',
  );
});
