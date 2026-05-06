import fs from 'node:fs';
import path from 'node:path';

const FLOW_VERSION = '01.00.000';

const FLOW_PROPERTIES = {
  mass: {
    id: '93a60a56-a3c8-11da-a746-0800200b9a66',
    uri: '../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml',
    name: 'Mass',
  },
  volume: {
    id: '93a60a56-a3c8-22da-a746-0800200c9a66',
    uri: '../flowproperties/93a60a56-a3c8-22da-a746-0800200c9a66.xml',
    name: 'Volume',
  },
  energy: {
    id: '93a60a56-a3c8-11da-a746-0800200c9a66',
    uri: '../flowproperties/93a60a56-a3c8-11da-a746-0800200c9a66_03.00.003.xml',
    version: '03.00.003',
    name: 'Net calorific value',
  },
  item: {
    id: '01846770-4cfe-4a25-8ad9-919d8d378345',
    uri: '../flowproperties/01846770-4cfe-4a25-8ad9-919d8d378345.xml',
    version: '00.00.000',
    name: 'Number of items',
  },
};

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function unitGroup(unit) {
  const normalized = String(unit || '').trim().toLowerCase();
  if (['l', 'liter', 'litre', 'm3', 'm^3'].includes(normalized)) return 'volume';
  if (['kwh', 'mj', 'gj', 'j'].includes(normalized)) return 'energy';
  if (['item', 'items', 'pcs', 'piece', 'pieces'].includes(normalized)) return 'item';
  return 'mass';
}

function langList(en, zh) {
  return [
    { '@xml:lang': 'en', '#text': en },
    ...(zh ? [{ '@xml:lang': 'zh', '#text': zh }] : []),
  ];
}

function flowPropertyReference(unit) {
  const property = FLOW_PROPERTIES[unitGroup(unit)];
  return {
    '@uri': property.uri,
    '@type': 'flow property data set',
    ...(property.version ? { '@version': property.version } : {}),
    '@refObjectId': property.id,
    'common:shortDescription': { '@xml:lang': 'en', '#text': property.name },
  };
}

function buildFlowDataset(flowKey, flow, uuid, plan) {
  const nameEn = flow?.name_en || flow?.name || flowKey;
  const nameZh = flow?.name_zh || '';
  const unit = flow?.unit || plan?.goal?.functional_unit?.unit || 'kg';
  const comment = [
    `Patent-derived flow for ${plan?.source?.id || 'source'}.`,
    `Reference unit used by process exchanges: ${unit}.`,
    flow?.description || '',
  ]
    .filter(Boolean)
    .join(' ');

  return {
    flowDataSet: {
      '@xmlns': 'http://lca.jrc.it/ILCD/Flow',
      '@xmlns:common': 'http://lca.jrc.it/ILCD/Common',
      '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      '@version': '1.1',
      '@xsi:schemaLocation': 'http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd',
      flowInformation: {
        dataSetInformation: {
          'common:UUID': uuid,
          name: {
            baseName: langList(nameEn, nameZh),
            mixAndLocationTypes: [],
            flowProperties: [],
            treatmentStandardsRoutes: [],
          },
          'common:generalComment': [{ '@xml:lang': 'en', '#text': comment }],
          classificationInformation: {
            'common:classification': {
              'common:class': [
                { '@level': '0', '#text': 'Patent-derived flows' },
                { '@level': '1', '#text': plan?.source?.id || 'Patent source' },
              ],
            },
          },
        },
        quantitativeReference: {
          referenceToReferenceFlowProperty: '0',
        },
      },
      modellingAndValidation: {
        LCIMethod: {
          typeOfDataSet: unitGroup(unit) === 'item' ? 'Other flow' : 'Product flow',
        },
      },
      administrativeInformation: {
        dataEntryBy: {
          'common:timeStamp': new Date(0).toISOString(),
        },
        publicationAndOwnership: {
          'common:dataSetVersion': FLOW_VERSION,
          'common:permanentDataSetURI': `https://local.tiangong.invalid/flows/${uuid}`,
          'common:referenceToOwnershipOfDataSet': {
            '@type': 'contact data set',
            '@refObjectId': plan?.source?.assignee || plan?.source?.id || 'patent-source',
            '@version': '01.00.000',
            'common:shortDescription': [
              { '@xml:lang': 'en', '#text': plan?.source?.assignee || plan?.source?.id || 'Patent source' },
            ],
          },
        },
      },
      flowProperties: {
        flowProperty: {
          '@dataSetInternalID': '0',
          meanValue: '1.0',
          referenceToFlowPropertyDataSet: flowPropertyReference(unit),
          generalComment: [{ '@xml:lang': 'en', '#text': `Reference unit: ${unit}` }],
        },
      },
    },
  };
}

export function buildPatentFlowRowsFromPlan(plan, uuids, options = {}) {
  const resolution = options.resolution || null;
  return Object.entries(plan?.flows || {})
    .sort(([left], [right]) => left.localeCompare(right))
    .filter(([flowKey]) => resolution?.flows?.[flowKey]?.decision !== 'reuse_existing')
    .map(([flowKey, flow]) => {
      const id = resolution?.flows?.[flowKey]?.id || uuids?.flows?.[flowKey];
      if (!id) {
        throw new Error(`Missing UUID for flow ${flowKey}`);
      }
      return {
        id,
        version: FLOW_VERSION,
        json_ordered: buildFlowDataset(flowKey, flow, id, plan),
      };
    });
}

export function writePatentFlowExports(base, combinedRunName, plan, uuids, options = {}) {
  const exportDir = path.join(base, 'runs', combinedRunName, 'exports', 'flows');
  fs.rmSync(exportDir, { recursive: true, force: true });
  const rows = buildPatentFlowRowsFromPlan(plan, uuids, options);
  rows.forEach((row) => {
    writeJson(path.join(exportDir, `${row.id}_${row.version}.json`), row.json_ordered);
  });
  return { exportDir, rows };
}

function payloadIdentity(payload) {
  const dataset = payload?.flowDataSet || payload || {};
  return {
    id: dataset?.flowInformation?.dataSetInformation?.['common:UUID'] || '',
    version:
      dataset?.administrativeInformation?.publicationAndOwnership?.['common:dataSetVersion'] ||
      FLOW_VERSION,
  };
}

export function listPatentFlowExportFiles(base) {
  const runsDir = path.join(base, 'runs');
  if (!fs.existsSync(runsDir)) return [];
  const files = [];
  for (const runName of fs.readdirSync(runsDir).sort()) {
    const exportsDir = path.join(runsDir, runName, 'exports', 'flows');
    if (!fs.existsSync(exportsDir)) continue;
    for (const fileName of fs.readdirSync(exportsDir).sort()) {
      if (fileName.endsWith('.json')) {
        files.push(path.join(exportsDir, fileName));
      }
    }
  }
  return files;
}

export function writePatentFlowPublishRowsFile(base, filePath) {
  const rows = listPatentFlowExportFiles(base).map((flowFile) => {
    const payload = JSON.parse(fs.readFileSync(flowFile, 'utf8'));
    const identity = payloadIdentity(payload);
    return {
      id: identity.id,
      version: identity.version,
      json_ordered: payload,
      source_file: path.relative(base, flowFile),
    };
  });
  writeJson(filePath, rows);
  return { path: filePath, rows };
}

export function isExistingFlowPreflightOnlyReport(report) {
  const reports = Array.isArray(report?.flow_reports) ? report.flow_reports : [];
  if (reports.length === 0) return false;
  return reports.every((item) => {
    if (item?.status !== 'failed') return true;
    const errors = Array.isArray(item.error) ? item.error : [];
    return (
      errors.length > 0 &&
      errors.every((error) =>
        ['target_user_id_required', 'exact_version_visible_not_owned'].includes(error?.code),
      )
    );
  });
}
