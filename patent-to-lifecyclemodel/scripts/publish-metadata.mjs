import fs from 'node:fs';
import path from 'node:path';

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function listify(value) {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function text(value) {
  if (typeof value === 'string') return value.trim();
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = text(item);
      if (found) return found;
    }
    return '';
  }
  if (isRecord(value)) {
    if (typeof value['#text'] === 'string') return value['#text'].trim();
    if (isRecord(value.baseName) || Array.isArray(value.baseName)) {
      return text(value.baseName);
    }
    for (const nested of Object.values(value)) {
      const found = text(nested);
      if (found) return found;
    }
  }
  return '';
}

function root(payload) {
  return isRecord(payload.lifeCycleModelDataSet) ? payload.lifeCycleModelDataSet : payload;
}

function compactParts(parts) {
  return parts.filter((part) => typeof part === 'string' && part.trim()).map((part) => part.trim());
}

function langText(entries, lang) {
  return listify(entries)
    .filter(isRecord)
    .find((entry) => entry['@xml:lang'] === lang && text(entry))?.['#text'];
}

function localizedText(zh, en) {
  return [
    { '@xml:lang': 'zh', '#text': zh },
    { '@xml:lang': 'en', '#text': en },
  ];
}

const ADMIN_CONTACT = {
  id: '1ed5e71c-3ec3-4666-b0fc-9167b60c8056',
  version: '01.01.000',
  nameEn: 'Wang Boxiang',
  nameZh: '王博翔',
};

const DATA_FORMAT_REFERENCE = {
  '@refObjectId': 'a97a0155-0234-4b87-b4ce-a45da52f2a40',
  '@type': 'source data set',
  '@uri': '../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml',
  '@version': '03.00.003',
  'common:shortDescription': localizedText('ILCD 格式', 'ILCD format'),
};

const COMPLIANCE_REFERENCE = {
  '@refObjectId': 'd92a1a12-2545-49e2-a585-55c259997756',
  '@type': 'source data set',
  '@uri': '../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml',
  '@version': '20.20.002',
  'common:shortDescription': localizedText('ILCD 数据网络入门级', 'ILCD Data Network - Entry-level'),
};

function referencesMatch(actual, expected) {
  if (!isRecord(actual)) return false;
  return ['@refObjectId', '@type', '@uri', '@version'].every((key) => text(actual[key]) === text(expected[key]));
}

function setReference(target, key, expected) {
  if (referencesMatch(target[key], expected)) return false;
  target[key] = clone(expected);
  return true;
}

function adminContactReference() {
  return {
    '@refObjectId': ADMIN_CONTACT.id,
    '@type': 'contact data set',
    '@uri': `../contacts/${ADMIN_CONTACT.id}_${ADMIN_CONTACT.version}.xml`,
    '@version': ADMIN_CONTACT.version,
    'common:shortDescription': localizedText(ADMIN_CONTACT.nameZh, ADMIN_CONTACT.nameEn),
  };
}

function sourceClassification(classId, label) {
  return {
    'common:classification': {
      'common:class': {
        '@level': '0',
        '@classId': classId,
        '#text': label,
      },
    },
  };
}

function validUri(value) {
  const candidate = text(value);
  if (!candidate) return null;
  try {
    return new URL(candidate).toString();
  } catch {
    return null;
  }
}

export function buildPatentSourceDataset({ source = {}, sourceUuid, now = new Date() } = {}) {
  const id = text(sourceUuid);
  if (!id) {
    throw new Error('buildPatentSourceDataset requires sourceUuid');
  }
  const sourceId = text(source.id) || text(source.source_id) || id;
  const title = text(source.title) || sourceId;
  const citation = compactParts([
    sourceId,
    title,
    source.assignee ? `assignee: ${text(source.assignee)}` : '',
    source.publication_date ? `publication: ${text(source.publication_date)}` : '',
    source.priority_date ? `priority: ${text(source.priority_date)}` : '',
  ]).join('; ');
  const digitalUri =
    validUri(source.url) ||
    validUri(source.google_patents_url) ||
    validUri(source.pdf_url) ||
    validUri(source.extra_metadata?.url);

  return {
    sourceDataSet: {
      '@xmlns:common': 'http://lca.jrc.it/ILCD/Common',
      '@xmlns': 'http://lca.jrc.it/ILCD/Source',
      '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
      '@version': '1.1',
      '@xsi:schemaLocation': 'http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd',
      sourceInformation: {
        dataSetInformation: {
          'common:UUID': id,
          'common:shortName': localizedText(sourceId, title),
          classificationInformation: sourceClassification('patent', 'Patent source'),
          sourceCitation: citation || sourceId,
          publicationType: 'Other unpublished and grey literature',
          sourceDescriptionOrComment: localizedText(
            `专利来源：${sourceId}。${title}`,
            `Patent source: ${sourceId}. ${title}`,
          ),
          ...(digitalUri ? { referenceToDigitalFile: { '@uri': digitalUri } } : {}),
        },
      },
      administrativeInformation: {
        dataEntryBy: {
          'common:timeStamp': now.toISOString(),
          'common:referenceToDataSetFormat': clone(DATA_FORMAT_REFERENCE),
        },
        publicationAndOwnership: {
          'common:dataSetVersion': '01.00.000',
          'common:permanentDataSetURI': `urn:uuid:${id}`,
          'common:referenceToOwnershipOfDataSet': adminContactReference(),
        },
      },
    },
  };
}

function ensureLocalizedField(target, key, zh, en) {
  if (text(target[key])) return false;
  target[key] = localizedText(zh, en);
  return true;
}

function isWeakLifecyclemodelMixAndLocation(value) {
  const entries = listify(value);
  if (entries.length === 0) return true;
  const values = entries.map((entry) => text(entry).toLowerCase()).filter(Boolean);
  if (values.length === 0) return true;
  return values.every((entry) => ['lab', 'laboratory', 'pilot', 'industrial'].includes(entry));
}

function ensurePatentProcessNameFields(processDataset) {
  const processInformation = isRecord(processDataset.processInformation)
    ? processDataset.processInformation
    : {};
  processDataset.processInformation = processInformation;
  const dataSetInformation = isRecord(processInformation.dataSetInformation)
    ? processInformation.dataSetInformation
    : {};
  processInformation.dataSetInformation = dataSetInformation;
  const name = isRecord(dataSetInformation.name) ? dataSetInformation.name : {};
  dataSetInformation.name = name;

  let changed = false;
  changed =
    ensureLocalizedField(name, 'treatmentStandardsRoutes', '基于专利的工艺路线', 'Patent-derived process route') ||
    changed;

  const geography = text(processInformation.geography?.locationOfOperationSupplyOrProduction?.['@location']);
  const existingMix = text(name.mixAndLocationTypes);
  if (isWeakLifecyclemodelMixAndLocation(name.mixAndLocationTypes)) {
    const mixZh = compactParts([geography, existingMix, '专利工艺']).join('；') || '专利工艺';
    const mixEn = compactParts([geography, existingMix, 'patent-derived process']).join('; ') || 'patent-derived process';
    name.mixAndLocationTypes = localizedText(mixZh, mixEn);
    changed = true;
  }

  if (!text(name.functionalUnitFlowProperties)) {
    const referenceFlowId = text(processInformation.quantitativeReference?.referenceToReferenceFlow);
    const exchanges = listify(processDataset.exchanges?.exchange).filter(isRecord);
    const referenceExchange =
      exchanges.find((entry) => text(entry['@dataSetInternalID']) === referenceFlowId) ||
      exchanges.find((entry) => entry.quantitativeReference === true || text(entry.quantitativeReference) === '1') ||
      exchanges.find((entry) => text(entry.exchangeDirection).toLowerCase() === 'output') ||
      null;
    const referenceFlow = text(referenceExchange?.referenceToFlowDataSet?.['common:shortDescription']);
    const unit = text(referenceExchange?.referenceUnit);
    const flowText = referenceFlow || text(name.baseName) || 'reference output';
    name.functionalUnitFlowProperties = localizedText(
      `参考流：${flowText}`,
      compactParts(['Reference flow', flowText, unit ? `unit ${unit}` : '']).join('; '),
    );
    changed = true;
  }

  return changed;
}

function ensureFourLevelClassification(processDataset) {
  const dataSetInformation = processDataset.processInformation?.dataSetInformation;
  if (!isRecord(dataSetInformation)) return false;
  const classificationInformation = isRecord(dataSetInformation.classificationInformation)
    ? dataSetInformation.classificationInformation
    : {};
  dataSetInformation.classificationInformation = classificationInformation;
  const classification = isRecord(classificationInformation['common:classification'])
    ? classificationInformation['common:classification']
    : {};
  classificationInformation['common:classification'] = classification;

  const existing = listify(classification['common:class']).filter(isRecord);
  const defaults = [
    'Materials production',
    'Battery materials',
    'Patent-derived process',
    'Patent-derived process',
  ];
  let changed = false;
  const classes = Array.from({ length: 4 }, (_, index) => {
    const prior = existing[index] || {};
    const label = text(prior) || defaults[index];
    const classId = text(prior['@classId']) || `patent-process-${index}`;
    if (!text(prior['@classId']) || text(prior['@level']) !== String(index) || !text(prior)) {
      changed = true;
    }
    return {
      ...prior,
      '@level': String(index),
      '@classId': classId,
      '#text': label,
    };
  });
  if (existing.length !== 4) changed = true;
  classification['common:class'] = classes;
  return changed;
}

function normalizeYear(value, fallback) {
  const numberValue = Number.parseInt(text(value), 10);
  return Number.isFinite(numberValue) && numberValue > 0 ? numberValue : fallback;
}

function stableClassId(value, fallback) {
  const normalized = text(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-+|-+$/gu, '');
  return normalized || fallback;
}

function ensurePatentProcessSchemaFields(processDataset) {
  let changed = false;
  const nowYear = new Date().getUTCFullYear();

  const requiredRootAttributes = {
    '@xmlns:common': 'http://lca.jrc.it/ILCD/Common',
    '@xmlns': 'http://lca.jrc.it/ILCD/Process',
    '@xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    '@version': '1.1',
    '@locations': '../ILCDLocations.xml',
    '@xsi:schemaLocation': 'http://lca.jrc.it/ILCD/Process ../schemas/ILCD_ProcessDataSet.xsd',
  };
  for (const [key, value] of Object.entries(requiredRootAttributes)) {
    if (processDataset[key] !== value) {
      processDataset[key] = value;
      changed = true;
    }
  }

  changed = ensureFourLevelClassification(processDataset) || changed;

  const processInformation = isRecord(processDataset.processInformation)
    ? processDataset.processInformation
    : {};
  processDataset.processInformation = processInformation;
  const timeInfo = isRecord(processInformation.time) ? processInformation.time : {};
  processInformation.time = timeInfo;
  const referenceYear = normalizeYear(timeInfo['common:referenceYear'], nowYear);
  const validUntil = normalizeYear(timeInfo['common:dataSetValidUntil'], Math.max(referenceYear, 2030));
  if (timeInfo['common:referenceYear'] !== referenceYear) {
    timeInfo['common:referenceYear'] = referenceYear;
    changed = true;
  }
  if (timeInfo['common:dataSetValidUntil'] !== validUntil) {
    timeInfo['common:dataSetValidUntil'] = validUntil;
    changed = true;
  }

  const modellingAndValidation = isRecord(processDataset.modellingAndValidation)
    ? processDataset.modellingAndValidation
    : {};
  processDataset.modellingAndValidation = modellingAndValidation;
  const method = isRecord(modellingAndValidation.LCIMethodAndAllocation)
    ? modellingAndValidation.LCIMethodAndAllocation
    : {};
  modellingAndValidation.LCIMethodAndAllocation = method;
  if (Array.isArray(method.LCIMethodApproaches)) {
    method.LCIMethodApproaches = text(method.LCIMethodApproaches) || 'Allocation - mass';
    changed = true;
  }
  const representativeness = isRecord(
    modellingAndValidation.dataSourcesTreatmentAndRepresentativeness,
  )
    ? modellingAndValidation.dataSourcesTreatmentAndRepresentativeness
    : {};
  modellingAndValidation.dataSourcesTreatmentAndRepresentativeness = representativeness;
  changed =
    ensureLocalizedField(
      representativeness,
      'dataCutOffAndCompletenessPrinciples',
      '基于专利公开文本和可获得技术参数建模；未披露参数按保守默认值补齐。',
      'Modelled from patent disclosure and available technical parameters; undisclosed parameters are filled with conservative defaults.',
    ) || changed;
  if (!isRecord(modellingAndValidation.validation)) {
    modellingAndValidation.validation = { review: { '@type': 'Not reviewed' } };
    changed = true;
  } else if (!isRecord(modellingAndValidation.validation.review)) {
    modellingAndValidation.validation.review = { '@type': 'Not reviewed' };
    changed = true;
  } else if (text(modellingAndValidation.validation.review['@type']) !== 'Not reviewed') {
    modellingAndValidation.validation.review['@type'] = 'Not reviewed';
    changed = true;
  }
  const compliance = {
    'common:referenceToComplianceSystem': clone(COMPLIANCE_REFERENCE),
    'common:approvalOfOverallCompliance': 'Not defined',
    'common:nomenclatureCompliance': 'Not defined',
    'common:methodologicalCompliance': 'Not defined',
    'common:reviewCompliance': 'Not defined',
    'common:documentationCompliance': 'Not defined',
    'common:qualityCompliance': 'Not defined',
  };
  if (!isRecord(modellingAndValidation.complianceDeclarations)) {
    modellingAndValidation.complianceDeclarations = { compliance };
    changed = true;
  } else if (!isRecord(modellingAndValidation.complianceDeclarations.compliance)) {
    modellingAndValidation.complianceDeclarations.compliance = compliance;
    changed = true;
  } else {
    for (const [key, value] of Object.entries(compliance)) {
      if (key === 'common:referenceToComplianceSystem') {
        changed = setReference(modellingAndValidation.complianceDeclarations.compliance, key, value) || changed;
      } else if (!text(modellingAndValidation.complianceDeclarations.compliance[key])) {
        modellingAndValidation.complianceDeclarations.compliance[key] = clone(value);
        changed = true;
      }
    }
  }

  const administrativeInformation = isRecord(processDataset.administrativeInformation)
    ? processDataset.administrativeInformation
    : {};
  processDataset.administrativeInformation = administrativeInformation;
  const dataEntryBy = isRecord(administrativeInformation.dataEntryBy)
    ? administrativeInformation.dataEntryBy
    : {};
  administrativeInformation.dataEntryBy = dataEntryBy;
  changed = setReference(dataEntryBy, 'common:referenceToDataSetFormat', DATA_FORMAT_REFERENCE) || changed;
  const publicationAndOwnership = isRecord(administrativeInformation.publicationAndOwnership)
    ? administrativeInformation.publicationAndOwnership
    : {};
  administrativeInformation.publicationAndOwnership = publicationAndOwnership;
  const processId = text(processDataset.processInformation?.dataSetInformation?.['common:UUID']);
  if (!text(publicationAndOwnership['common:permanentDataSetURI']) && processId) {
    publicationAndOwnership['common:permanentDataSetURI'] = `urn:uuid:${processId}`;
    changed = true;
  }
  if (!text(publicationAndOwnership['common:copyright'])) {
    publicationAndOwnership['common:copyright'] = 'false';
    changed = true;
  }

  for (const exchange of listify(processDataset.exchanges?.exchange).filter(isRecord)) {
    const flowReference = isRecord(exchange.referenceToFlowDataSet)
      ? exchange.referenceToFlowDataSet
      : {};
    exchange.referenceToFlowDataSet = flowReference;
    const flowId = text(flowReference['@refObjectId']);
    const flowVersion = text(flowReference['@version']) || '00.00.001';
    if (flowId && !text(flowReference['@uri'])) {
      flowReference['@uri'] = `../flows/${flowId}_${flowVersion}.xml`;
      changed = true;
    }
    for (const key of ['meanAmount', 'resultingAmount', 'minimumAmount', 'maximumAmount']) {
      if (typeof exchange[key] === 'number') {
        exchange[key] = String(exchange[key]);
        changed = true;
      }
    }
  }

  return changed;
}

function ensurePatentFlowNameFields(flowDataset) {
  const flowInformation = isRecord(flowDataset.flowInformation) ? flowDataset.flowInformation : {};
  flowDataset.flowInformation = flowInformation;
  const dataSetInformation = isRecord(flowInformation.dataSetInformation)
    ? flowInformation.dataSetInformation
    : {};
  flowInformation.dataSetInformation = dataSetInformation;
  const name = isRecord(dataSetInformation.name) ? dataSetInformation.name : {};
  dataSetInformation.name = name;

  let changed = false;
  changed =
    ensureLocalizedField(name, 'treatmentStandardsRoutes', '基于专利的流数据', 'Patent-derived flow dataset') ||
    changed;
  if (isWeakLifecyclemodelMixAndLocation(name.mixAndLocationTypes)) {
    name.mixAndLocationTypes = localizedText('专利派生流', 'patent-derived flow');
    changed = true;
  }

  const classificationInformation = isRecord(dataSetInformation.classificationInformation)
    ? dataSetInformation.classificationInformation
    : {};
  dataSetInformation.classificationInformation = classificationInformation;
  const classification = isRecord(classificationInformation['common:classification'])
    ? classificationInformation['common:classification']
    : {};
  classificationInformation['common:classification'] = classification;
  const existingClasses = listify(classification['common:class']).filter(isRecord);
  const classes = existingClasses.length
    ? existingClasses
    : [
        { '@level': '0', '#text': 'Patent-derived flows' },
        { '@level': '1', '#text': 'Patent-based lifecycle modeling' },
      ];
  for (const [index, entry] of classes.entries()) {
    if (!text(entry['@level'])) {
      entry['@level'] = String(index);
      changed = true;
    }
    if (!text(entry['@classId'])) {
      entry['@classId'] = stableClassId(entry['#text'], `patent-flow-${index}`);
      changed = true;
    }
  }
  classification['common:class'] = classes;

  return changed;
}

function ensurePatentLifecyclemodelNameFields(dataSetInformation, manifest) {
  const name = isRecord(dataSetInformation.name) ? dataSetInformation.name : {};
  dataSetInformation.name = name;
  let changed = false;

  changed =
    ensureLocalizedField(
      name,
      'treatmentStandardsRoutes',
      '基于专利的生命周期建模路线',
      'Patent-based lifecycle modeling route',
    ) || changed;

  const geography = text(manifest?.basic_info?.geography) || text(manifest?.basic_info?.source?.jurisdiction);
  const boundary = text(manifest?.basic_info?.boundary) || text(manifest?.goal?.boundary);
  const mixZh = compactParts([geography, boundary, '专利路线']).join('；') || '专利路线';
  const mixEn = compactParts([geography, boundary, 'patent-derived route']).join('; ') || 'patent-derived route';
  if (isWeakLifecyclemodelMixAndLocation(name.mixAndLocationTypes)) {
    name.mixAndLocationTypes = localizedText(mixZh, mixEn);
    changed = true;
  }

  return changed;
}

function ensurePatentIntendedApplications(model) {
  const administrativeInformation = isRecord(model.administrativeInformation)
    ? model.administrativeInformation
    : {};
  model.administrativeInformation = administrativeInformation;

  const commissionerAndGoal = isRecord(administrativeInformation['common:commissionerAndGoal'])
    ? administrativeInformation['common:commissionerAndGoal']
    : {};
  administrativeInformation['common:commissionerAndGoal'] = commissionerAndGoal;

  if (text(commissionerAndGoal['common:intendedApplications'])) return false;
  commissionerAndGoal['common:intendedApplications'] = localizedText(
    '基于专利的生命周期建模',
    'Patent-based lifecycle modeling',
  );
  return true;
}

function normalizeReviewContact(datasetRoot, contact) {
  const review = datasetRoot.modellingAndValidation?.validation?.review;
  if (!isRecord(review) || !isRecord(review['common:referenceToNameOfReviewerAndInstitution'])) {
    return false;
  }
  review['common:referenceToNameOfReviewerAndInstitution'] = clone(contact);
  return true;
}

export function applyPatentAdministrativeMetadataToDataset(payload, options = {}) {
  const datasetRoot =
    (isRecord(payload?.lifeCycleModelDataSet) && payload.lifeCycleModelDataSet) ||
    (isRecord(payload?.processDataSet) && payload.processDataSet) ||
    (isRecord(payload?.flowDataSet) && payload.flowDataSet) ||
    payload;
  if (!isRecord(datasetRoot)) return false;

  const administrativeInformation = isRecord(datasetRoot.administrativeInformation)
    ? datasetRoot.administrativeInformation
    : {};
  datasetRoot.administrativeInformation = administrativeInformation;

  let changed = false;
  const contact = adminContactReference();

  const commissionerAndGoal = isRecord(administrativeInformation['common:commissionerAndGoal'])
    ? administrativeInformation['common:commissionerAndGoal']
    : {};
  administrativeInformation['common:commissionerAndGoal'] = commissionerAndGoal;
  commissionerAndGoal['common:referenceToCommissioner'] = clone(contact);
  changed = true;
  if (options.commissioner !== false) {
    changed = ensurePatentIntendedApplications(datasetRoot) || changed;
  }

  const dataEntryBy = isRecord(administrativeInformation.dataEntryBy)
    ? administrativeInformation.dataEntryBy
    : {};
  administrativeInformation.dataEntryBy = dataEntryBy;
  dataEntryBy['common:referenceToPersonOrEntityEnteringTheData'] = clone(contact);
  changed = setReference(dataEntryBy, 'common:referenceToDataSetFormat', DATA_FORMAT_REFERENCE) || changed;
  changed = true;

  const dataGenerator = isRecord(administrativeInformation.dataGenerator)
    ? administrativeInformation.dataGenerator
    : {};
  administrativeInformation.dataGenerator = dataGenerator;
  dataGenerator['common:referenceToPersonOrEntityGeneratingTheDataSet'] = clone(contact);
  changed = true;

  const publicationAndOwnership = isRecord(administrativeInformation.publicationAndOwnership)
    ? administrativeInformation.publicationAndOwnership
    : {};
  administrativeInformation.publicationAndOwnership = publicationAndOwnership;
  publicationAndOwnership['common:referenceToOwnershipOfDataSet'] = clone(contact);
  publicationAndOwnership['common:licenseType'] = 'Other';
  changed = true;
  changed = normalizeReviewContact(datasetRoot, contact) || changed;

  if (datasetRoot === payload?.processDataSet || isRecord(datasetRoot.processInformation)) {
    changed = ensurePatentProcessNameFields(datasetRoot) || changed;
    changed = ensurePatentProcessSchemaFields(datasetRoot) || changed;
  }
  if (datasetRoot === payload?.flowDataSet || isRecord(datasetRoot.flowInformation)) {
    changed = ensurePatentFlowNameFields(datasetRoot) || changed;
  }

  return changed;
}

export function applyPatentAdministrativeMetadataToJsonFile(filePath, options = {}) {
  const resolved = path.resolve(filePath);
  const payload = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  const changed = applyPatentAdministrativeMetadataToDataset(payload, options);
  if (changed) fs.writeFileSync(resolved, `${JSON.stringify(payload, null, 2)}\n`);
  return { path: resolved, changed };
}

export function applyPatentAdministrativeMetadataToRowsFile(filePath, options = {}) {
  const resolved = path.resolve(filePath);
  const rows = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  let changed = false;
  for (const row of listify(rows).filter(isRecord)) {
    if (isRecord(row.json_ordered)) {
      changed = applyPatentAdministrativeMetadataToDataset(row.json_ordered, options) || changed;
    }
    if (isRecord(row.json)) {
      changed = applyPatentAdministrativeMetadataToDataset(row.json, options) || changed;
    }
  }
  if (changed) fs.writeFileSync(resolved, `${JSON.stringify(rows, null, 2)}\n`);
  return { path: resolved, changed };
}

function ensurePatentUseAdvice(model) {
  const modellingAndValidation = isRecord(model.modellingAndValidation)
    ? model.modellingAndValidation
    : {};
  model.modellingAndValidation = modellingAndValidation;

  const dataSourcesTreatmentEtc = isRecord(modellingAndValidation.dataSourcesTreatmentEtc)
    ? modellingAndValidation.dataSourcesTreatmentEtc
    : {};
  modellingAndValidation.dataSourcesTreatmentEtc = dataSourcesTreatmentEtc;

  const expected = '基于专利的生命周期建模';
  if (langText(dataSourcesTreatmentEtc.useAdviceForDataSet, 'zh') === expected) return false;
  dataSourcesTreatmentEtc.useAdviceForDataSet = localizedText(
    expected,
    'Patent-based lifecycle modeling',
  );
  return true;
}

function patentSourceSummary(source) {
  if (!isRecord(source)) return '';
  return compactParts([
    source.source_id ? `patent ${source.source_id}` : '',
    source.assignee ? `assignee ${source.assignee}` : '',
    source.year ? `year ${source.year}` : '',
    source.priority_date ? `priority ${source.priority_date}` : '',
    source.publication_date ? `publication ${source.publication_date}` : '',
    source.grant_date ? `grant ${source.grant_date}` : '',
    source.inventor ? `inventor ${source.inventor}` : '',
  ]).join('; ');
}

function patentSourceComment(manifest) {
  const source = manifest?.basic_info?.source;
  const summary = patentSourceSummary(source);
  if (!summary) return null;
  return {
    '@xml:lang': 'en',
    '#text': `Patent source metadata: ${summary}.`,
  };
}

function appendUniqueGeneralComment(target, comment) {
  if (!comment) return false;
  const existing = listify(target['common:generalComment']).filter(isRecord);
  const commentText = text(comment);
  const withoutPriorPatentMetadata = existing.filter(
    (entry) => !text(entry).startsWith('Patent source metadata:'),
  );
  if (withoutPriorPatentMetadata.some((entry) => text(entry) === commentText)) {
    target['common:generalComment'] = withoutPriorPatentMetadata;
    return false;
  }
  target['common:generalComment'] = [...withoutPriorPatentMetadata, comment];
  return true;
}

function applyPatentBasicInfoToLifecyclemodelPayload(payload, manifest) {
  if (!isRecord(manifest?.basic_info)) return false;
  const model = root(payload);
  let changed = applyPatentAdministrativeMetadataToDataset(model);
  const info = model.lifeCycleModelInformation || {};
  model.lifeCycleModelInformation = info;
  const dataSetInformation = info.dataSetInformation || {};
  info.dataSetInformation = dataSetInformation;
  changed = appendUniqueGeneralComment(dataSetInformation, patentSourceComment(manifest)) || changed;
  const modelName = text(manifest.basic_info.name);
  if (modelName) {
    const existingName = isRecord(dataSetInformation.name) ? dataSetInformation.name : {};
    dataSetInformation.name = {
      ...existingName,
      baseName: [{ '@xml:lang': 'en', '#text': modelName }],
    };
    changed = true;
  }
  changed = ensurePatentLifecyclemodelNameFields(dataSetInformation, manifest) || changed;
  changed = ensurePatentIntendedApplications(model) || changed;
  changed = ensurePatentUseAdvice(model) || changed;
  if (manifest.basic_info.reference_year && !text(dataSetInformation.referenceYear)) {
    dataSetInformation.referenceYear = String(manifest.basic_info.reference_year);
    changed = true;
  }
  dataSetInformation.patentSource = clone(manifest.basic_info.source || {});
  return true;
}

function processInstances(payload) {
  const model = root(payload);
  const tech = model.lifeCycleModelInformation?.technology || {};
  return listify(tech.processes?.processInstance).filter(isRecord);
}

function processLabel(reference, fallback) {
  return text(reference.name) || text(reference['common:shortDescription']) || fallback;
}

function localizedDescription(reference, fallback) {
  const shortDescription = reference['common:shortDescription'];
  if (shortDescription !== undefined) {
    const cloned = clone(shortDescription);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  const name = reference.name;
  if (name !== undefined) {
    const cloned = clone(name);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  return [{ '@xml:lang': 'en', '#text': fallback }];
}

function localizedName(reference, fallback) {
  if (isRecord(reference.name)) {
    return clone(reference.name);
  }
  return { baseName: localizedDescription(reference, fallback) };
}

function referenceProcessInternalId(payload) {
  const model = root(payload);
  return text(
    model.lifeCycleModelInformation?.quantitativeReference?.referenceToReferenceProcess,
  );
}

function matchingResultingProcessType(instance, payload) {
  const model = root(payload);
  const referenceProcessInternalIdValue = referenceProcessInternalId(payload);
  const instanceInternalId = text(instance['@dataSetInternalID']);
  if (referenceProcessInternalIdValue && instanceInternalId === referenceProcessInternalIdValue) {
    return 'primary';
  }

  const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
  const resulting =
    model.lifeCycleModelInformation?.dataSetInformation?.referenceToResultingProcess || {};
  const processId = text(reference['@refObjectId']);
  const processVersion = text(reference['@version']);
  const resultingId = text(resulting['@refObjectId']);
  const resultingVersion = text(resulting['@version']);
  return processId &&
    processId === resultingId &&
    (!resultingVersion || processVersion === resultingVersion)
    ? 'primary'
    : 'secondary';
}

export function buildPatentLifecyclemodelJsonTg(payload, options = {}) {
  return buildPatentLifecyclemodelJsonTgWithOptions(payload, options);
}

function processDatasetRoot(payload) {
  return isRecord(payload?.processDataSet) ? payload.processDataSet : payload;
}

function processIdFromPayload(payload) {
  const rootPayload = processDatasetRoot(payload);
  return text(rootPayload?.processInformation?.dataSetInformation?.['common:UUID']);
}

function exchangeFlowId(exchange) {
  return text(exchange?.referenceToFlowDataSet?.['@refObjectId']);
}

function exchangeLabel(exchange, fallback) {
  return text(exchange?.referenceToFlowDataSet?.['common:shortDescription']) || fallback;
}

function exchangeShortDescription(exchange, fallback) {
  const description = exchange?.referenceToFlowDataSet?.['common:shortDescription'];
  if (description !== undefined) {
    const cloned = clone(description);
    return Array.isArray(cloned) ? cloned : [cloned];
  }
  return [{ '@xml:lang': 'en', '#text': fallback }];
}

function buildProcessExchangeIndex(processPayloads = []) {
  const byProcessId = new Map();
  for (const payload of processPayloads.filter(isRecord)) {
    const processId = processIdFromPayload(payload);
    if (!processId) continue;
    const rootPayload = processDatasetRoot(payload);
    const exchanges = listify(rootPayload?.exchanges?.exchange).filter(isRecord);
    const summary = {
      byDirectionAndFlow: new Map(),
      exchanges: [],
      exchangeCount: exchanges.length,
    };
    for (const exchange of exchanges) {
      const flowId = exchangeFlowId(exchange);
      const direction = text(exchange.exchangeDirection);
      if (!flowId || !direction) continue;
      const flowName = exchangeLabel(exchange, flowId);
      const shortDescription = exchangeShortDescription(exchange, flowName);
      const flowVersion = text(exchange.referenceToFlowDataSet?.['@version']);
      const exchangeSummary = {
        exchangeInternalId: text(exchange['@dataSetInternalID']),
        flowId,
        direction,
        name: flowName,
        shortDescription,
        referenceUnit: text(exchange.referenceUnit),
        meanAmount: exchange.meanAmount,
        flowVersion,
        allocations: clone(exchange.allocations || { allocation: {} }),
        quantitativeReference:
          exchange.quantitativeReference === true || text(exchange.quantitativeReference) === '1',
      };
      summary.exchanges.push(exchangeSummary);
    }
    const portCounts = new Map();
    for (const exchange of summary.exchanges) {
      const directionCode = exchange.direction.toUpperCase();
      const countKey = `${directionCode}\u0000${exchange.flowId}`;
      const count = portCounts.get(countKey) || 0;
      portCounts.set(countKey, count + 1);
      exchange.portId =
        count === 0
          ? `${directionCode}:${exchange.flowId}`
          : `${directionCode}:${exchange.flowId}:${exchange.exchangeInternalId || count + 1}`;
    }
    for (const exchange of summary.exchanges) {
      const key = `${exchange.direction}\u0000${exchange.flowId}`;
      const current = summary.byDirectionAndFlow.get(key);
      if (!current || (!current.quantitativeReference && exchange.quantitativeReference)) {
        summary.byDirectionAndFlow.set(key, exchange);
      }
    }
    const current = byProcessId.get(processId);
    if (!current || summary.exchangeCount >= current.exchangeCount) {
      byProcessId.set(processId, summary);
    }
  }
  return byProcessId;
}

function findExchangeSummary(index, processId, direction, flowId) {
  return index.get(processId)?.byDirectionAndFlow.get(`${direction}\u0000${flowId}`) || null;
}

function portGroups() {
  return {
    groupInput: {
      attrs: {
        text: { fill: 'rgba(0,0,0,0.45)', fontSize: 14 },
        portBody: {
          x: -4,
          y: -4,
          fill: '#fff',
          width: 8,
          height: 8,
          magnet: true,
          stroke: '#5c246a',
          strokeWidth: 1,
        },
      },
      label: { position: { name: 'right' } },
      markup: [{ tagName: 'rect', selector: 'portBody' }],
      position: { name: 'absolute' },
    },
    groupOutput: {
      attrs: {
        text: { fill: 'rgba(0,0,0,0.45)', fontSize: 14 },
        portBody: {
          x: -4,
          y: -4,
          fill: '#fff',
          width: 8,
          height: 8,
          magnet: true,
          stroke: '#5c246a',
          strokeWidth: 1,
        },
      },
      label: { position: { name: 'left' } },
      markup: [{ tagName: 'rect', selector: 'portBody' }],
      position: { name: 'absolute' },
    },
  };
}

function truncateForGraph(value, maxLength = 28) {
  const normalized = text(value);
  return normalized.length > maxLength ? `${normalized.slice(0, maxLength)}...` : normalized;
}

function buildPort(exchange, indexByDirection) {
  const directionCode = exchange.direction.toUpperCase();
  const isInput = directionCode === 'INPUT';
  const y = 65 + indexByDirection * 20;
  const title = exchange.name;
  const allocationFraction = text(exchange.allocations?.allocation?.['@allocatedFraction']);
  const prefix = allocationFraction && !isInput ? `[${allocationFraction}] ` : '';
  const display = `${prefix}${title}`;
  return {
    id: exchange.portId || `${directionCode}:${exchange.flowId}`,
    args: { x: isInput ? 0 : '100%', y },
    data: {
      flowId: exchange.flowId,
      textLang: clone(exchange.shortDescription),
      ...(exchange.flowVersion ? { flowVersion: exchange.flowVersion } : {}),
      allocations: clone(exchange.allocations),
      quantitativeReference: exchange.quantitativeReference,
    },
    attrs: {
      text: {
        fill:
          exchange.quantitativeReference ||
          (allocationFraction && Number.parseFloat(allocationFraction) > 0)
            ? '#5c246a'
            : 'rgba(0,0,0,0.45)',
        text: truncateForGraph(display, 28),
        title: display,
        cursor: 'pointer',
        'font-weight': exchange.quantitativeReference ? 'bold' : 'normal',
      },
    },
    group: isInput ? 'groupInput' : 'groupOutput',
    tools: [{ id: 'portTool' }],
  };
}

function buildPorts(processExchangeIndex, processId) {
  const summary = processExchangeIndex.get(processId);
  const exchanges = summary?.exchanges || [];
  const directionCounts = new Map();
  const items = exchanges.map((exchange) => {
    const directionCode = exchange.direction.toUpperCase();
    const index = directionCounts.get(directionCode) || 0;
    directionCounts.set(directionCode, index + 1);
    return buildPort(exchange, index);
  });
  return { items, groups: portGroups() };
}

function buildPatentLifecyclemodelJsonTgWithOptions(payload, options = {}) {
  const instances = processInstances(payload);
  const processExchangeIndex = buildProcessExchangeIndex(options.processPayloads || []);
  const referenceProcessInternalIdValue = referenceProcessInternalId(payload);
  const columnCount = Math.max(1, Math.ceil(Math.sqrt(Math.max(instances.length, 1))));
  const nodes = instances.map((instance, index) => {
    const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
    const internalId = text(instance['@dataSetInternalID']) || `node-${index + 1}`;
    const processId = text(reference['@refObjectId']) || `process-${index + 1}`;
    const version = text(reference['@version']);
    const label = processLabel(reference, processId);
    const ports = buildPorts(processExchangeIndex, processId);
    const sidePortCount = Math.max(
      ports.items.filter((port) => port.group === 'groupInput').length,
      ports.items.filter((port) => port.group === 'groupOutput').length,
    );
    const height = Math.max(100, 55 + sidePortCount * 20);
    const isReferenceProcess =
      referenceProcessInternalIdValue && internalId === referenceProcessInternalIdValue;
    return {
      id: internalId,
      x: (index % columnCount) * 420,
      y: Math.floor(index / columnCount) * 180,
      width: 350,
      height,
      size: { width: 350, height },
      shape: 'rect',
      ports,
      data: {
        id: processId,
        index: String(index),
        ...(version ? { version } : {}),
        label: localizedName(reference, label),
        labelText: label,
        name: label,
        nameText: label,
        shortDescription: localizedDescription(reference, label),
        displayName: localizedDescription(reference, label),
        quantitativeReference: isReferenceProcess ? '1' : '0',
      },
    };
  });

  const nodeByInternalId = Object.fromEntries(
    nodes.map((node) => [
      node.id,
      {
        processId: node.data.id,
        version: node.data.version || null,
      },
    ]),
  );
  const edges = [];
  instances.forEach((instance, index) => {
    const sourceCell = text(instance['@dataSetInternalID']) || `node-${index + 1}`;
    const sourceNode = nodeByInternalId[sourceCell] || {};
    for (const outputExchange of listify(instance.connections?.outputExchange).filter(isRecord)) {
      const flowUuid = text(outputExchange['@flowUUID']);
      for (const downstreamProcess of listify(outputExchange.downstreamProcess).filter(isRecord)) {
        const targetCell = text(downstreamProcess['@id']);
        if (!targetCell) continue;
        const targetNode = nodeByInternalId[targetCell] || {};
        const outputFlow = findExchangeSummary(
          processExchangeIndex,
          sourceNode.processId,
          'Output',
          flowUuid,
        );
        const inputFlow = findExchangeSummary(
          processExchangeIndex,
          targetNode.processId,
          'Input',
          text(downstreamProcess['@flowUUID']) || flowUuid,
        );
        const flowLabel = outputFlow?.name || inputFlow?.name || flowUuid;
        edges.push({
          id: [sourceCell, targetCell, flowUuid || `edge-${edges.length + 1}`].join(':'),
          source: {
            cell: sourceCell,
            ...(outputFlow?.portId
              ? { port: outputFlow.portId }
              : flowUuid
                ? { port: `OUTPUT:${flowUuid}` }
                : {}),
          },
          target: {
            cell: targetCell,
            ...(inputFlow?.portId
              ? { port: inputFlow.portId }
              : flowUuid
                ? { port: `INPUT:${text(downstreamProcess['@flowUUID']) || flowUuid}` }
                : {}),
          },
          shape: 'edge',
          attrs: { line: { stroke: '#5c246a', strokeWidth: 1 } },
          labels: flowLabel ? [{ text: flowLabel }] : [],
          data: {
            connection: {
              outputExchange: {
                ...(flowUuid ? { '@flowUUID': flowUuid } : {}),
                ...(outputFlow?.shortDescription?.length
                  ? { 'common:shortDescription': outputFlow.shortDescription }
                  : {}),
                downstreamProcess: clone(downstreamProcess),
              },
            },
            flow: {
              id: flowUuid || inputFlow?.flowId || outputFlow?.flowId,
              name: flowLabel,
              output: outputFlow
                ? {
                    processId: sourceNode.processId,
                    exchangeInternalId: outputFlow.exchangeInternalId,
                    flowId: outputFlow.flowId,
                    name: outputFlow.name,
                    referenceUnit: outputFlow.referenceUnit,
                    meanAmount: outputFlow.meanAmount,
                  }
                : null,
              input: inputFlow
                ? {
                    processId: targetNode.processId,
                    exchangeInternalId: inputFlow.exchangeInternalId,
                    flowId: inputFlow.flowId,
                    name: inputFlow.name,
                    referenceUnit: inputFlow.referenceUnit,
                    meanAmount: inputFlow.meanAmount,
                  }
                : null,
            },
            node: {
              sourceNodeID: sourceCell,
              targetNodeID: targetCell,
              sourceProcessId: sourceNode.processId,
              ...(sourceNode.version ? { sourceProcessVersion: sourceNode.version } : {}),
              ...(targetNode.processId ? { targetProcessId: targetNode.processId } : {}),
              ...(targetNode.version ? { targetProcessVersion: targetNode.version } : {}),
            },
          },
        });
      }
    }
  });

  const submodels = instances
    .map((instance, index) => {
      const reference = isRecord(instance.referenceToProcess) ? instance.referenceToProcess : {};
      const id = text(reference['@refObjectId']);
      if (!id) return null;
      const version = text(reference['@version']);
      return {
        id,
        ...(version ? { version } : {}),
        type: matchingResultingProcessType(instance, payload),
        name: processLabel(reference, id),
        instanceId: text(instance['@dataSetInternalID']) || `instance-${index + 1}`,
      };
    })
    .filter(Boolean);

  return {
    xflow: { nodes, edges },
    submodels,
  };
}

export { buildPatentLifecyclemodelJsonTgWithOptions };

function loadLifecyclemodelPayload(entry, bundleDir) {
  if (isRecord(entry.json_ordered)) return entry.json_ordered;
  if (isRecord(entry.jsonOrdered)) return entry.jsonOrdered;
  if (isRecord(entry.json)) return entry.json;
  if (entry.file) {
    const filePath = path.resolve(bundleDir, String(entry.file));
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  }
  return null;
}

export function applyPatentPublishMetadataToBundle(bundlePath) {
  const resolved = path.resolve(bundlePath);
  const bundle = JSON.parse(fs.readFileSync(resolved, 'utf8'));
  const bundleDir = path.dirname(resolved);
  const baseDir = path.dirname(bundleDir);
  const manifestPath = path.join(baseDir, 'manifests', 'lifecyclemodel-manifest.json');
  const manifest = fs.existsSync(manifestPath)
    ? JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
    : null;
  const processPayloads = [];
  const runsDir = path.join(baseDir, 'runs');
  if (fs.existsSync(runsDir)) {
    for (const runName of fs.readdirSync(runsDir).sort()) {
      const exportsDir = path.join(runsDir, runName, 'exports', 'processes');
      if (!fs.existsSync(exportsDir)) continue;
      for (const fileName of fs.readdirSync(exportsDir).sort()) {
        if (!fileName.endsWith('.json')) continue;
        processPayloads.push(JSON.parse(fs.readFileSync(path.join(exportsDir, fileName), 'utf8')));
      }
    }
  }
  let changed = false;

  for (const entry of listify(bundle.lifecyclemodels).filter(isRecord)) {
    const payload = loadLifecyclemodelPayload(entry, bundleDir);
    if (!payload) continue;
    delete entry.file;
    delete entry.path;
    if (manifest?.basic_info) {
      applyPatentBasicInfoToLifecyclemodelPayload(payload, manifest);
      entry.basic_info = clone(manifest.basic_info);
    }
    entry.json_ordered = payload;
    entry.json_tg = buildPatentLifecyclemodelJsonTgWithOptions(payload, { processPayloads });
    changed = true;
  }
  for (const entry of listify(bundle.projected_processes).filter(isRecord)) {
    const payload = loadLifecyclemodelPayload(entry, bundleDir);
    if (!payload) continue;
    delete entry.file;
    delete entry.path;
    applyPatentAdministrativeMetadataToDataset(payload);
    entry.json_ordered = payload;
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(resolved, `${JSON.stringify(bundle, null, 2)}\n`);
  }
  return { bundlePath: resolved, changed };
}
