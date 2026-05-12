import fs from 'node:fs';
import path from 'node:path';

const MASS_PROPERTY_ID = '93a60a56-a3c8-11da-a746-0800200b9a66';
const VOLUME_PROPERTY_ID = '93a60a56-a3c8-22da-a746-0800200c9a66';
const ITEM_PROPERTY_ID = '01846770-4cfe-4a25-8ad9-919d8d378345';

const UNIT_TO_BASE = {
  kg: { group: 'mass', factor: 1 },
  g: { group: 'mass', factor: 0.001 },
  mg: { group: 'mass', factor: 0.000001 },
  t: { group: 'mass', factor: 1000 },
  l: { group: 'volume', factor: 1 },
  L: { group: 'volume', factor: 1 },
  ml: { group: 'volume', factor: 0.001 },
  m3: { group: 'volume', factor: 1000 },
  'm^3': { group: 'volume', factor: 1000 },
  item: { group: 'item', factor: 1 },
  items: { group: 'item', factor: 1 },
  kWh: { group: 'energy', factor: 1 },
  MJ: { group: 'energy', factor: 0.2777777777777778 },
  J: { group: 'energy', factor: 2.7777777777777776e-7 },
};

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function listify(value) {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
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
    if (value.baseName !== undefined) return text(value.baseName);
    for (const nested of Object.values(value)) {
      const found = text(nested);
      if (found) return found;
    }
  }
  return '';
}

function texts(value) {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? [trimmed] : [];
  }
  if (Array.isArray(value)) {
    return value.flatMap((item) => texts(item));
  }
  if (isRecord(value)) {
    if (typeof value['#text'] === 'string') return texts(value['#text']);
    if (value.baseName !== undefined) return texts(value.baseName);
    return Object.values(value).flatMap((nested) => texts(nested));
  }
  return [];
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];
  for (const value of values) {
    const trimmed = typeof value === 'string' ? value.trim() : '';
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

function normalizeText(value) {
  return String(value || '')
    .replace(/[^0-9a-zA-Z\u4e00-\u9fff]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .trim()
    .toLowerCase();
}

const NEAREST_STOPWORDS = new Set([
  'active',
  'ammonium',
  'anhydrous',
  'anode',
  'cathode',
  'commercial',
  'consumed',
  'calcium',
  'dihydrate',
  'disodium',
  'electrolyte',
  'gaseous',
  'hexahydrate',
  'lithium',
  'material',
  'medium',
  'monohydrate',
  'molten',
  'sodium',
  'solid',
]);

const NEAREST_SINGLE_TOKEN_ALLOWED = new Set([
  'chloride',
  'hydroxide',
  'molybdate',
  'nitrate',
  'oxide',
  'phosphate',
  'sulfate',
]);

const CHEMICAL_FORM_TOKENS = new Set([
  'carbonate',
  'chloride',
  'fluoride',
  'hydroxide',
  'molybdate',
  'nitrate',
  'ore',
  'oxide',
  'phosphate',
  'sulfate',
]);

const STRICT_COUNTERION_FORM_TOKENS = new Set([
  'carbonate',
  'chloride',
  'fluoride',
  'hydroxide',
  'nitrate',
  'sulfate',
]);

const METAL_ELEMENT_TOKENS = new Set([
  'aluminum',
  'antimony',
  'barium',
  'beryllium',
  'cadmium',
  'cerium',
  'chromium',
  'cobalt',
  'copper',
  'gallium',
  'gold',
  'indium',
  'iron',
  'lanthanum',
  'lead',
  'magnesium',
  'manganese',
  'molybdenum',
  'neodymium',
  'nickel',
  'niobium',
  'palladium',
  'platinum',
  'praseodymium',
  'scandium',
  'silver',
  'strontium',
  'tin',
  'titanium',
  'tungsten',
  'vanadium',
  'yttrium',
  'zinc',
  'zirconium',
]);

const COUNTERION_TOKENS = new Set([
  ...METAL_ELEMENT_TOKENS,
  'ammonium',
  'lithium',
  'potassium',
  'sodium',
]);

const UNSUITABLE_METAL_INPUT_TOKENS = new Set([
  'based',
  'environmental',
  'remediation',
  'residue',
  'residual',
  'slag',
  'sludge',
  'spent',
  'waste',
]);

const GENERIC_SOURCE_FORM_TOKENS = new Set([
  'composite',
  'dopant',
  'precursor',
  'rich',
  'shell',
  'sol',
  'source',
]);

const INPUT_GAS_TOKENS = new Set([
  'air',
  'argon',
  'carbon',
  'co2',
  'dioxide',
  'gas',
  'gaseous',
  'hydrogen',
  'nitrogen',
  'oxygen',
]);

function normalizeChemicalText(value) {
  return String(value || '')
    .replace(/\((?:i|ii|iii|iv|v|vi|vii|viii|ix|x)\)/giu, ' ')
    .replace(/（(?:i|ii|iii|iv|v|vi|vii|viii|ix|x)）/giu, ' ')
    .replace(/\b(?:i|ii|iii|iv|v|vi|vii|viii|ix|x)\b/giu, ' ');
}

function nearestTokens(value) {
  return normalizeText(
    normalizeChemicalText(value)
      .replace(/金属镍/gu, 'nickel metal')
      .replace(/金属钴/gu, 'cobalt metal')
      .replace(/金属锰/gu, 'manganese metal')
      .replace(/\baluminium\b/giu, 'aluminum')
      .replace(/\bmetallic\b/giu, 'metal')
      .replace(/\bni\b/giu, 'nickel')
      .replace(/\bco\b/giu, 'cobalt')
      .replace(/\bmn\b/giu, 'manganese')
      .replace(/\bmeta\s*phosphate\b/giu, 'phosphate')
      .replace(/\bmetaphosphate\b/giu, 'phosphate')
      .replace(/\btetra\s*oxo\s*molybdate\b/giu, 'molybdate')
      .replace(/\btetraoxomolybdate\b/giu, 'molybdate')
      .replace(/\bortho\s*molybdate\b/giu, 'molybdate')
      .replace(/\borthomolybdate\b/giu, 'molybdate'),
  )
    .split(' ')
    .map((part) => part.trim())
    .filter((part) => part && !NEAREST_STOPWORDS.has(part));
}

function nearestTokensWithCounterions(value) {
  return normalizeText(
    normalizeChemicalText(value)
      .replace(/金属镍/gu, 'nickel metal')
      .replace(/金属钴/gu, 'cobalt metal')
      .replace(/金属锰/gu, 'manganese metal')
      .replace(/\baluminium\b/giu, 'aluminum')
      .replace(/\bmetallic\b/giu, 'metal')
      .replace(/\bni\b/giu, 'nickel')
      .replace(/\bco\b/giu, 'cobalt')
      .replace(/\bmn\b/giu, 'manganese')
      .replace(/\bmeta\s*phosphate\b/giu, 'phosphate')
      .replace(/\bmetaphosphate\b/giu, 'phosphate')
      .replace(/\btetra\s*oxo\s*molybdate\b/giu, 'molybdate')
      .replace(/\btetraoxomolybdate\b/giu, 'molybdate')
      .replace(/\bortho\s*molybdate\b/giu, 'molybdate')
      .replace(/\borthomolybdate\b/giu, 'molybdate'),
  )
    .split(' ')
    .map((part) => part.trim())
    .filter(Boolean);
}

function uniqueTokens(value) {
  return [...new Set(nearestTokens(value))];
}

function uniqueTokensWithCounterions(value) {
  return [...new Set(nearestTokensWithCounterions(value))];
}

function normalizeCandidateText(value) {
  return normalizeText(
    normalizeChemicalText(value)
      .replace(
        /\b(?:mono|di|tri|tetra|penta|hexa|hepta|octa|nona|deca)hydrate\b/giu,
        ' ',
      )
      .replace(/^[一二三四五六七八九十]水/u, ' '),
  );
}

function compareVersions(left, right) {
  const leftParts = String(left || '').split('.').map((part) => Number.parseInt(part, 10) || 0);
  const rightParts = String(right || '').split('.').map((part) => Number.parseInt(part, 10) || 0);
  const length = Math.max(leftParts.length, rightParts.length);
  for (let index = 0; index < length; index += 1) {
    const delta = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (delta !== 0) return delta;
  }
  return 0;
}

function datasetPayload(row) {
  if (isRecord(row?.json_ordered)) return row.json_ordered;
  if (isRecord(row?.jsonOrdered)) return row.jsonOrdered;
  if (isRecord(row?.json)) return row.json;
  return row;
}

function flowDataset(row) {
  const payload = datasetPayload(row);
  return isRecord(payload?.flowDataSet) ? payload.flowDataSet : payload;
}

function flowNamesFromDataset(dataset, fallback) {
  const info = isRecord(dataset?.flowInformation?.dataSetInformation)
    ? dataset.flowInformation.dataSetInformation
    : {};
  return uniqueStrings([
    ...texts(info.name?.baseName),
    ...texts(info.name),
    ...texts(info['common:shortDescription']),
    fallback,
  ]);
}

function flowTypeFromDataset(dataset) {
  const info = isRecord(dataset?.flowInformation?.dataSetInformation)
    ? dataset.flowInformation.dataSetInformation
    : {};
  const method = isRecord(dataset?.modellingAndValidation?.LCIMethod)
    ? dataset.modellingAndValidation.LCIMethod
    : {};
  return text(method.typeOfDataSet) || text(info.typeOfDataSet);
}

function classificationTextsFromDataset(dataset) {
  const info = isRecord(dataset?.flowInformation?.dataSetInformation)
    ? dataset.flowInformation.dataSetInformation
    : {};
  const classification = info.classificationInformation?.['common:classification'];
  return texts(classification?.['common:class']);
}

function propertyGroupFromDataset(dataset) {
  const property = listify(dataset?.flowProperties?.flowProperty)[0] || {};
  const ref = isRecord(property.referenceToFlowPropertyDataSet)
    ? property.referenceToFlowPropertyDataSet
    : {};
  const id = text(ref['@refObjectId']);
  const description = normalizeText(text(ref['common:shortDescription']));
  if (id === MASS_PROPERTY_ID || description.includes('mass')) return 'mass';
  if (id === VOLUME_PROPERTY_ID || description.includes('volume')) return 'volume';
  if (id === ITEM_PROPERTY_ID || description.includes('number of items')) return 'item';
  if (description.includes('energy') || description.includes('calorific')) return 'energy';
  return '';
}

function unitInfo(unit) {
  const raw = String(unit || '').trim();
  return UNIT_TO_BASE[raw] ?? UNIT_TO_BASE[raw.toLowerCase()] ?? { group: '', factor: 1 };
}

function referenceUnitForGroup(group, fallbackUnit) {
  if (group === 'mass') return 'kg';
  if (group === 'volume') return 'L';
  if (group === 'item') return 'item';
  if (group === 'energy') return 'kWh';
  return fallbackUnit || 'kg';
}

function amountFactor(sourceUnit, targetUnit) {
  const source = unitInfo(sourceUnit);
  const target = unitInfo(targetUnit);
  if (!source.group || !target.group || source.group !== target.group) return 1;
  return source.factor / target.factor;
}

function buildFlowSearchCandidates(flowNames) {
  const candidates = [];
  const seen = new Set();
  const add = (candidate) => {
    const normalizedName = candidate.normalizedName || normalizeText(candidate.name);
    if (!normalizedName) return;
    const key = `${normalizedName}\u0000${candidate.reason}`;
    if (seen.has(key)) return;
    seen.add(key);
    candidates.push({
      name: candidate.name,
      normalizedName,
      reason: candidate.reason || 'exact_name_match',
      autoReuse: candidate.autoReuse === true,
    });
  };

  for (const name of flowNames) {
    add({ name, reason: 'exact_name_match', autoReuse: true });
    const normalizedCandidate = normalizeCandidateText(name);
    const exactNormalized = normalizeText(name);
    if (normalizedCandidate && normalizedCandidate !== exactNormalized) {
      add({
        name,
        normalizedName: normalizedCandidate,
        reason: 'normalized_name_candidate',
        autoReuse: false,
      });
    }
  }

  return candidates;
}

function reasonRank(reason) {
  if (reason === 'exact_name_match') return 0;
  if (reason === 'normalized_name_candidate') return 1;
  return 2;
}

function compareCandidateMatches(left, right) {
  const reasonDelta = reasonRank(left.candidate.reason) - reasonRank(right.candidate.reason);
  if (reasonDelta !== 0) return reasonDelta;

  const leftState = Number(left.record.stateCode ?? -1);
  const rightState = Number(right.record.stateCode ?? -1);
  if (leftState !== rightState) return rightState - leftState;

  const versionDelta = compareVersions(right.record.version, left.record.version);
  if (versionDelta !== 0) return versionDelta;

  const leftModified = Date.parse(left.record.modifiedAt || '') || 0;
  const rightModified = Date.parse(right.record.modifiedAt || '') || 0;
  if (leftModified !== rightModified) return rightModified - leftModified;

  return left.record.id.localeCompare(right.record.id);
}

function compareNearestMatches(left, right) {
  const scoreDelta = (right.score ?? 0) - (left.score ?? 0);
  if (scoreDelta !== 0) return scoreDelta;
  return compareCandidateMatches(left, right);
}

function sharedMetalElementTokens(leftTokens, rightTokens) {
  const right = new Set(rightTokens);
  return leftTokens.filter((token) => METAL_ELEMENT_TOKENS.has(token) && right.has(token));
}

function hasSpecificMetalElement(tokens) {
  return tokens.some((token) => METAL_ELEMENT_TOKENS.has(token));
}

function chemicalFormTokens(tokens) {
  return tokens.filter((token) => CHEMICAL_FORM_TOKENS.has(token));
}

function strictCounterionForms(tokens) {
  return tokens.filter((token) => STRICT_COUNTERION_FORM_TOKENS.has(token));
}

function counterionTokens(tokens) {
  return tokens.filter((token) => COUNTERION_TOKENS.has(token));
}

function hasSharedToken(leftTokens, rightTokens) {
  const right = new Set(rightTokens);
  return leftTokens.some((token) => right.has(token));
}

function isGenericMetalOnly(tokens) {
  return tokens.length === 1 && tokens[0] === 'metal';
}

function nearestInputMatchScore(recordTokens, queryTokens) {
  if (recordTokens.length === 0 || queryTokens.size === 0) return null;
  const queryTokenList = [...queryTokens];
  const queryHasSpecificMetal = hasSpecificMetalElement(queryTokenList);
  const queryHasGenericSourceForm = queryTokenList.some((token) =>
    GENERIC_SOURCE_FORM_TOKENS.has(token),
  );
  const recordHasUnsuitableInputToken = recordTokens.some((token) =>
    UNSUITABLE_METAL_INPUT_TOKENS.has(token),
  );
  if (isGenericMetalOnly(recordTokens)) return null;
  if (queryHasGenericSourceForm) {
    return null;
  }
  if (queryHasSpecificMetal && recordHasUnsuitableInputToken) {
    return null;
  }
  if (recordTokens.includes('ore') && !queryTokens.has('ore')) {
    return null;
  }

  const sharedMetals = sharedMetalElementTokens(recordTokens, queryTokenList);
  const queryForms = chemicalFormTokens(queryTokenList);
  const recordForms = chemicalFormTokens(recordTokens);
  if (queryForms.length > 0 && recordForms.length === 0) {
    return null;
  }
  if (queryForms.length > 0 && recordForms.length > 0 && !hasSharedToken(queryForms, recordForms)) {
    return null;
  }
  if (
    queryHasSpecificMetal &&
    sharedMetals.length === 0 &&
    recordTokens.every((token) => CHEMICAL_FORM_TOKENS.has(token))
  ) {
    return null;
  }

  const subsetMatch = recordTokens.every((token) => queryTokens.has(token));
  if (subsetMatch) {
    const token = recordTokens[0];
    if (
      recordTokens.length === 1 &&
      !NEAREST_SINGLE_TOKEN_ALLOWED.has(token) &&
      !METAL_ELEMENT_TOKENS.has(token)
    ) {
      return null;
    }
    const metalBonus =
      queryHasSpecificMetal && sharedMetalElementTokens(recordTokens, queryTokenList).length > 0
        ? 1
        : 0;
    const formBonus = queryTokens.has('metal') && recordTokens.includes('metal') ? 0.25 : 0;
    return 1 + recordTokens.length / queryTokens.size + metalBonus + formBonus;
  }

  if (queryHasSpecificMetal && sharedMetals.length > 0) {
    let score = 1.5 + sharedMetals.length;
    if (queryTokens.has('metal') && recordTokens.includes('metal')) score += 0.5;
    if (recordTokens.includes('electrodeposit')) score += 0.25;
    if (queryTokens.has('metal') && recordTokens.some((token) => ['salt', 'sulfate'].includes(token))) {
      score -= 0.5;
    }
    if (queryTokens.has('metal') && recordTokens.includes('oxide')) score -= 0.35;
    if (queryTokens.has('metal') && recordTokens.includes('ore')) score -= 0.25;
    return score;
  }

  return null;
}

function simpleSaltCounterionsCompatible(recordName, queryName) {
  const recordTokens = uniqueTokensWithCounterions(recordName);
  const queryTokens = uniqueTokensWithCounterions(queryName);
  const sharedStrictForms = strictCounterionForms(recordTokens).filter((token) =>
    strictCounterionForms(queryTokens).includes(token),
  );
  if (sharedStrictForms.length === 0) return true;
  const recordCounterions = counterionTokens(recordTokens);
  const queryCounterions = counterionTokens(queryTokens);
  if (recordCounterions.length === 0 || queryCounterions.length === 0) return true;
  return hasSharedToken(recordCounterions, queryCounterions);
}

function collapseBestNearestMatchesById(matches) {
  const byId = new Map();
  for (const match of matches) {
    const current = byId.get(match.record.id);
    if (!current || compareNearestMatches(match, current) < 0) {
      byId.set(match.record.id, match);
    }
  }
  return [...byId.values()].sort(compareNearestMatches);
}

function buildReuseDecision(flowKey, unit, target, matchedCandidate, candidateCount, reason) {
  const factor = amountFactor(unit, target.unit);
  return {
    flow_key: flowKey,
    decision: 'reuse_existing',
    reason,
    id: target.id,
    version: target.version,
    name: target.name,
    unit: target.unit,
    amount_factor: factor,
    source_unit: unit,
    candidate_count: candidateCount,
  };
}

function buildCreateNewDecision(flowKey, flow, generatedId, reason, candidateCount = 0, candidates = []) {
  const name = flow?.name_en || flow?.name || flowKey;
  const unit = flow?.unit || 'kg';
  return {
    flow_key: flowKey,
    decision: 'create_new',
    reason,
    id: generatedId,
    version: '01.00.000',
    name,
    unit,
    amount_factor: 1,
    source_unit: unit,
    candidate_count: candidateCount,
    candidates,
  };
}

function explicitExistingFlowRef(flow) {
  const raw = flow?.existing_flow_ref || flow?.db_flow || flow?.database_flow;
  if (!isRecord(raw)) return null;
  const id = text(raw.id) || text(raw.uuid) || text(raw.refObjectId) || text(raw['@refObjectId']);
  if (!id) return null;
  return {
    id,
    version: text(raw.version) || text(raw['@version']) || '01.00.000',
    name: text(raw.name) || text(raw.name_en) || text(raw.shortDescription),
    unit: text(raw.unit) || text(raw.referenceUnit),
  };
}

function extractFlowRecord(row) {
  const dataset = flowDataset(row);
  const info = isRecord(dataset?.flowInformation?.dataSetInformation)
    ? dataset.flowInformation.dataSetInformation
    : {};
  const id = text(row?.id) || text(info['common:UUID']);
  const version =
    text(row?.version) ||
    text(dataset?.administrativeInformation?.publicationAndOwnership?.['common:dataSetVersion']) ||
    '01.00.000';
  const names = flowNamesFromDataset(dataset, id);
  const name = names[0] || id;
  const propertyGroup = propertyGroupFromDataset(dataset);
  return {
    id,
    version,
    stateCode: typeof row?.state_code === 'number' ? row.state_code : null,
    modifiedAt: text(row?.modified_at) || null,
    name,
    names,
    normalizedName: normalizeText(name),
    normalizedNames: uniqueStrings(
      names.flatMap((candidate) => [normalizeText(candidate), normalizeCandidateText(candidate)]),
    ),
    propertyGroup,
    flowType: flowTypeFromDataset(dataset),
    classificationTexts: classificationTextsFromDataset(dataset),
    unit: referenceUnitForGroup(propertyGroup, 'kg'),
  };
}

function collapseLatestMatchesById(matches) {
  const byId = new Map();
  for (const match of matches) {
    const current = byId.get(match.record.id);
    if (
      !current ||
      compareVersions(match.record.version, current.record.version) > 0 ||
      (
        compareVersions(match.record.version, current.record.version) === 0 &&
        reasonRank(match.candidate.reason) < reasonRank(current.candidate.reason)
      )
    ) {
      byId.set(match.record.id, match);
    }
  }
  return [...byId.values()].sort((left, right) => left.record.id.localeCompare(right.record.id));
}

function buildNearestInputMatches(flowNames, sourceUnit, scopeRecords) {
  const allQueryEntries = flowNames
    .map((name) => ({ name, tokens: new Set(uniqueTokens(name)) }))
    .filter((entry) => entry.tokens.size > 0);
  const queryHasChemicalForm = allQueryEntries.some((entry) =>
    chemicalFormTokens([...entry.tokens]).length > 0,
  );
  const queryEntries = queryHasChemicalForm
    ? allQueryEntries.filter((entry) => chemicalFormTokens([...entry.tokens]).length > 0)
    : allQueryEntries;
  const matches = [];
  for (const record of scopeRecords) {
    if (!isInputCompatibleFlowRecord(record, flowNames)) continue;
    if (sourceUnit.group && record.propertyGroup && sourceUnit.group !== record.propertyGroup) continue;
    for (const recordName of record.normalizedNames || [record.normalizedName]) {
      const recordTokens = uniqueTokens(recordName);
      if (recordTokens.length === 0) continue;
      for (const { name: queryName, tokens: queryTokens } of queryEntries) {
        if (!simpleSaltCounterionsCompatible(recordName, queryName)) continue;
        const score = nearestInputMatchScore(recordTokens, queryTokens);
        if (score === null) continue;
        matches.push({
          record,
          candidate: {
            name: record.name,
            normalizedName: record.normalizedName,
            reason: 'nearest_input_name_match',
            autoReuse: true,
          },
          score,
        });
      }
    }
  }
  return collapseBestNearestMatchesById(matches);
}

function isGasInputFlow(flowNames) {
  const tokens = new Set(flowNames.flatMap((name) => nearestTokens(name)));
  if (tokens.has('co2')) return true;
  return [...tokens].some((token) => INPUT_GAS_TOKENS.has(token));
}

function isInputCompatibleFlowRecord(record, flowNames) {
  const flowType = normalizeText(record.flowType);
  const classification = normalizeText((record.classificationTexts || []).join(' > '));
  const isEmission =
    classification.includes('emission') ||
    classification.includes('排放') ||
    classification.includes('non agricultural soil') ||
    classification.includes('nonagricultural soil') ||
    classification.includes('非农业土壤');
  const isElementary = flowType.includes('elementary');
  if (isGasInputFlow(flowNames)) {
    return !isEmission;
  }
  if (isElementary || isEmission) return false;
  if (flowType && !flowType.includes('product')) return false;
  return true;
}

function buildFlowUsage(plan) {
  const usage = {};
  for (const proc of plan?.processes || []) {
    for (const entry of proc.inputs || []) {
      usage[entry.flow] ||= { input: 0, output: 0 };
      usage[entry.flow].input += 1;
    }
    for (const entry of proc.outputs || []) {
      usage[entry.flow] ||= { input: 0, output: 0 };
      usage[entry.flow].output += 1;
    }
  }
  return usage;
}

function resolveOneFlow(flowKey, flow, generatedId, scopeRecords, options = {}) {
  const name = flow?.name_en || flow?.name || flowKey;
  const unit = flow?.unit || 'kg';
  const explicitRef = explicitExistingFlowRef(flow);
  if (explicitRef) {
    const targetUnit = explicitRef.unit || unit;
    return {
      flow_key: flowKey,
      decision: 'reuse_existing',
      reason: 'plan_existing_flow_ref',
      id: explicitRef.id,
      version: explicitRef.version,
      name: explicitRef.name || name,
      unit: targetUnit,
      amount_factor: amountFactor(unit, targetUnit),
      source_unit: unit,
      candidate_count: 0,
    };
  }

  if (options.forceCreateNewReason) {
    return buildCreateNewDecision(flowKey, flow, generatedId, options.forceCreateNewReason);
  }

  const sourceUnit = unitInfo(unit);
  const flowNames = uniqueStrings([
    name,
    flow?.name_en,
    flow?.name_zh,
    flow?.name,
    ...listify(flow?.aliases),
    ...listify(flow?.match_names),
  ]);
  const flowSearchCandidates = buildFlowSearchCandidates(flowNames);
  const matches = [];
  for (const record of scopeRecords) {
    for (const recordName of record.normalizedNames || [record.normalizedName]) {
      for (const candidate of flowSearchCandidates) {
        if (recordName === candidate.normalizedName) {
          matches.push({ record, candidate });
        }
      }
    }
  }
  const compatible = matches.filter(({ record }) => {
    if (!sourceUnit.group || !record.propertyGroup) return true;
    return sourceUnit.group === record.propertyGroup;
  });
  const inputCompatible = options.allowNearestInputMatch
    ? compatible.filter(({ record }) => isInputCompatibleFlowRecord(record, flowNames))
    : compatible;
  const candidates = options.allowNearestInputMatch
    ? inputCompatible
    : compatible.length > 0
      ? compatible
      : matches;
  const latestById = collapseLatestMatchesById(candidates).sort(compareCandidateMatches);
  const autoReusableLatest = latestById.filter((match) => match.candidate.autoReuse);
  const stableExisting = generatedId
    ? autoReusableLatest.find((match) => match.record.id === generatedId)
    : null;

  if (stableExisting) {
    return buildReuseDecision(
      flowKey,
      unit,
      stableExisting.record,
      stableExisting.candidate,
      candidates.length,
      'stable_uuid_exact_name_match',
    );
  }

  if (autoReusableLatest.length >= 1) {
    const target = autoReusableLatest[0].record;
    const matchedCandidate = autoReusableLatest[0].candidate;
    return buildReuseDecision(
      flowKey,
      unit,
      target,
      matchedCandidate,
      latestById.length,
      autoReusableLatest.length === 1 ? 'unique_exact_name_match' : 'best_exact_name_match',
    );
  }

  if (latestById.length >= 1) {
    const target = latestById[0].record;
    const matchedCandidate = latestById[0].candidate;
    return buildReuseDecision(
      flowKey,
      unit,
      target,
      matchedCandidate,
      latestById.length,
      'best_candidate_name_match',
    );
  }

  if (options.allowNearestInputMatch) {
    const nearest = buildNearestInputMatches(flowNames, sourceUnit, scopeRecords);
    if (nearest.length > 0) {
      const target = nearest[0].record;
      const matchedCandidate = nearest[0].candidate;
      return buildReuseDecision(
        flowKey,
        unit,
        target,
        matchedCandidate,
        nearest.length,
        'nearest_input_name_match',
      );
    }
  }

  return buildCreateNewDecision(
    flowKey,
    flow,
    generatedId,
    'no_exact_name_match',
    latestById.length,
    latestById.slice(0, 5).map(({ record, candidate }) => ({
      id: record.id,
      version: record.version,
      name: record.name,
      unit: record.unit,
      match_reason: candidate.reason,
    })),
  );
}

export function buildFlowResolution(plan, uuids, scopeRows = []) {
  const scopeRecords = scopeRows.map(extractFlowRecord).filter((record) => record.id);
  const flowUsage = buildFlowUsage(plan);
  const flows = {};
  const review = [];
  const summary = {
    total: 0,
    reuse_existing: 0,
    create_new: 0,
    ambiguous: 0,
    no_match: 0,
  };

  for (const [flowKey, flow] of Object.entries(plan?.flows || {}).sort(([a], [b]) =>
    a.localeCompare(b),
  )) {
    summary.total += 1;
    const usage = flowUsage[flowKey] || { input: 0, output: 0 };
    const decision = resolveOneFlow(flowKey, flow, uuids?.flows?.[flowKey] || '', scopeRecords, {
      forceCreateNewReason:
        usage.input > 0 && usage.output > 0 ? 'patent_internal_intermediate' : '',
      allowNearestInputMatch: usage.input > 0 && usage.output === 0,
    });
    flows[flowKey] = decision;
    if (decision.decision === 'reuse_existing') {
      summary.reuse_existing += 1;
    } else {
      summary.create_new += 1;
      if (decision.reason.startsWith('ambiguous_')) summary.ambiguous += 1;
      if (decision.reason === 'no_exact_name_match') summary.no_match += 1;
      if (decision.candidate_count > 0) {
        review.push({
          flow_key: flowKey,
          name: decision.name,
          reason: decision.reason,
          candidate_count: decision.candidate_count,
          candidates: decision.candidates,
        });
      }
    }
  }

  return {
    schema_version: 1,
    source_id: plan?.source?.id || null,
    summary,
    flows,
    review,
  };
}

export function loadFlowScopeRows(filePath) {
  if (!filePath) return [];
  const resolved = path.resolve(filePath);
  const textValue = fs.readFileSync(resolved, 'utf8');
  if (resolved.endsWith('.jsonl')) {
    return textValue
      .split(/\r?\n/u)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  }
  const parsed = JSON.parse(textValue);
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed.rows)) {
    return parsed.rows.map((row) => ({
      id: row.id,
      version: row.version,
      state_code: row.state_code,
      modified_at: row.modified_at,
      json_ordered: row.flow ?? row.json_ordered ?? row.json,
    }));
  }
  return [];
}

export function writeFlowResolution(filePath, resolution) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(resolution, null, 2)}\n`);
  return filePath;
}

export function applyFlowResolutionToExchange(exchange, direction, plan, resolution, options) {
  const flowKey = exchange.flow;
  const flow = plan?.flows?.[flowKey] || {};
  const resolved = resolution?.flows?.[flowKey] || {};
  const unit = resolved.unit || flow.unit || 'kg';
  const amount = Number(exchange.amount ?? 0) * Number(resolved.amount_factor ?? 1);
  const flowId = resolved.id || options.generatedFlowId;
  const version = resolved.version || '01.00.000';
  const name = resolved.name || flow.name_en || flowKey;
  const commentBits = [];
  if (resolved.decision === 'reuse_existing') {
    commentBits.push(`reused database flow ${flowId}@${version}`);
  }
  if (resolved.amount_factor && Number(resolved.amount_factor) !== 1) {
    commentBits.push(
      resolved.conversion_note || `converted from ${resolved.source_unit || flow.unit} to ${unit}`,
    );
  }

  return {
    '@dataSetInternalID': options.exchangeId,
    referenceToFlowDataSet: {
      '@type': 'flow data set',
      '@refObjectId': flowId,
      '@version': version,
      'common:shortDescription': [{ '@xml:lang': 'en', '#text': name }],
    },
    referenceUnit: unit,
    exchangeDirection: direction,
    meanAmount: amount,
    resultingAmount: amount,
    dataDerivationTypeStatus:
      exchange.derivation || (direction === 'Output' ? 'Measured' : 'Estimated'),
    ...(commentBits.length
      ? { 'common:generalComment': [{ '@xml:lang': 'en', '#text': commentBits.join('; ') }] }
      : {}),
  };
}
