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

function propertyGroupFromDataset(dataset) {
  const property = listify(dataset?.flowProperties?.flowProperty)[0] || {};
  const ref = isRecord(property.referenceToFlowPropertyDataSet)
    ? property.referenceToFlowPropertyDataSet
    : {};
  const id = text(ref['@refObjectId']);
  const description = normalizeText(ref['common:shortDescription']);
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
    name,
    names,
    normalizedName: normalizeText(name),
    normalizedNames: uniqueStrings(names.map((candidate) => normalizeText(candidate))),
    propertyGroup,
    unit: referenceUnitForGroup(propertyGroup, 'kg'),
  };
}

function collapseLatestById(candidates) {
  const byId = new Map();
  for (const candidate of candidates) {
    const current = byId.get(candidate.id);
    if (!current || compareVersions(candidate.version, current.version) > 0) {
      byId.set(candidate.id, candidate);
    }
  }
  return [...byId.values()].sort((left, right) => left.id.localeCompare(right.id));
}

function resolveOneFlow(flowKey, flow, generatedId, scopeRecords) {
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

  const sourceUnit = unitInfo(unit);
  const flowNames = uniqueStrings([
    name,
    flow?.name_en,
    flow?.name_zh,
    flow?.name,
    ...listify(flow?.aliases),
    ...listify(flow?.match_names),
  ]);
  const normalizedFlowNames = uniqueStrings(flowNames.map((candidate) => normalizeText(candidate)));
  const exact = scopeRecords.filter((record) =>
    (record.normalizedNames || [record.normalizedName]).some((candidate) =>
      normalizedFlowNames.includes(candidate),
    ),
  );
  const compatible = exact.filter((record) => {
    if (!sourceUnit.group || !record.propertyGroup) return true;
    return sourceUnit.group === record.propertyGroup;
  });
  const candidates = compatible.length > 0 ? compatible : exact;
  const latestById = collapseLatestById(candidates);
  const stableExisting = generatedId
    ? latestById.find((candidate) => candidate.id === generatedId)
    : null;

  if (stableExisting) {
    const factor = amountFactor(unit, stableExisting.unit);
    return {
      flow_key: flowKey,
      decision: 'reuse_existing',
      reason: 'stable_uuid_exact_name_match',
      id: stableExisting.id,
      version: stableExisting.version,
      name: stableExisting.name,
      unit: stableExisting.unit,
      amount_factor: factor,
      source_unit: unit,
      candidate_count: candidates.length,
    };
  }

  if (latestById.length === 1) {
    const target = latestById[0];
    const factor = amountFactor(unit, target.unit);
    return {
      flow_key: flowKey,
      decision: 'reuse_existing',
      reason: 'unique_exact_name_match',
      id: target.id,
      version: target.version,
      name: target.name,
      unit: target.unit,
      amount_factor: factor,
      source_unit: unit,
      candidate_count: candidates.length,
    };
  }

  return {
    flow_key: flowKey,
    decision: 'create_new',
    reason: latestById.length > 1 ? 'ambiguous_exact_name_match' : 'no_exact_name_match',
    id: generatedId,
    version: '01.00.000',
    name,
    unit,
    amount_factor: 1,
    source_unit: unit,
    candidate_count: latestById.length,
    candidates: latestById.slice(0, 20).map((candidate) => ({
      id: candidate.id,
      version: candidate.version,
      name: candidate.name,
      unit: candidate.unit,
    })),
  };
}

export function buildFlowResolution(plan, uuids, scopeRows = []) {
  const scopeRecords = scopeRows.map(extractFlowRecord).filter((record) => record.id);
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
    const decision = resolveOneFlow(flowKey, flow, uuids?.flows?.[flowKey] || '', scopeRecords);
    flows[flowKey] = decision;
    if (decision.decision === 'reuse_existing') {
      summary.reuse_existing += 1;
    } else {
      summary.create_new += 1;
      if (decision.reason === 'ambiguous_exact_name_match') summary.ambiguous += 1;
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
    commentBits.push(`converted from ${resolved.source_unit || flow.unit} to ${unit}`);
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
