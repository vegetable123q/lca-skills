import fs from 'node:fs';
import path from 'node:path';

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function listify(value) {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];
  for (const value of values) {
    const trimmed = text(value);
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

function exactKey(name, unit) {
  const normalizedName = text(name).toLowerCase();
  const normalizedUnit = text(unit);
  return normalizedName && normalizedUnit ? `${normalizedName}\u0000${normalizedUnit}` : '';
}

function existingFlowRef(flow) {
  const raw = flow?.existing_flow_ref || flow?.db_flow || flow?.database_flow;
  if (!isRecord(raw)) return null;
  const id = text(raw.id) || text(raw.uuid) || text(raw.refObjectId) || text(raw['@refObjectId']);
  if (!id) return null;
  return {
    id,
    version: text(raw.version) || text(raw['@version']) || '01.00.000',
    name: text(raw.name) || text(raw.name_en) || text(raw.shortDescription),
    unit: text(raw.unit) || text(raw.referenceUnit) || text(flow?.unit),
  };
}

export function defaultUsedFlowCachePath(repoRoot) {
  return path.join(path.resolve(repoRoot), 'output', 'patent-to-lifecyclemodel-used-flows.json');
}

export function flowDirectNames(flow) {
  return uniqueStrings([
    flow?.name_en,
    flow?.name_zh,
    flow?.name,
    ...listify(flow?.aliases),
    ...listify(flow?.match_names),
  ]);
}

export function loadUsedFlowCache(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return { schema_version: 1, entries: [] };
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return {
    schema_version: 1,
    entries: Array.isArray(parsed?.entries) ? parsed.entries : [],
  };
}

function buildCacheIndex(cache) {
  const index = new Map();
  for (const entry of cache.entries || []) {
    const names = uniqueStrings([entry.name, ...listify(entry.names)]);
    for (const name of names) {
      const key = exactKey(name, entry.unit);
      if (!key) continue;
      const list = index.get(key) || [];
      list.push(entry);
      index.set(key, list);
    }
  }
  return index;
}

function directCacheMatch(flow, flowKey, index) {
  const unit = text(flow?.unit);
  if (!unit) return null;
  const matched = [];
  const seenIds = new Set();
  for (const name of flowDirectNames(flow)) {
    const candidates = index.get(exactKey(name, unit)) || [];
    for (const candidate of candidates) {
      const id = text(candidate.id);
      if (!id || seenIds.has(id)) continue;
      seenIds.add(id);
      matched.push(candidate);
    }
  }
  if (matched.length !== 1) return null;
  const entry = matched[0];
  return {
    id: entry.id,
    version: text(entry.version) || '01.00.000',
    name: text(entry.name),
    unit: text(entry.unit) || unit,
  };
}

export function applyUsedFlowCacheToPlan(plan, cache) {
  const index = buildCacheIndex(cache);
  const applied = [];
  const unresolved = [];

  for (const [flowKey, flow] of Object.entries(plan?.flows || {})) {
    if (existingFlowRef(flow)) continue;
    const match = directCacheMatch(flow, flowKey, index);
    if (!match) {
      unresolved.push(flowKey);
      continue;
    }
    flow.existing_flow_ref = match;
    applied.push({ flow_key: flowKey, id: match.id, version: match.version, name: match.name, unit: match.unit });
  }

  return { applied, unresolved };
}

export function unresolvedFlowKeysForRemoteScope(plan) {
  return Object.entries(plan?.flows || {})
    .filter(([, flow]) => !existingFlowRef(flow))
    .map(([flowKey]) => flowKey);
}

function directReusableDecision(decision) {
  if (decision?.decision !== 'reuse_existing') return false;
  if (Number(decision.amount_factor ?? 1) !== 1) return false;
  if (text(decision.source_unit) && text(decision.unit) && text(decision.source_unit) !== text(decision.unit)) {
    return false;
  }
  return ['unique_exact_name_match', 'best_exact_name_match', 'stable_uuid_exact_name_match', 'plan_existing_flow_ref'].includes(
    decision.reason,
  );
}

export function updateUsedFlowCacheFromResolution(plan, resolution, filePath) {
  const current = loadUsedFlowCache(filePath);
  const byId = new Map();
  for (const entry of current.entries || []) {
    const id = text(entry.id);
    if (!id) continue;
    byId.set(id, {
      id,
      version: text(entry.version) || '01.00.000',
      name: text(entry.name),
      unit: text(entry.unit),
      names: flowDirectNames({ name: entry.name, aliases: entry.names }),
    });
  }

  for (const [flowKey, decision] of Object.entries(resolution?.flows || {})) {
    if (!directReusableDecision(decision)) continue;
    const id = text(decision.id);
    const unit = text(decision.unit);
    if (!id || !unit) continue;
    const flow = plan?.flows?.[flowKey] || {};
    const previous = byId.get(id);
    byId.set(id, {
      id,
      version: text(decision.version) || previous?.version || '01.00.000',
      name: text(decision.name) || previous?.name || id,
      unit,
      names: uniqueStrings([
        ...(previous?.names || []),
        text(decision.name),
        ...flowDirectNames(flow),
      ]),
    });
  }

  const entries = [...byId.values()].sort((left, right) =>
    `${left.name}\u0000${left.id}`.localeCompare(`${right.name}\u0000${right.id}`),
  );
  const next = { schema_version: 1, entries };
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(next, null, 2)}\n`);
  return next;
}
