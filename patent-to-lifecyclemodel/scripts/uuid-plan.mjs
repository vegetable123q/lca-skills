import crypto from 'node:crypto';
import fs from 'node:fs';

function deterministicUuid(seed, key) {
  const h = crypto.createHash('sha256').update(`${seed}|${key}`).digest('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`;
}

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function text(value) {
  return typeof value === 'string' ? value.trim() : '';
}

export function readExistingUuids(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return {};
  const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return isRecord(parsed) ? parsed : {};
}

export function buildPlanUuids(plan, options = {}) {
  const existing = isRecord(options.existing) ? options.existing : {};
  const seed = text(options.seed) || text(plan?.source?.id) || 'patent-to-lifecyclemodel';
  const existingFlows = isRecord(existing.flows) ? existing.flows : {};
  const existingProcs = isRecord(existing.procs) ? existing.procs : {};
  const existingSrcs = isRecord(existing.srcs) ? existing.srcs : {};
  const flowKeys = Object.keys(plan?.flows || {});
  const procKeys = (plan?.processes || []).map((proc) => proc.key).filter(Boolean);

  return {
    flows: Object.fromEntries(
      flowKeys.map((key) => [key, text(existingFlows[key]) || deterministicUuid(seed, `flow:${key}`)]),
    ),
    procs: Object.fromEntries(
      procKeys.map((key) => [key, text(existingProcs[key]) || deterministicUuid(seed, `proc:${key}`)]),
    ),
    srcs: {
      patent:
        text(existingSrcs.patent) ||
        deterministicUuid(seed, `src:${plan?.source?.id || 'source'}`),
    },
  };
}
