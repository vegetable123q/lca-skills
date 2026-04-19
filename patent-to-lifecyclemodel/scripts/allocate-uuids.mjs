#!/usr/bin/env node
// allocate-uuids.mjs
// Generates one crypto.randomUUID per named flow/process/source and writes a
// single JSON map to stdout. This is the ONLY UUID allocator the patent->
// lifecyclemodel SOP uses; downstream datasets reference these UUIDs so the
// lifecyclemodel auto-build can infer edges via shared flow UUIDs.
//
// Usage:
//   node allocate-uuids.mjs \
//     --flows mofs,ncm_oxide,cathode,... \
//     --processes mofs_proc,ncm_oxide_proc,cathode_proc \
//     --sources patent \
//     [--seed <hex>]          # optional: deterministic UUIDs from a seed
//     > output/<SOURCE>/uuids.json
//
// The helper intentionally does NOT call any builder. It is pure data.

import crypto from 'node:crypto';

const argv = process.argv.slice(2);

function printHelp() {
  console.log(`Usage:
  node patent-to-lifecyclemodel/scripts/allocate-uuids.mjs --flows <csv> [--processes <csv>] [--sources <csv>] [--seed <seed>]

Examples:
  node patent-to-lifecyclemodel/scripts/allocate-uuids.mjs --flows precursor,cathode --processes precursor_proc,cathode_proc
  node patent-to-lifecyclemodel/scripts/allocate-uuids.mjs --flows precursor,cathode --seed cn111725499b
`.trim());
}

if (argv.includes('--help') || argv.includes('-h')) {
  printHelp();
  process.exit(0);
}

function parseArg(flag, def = '') {
  const i = argv.indexOf(flag);
  if (i === -1) return def;
  return argv[i + 1] ?? def;
}

function splitCsv(s) {
  return s.split(',').map((x) => x.trim()).filter(Boolean);
}

const flows = splitCsv(parseArg('--flows', ''));
const processes = splitCsv(parseArg('--processes', ''));
const sources = splitCsv(parseArg('--sources', 'patent'));
const seed = parseArg('--seed', '');

if (flows.length === 0 && processes.length === 0) {
  console.error('allocate-uuids: supply at least --flows or --processes');
  process.exit(2);
}

function seededUuid(key) {
  if (!seed) return crypto.randomUUID();
  // Deterministic: sha256(seed + "|" + key) → formatted as UUID v4-ish
  const h = crypto.createHash('sha256').update(`${seed}|${key}`).digest('hex');
  return (
    h.slice(0, 8) + '-' +
    h.slice(8, 12) + '-' +
    '4' + h.slice(13, 16) + '-' +
    '8' + h.slice(17, 20) + '-' +
    h.slice(20, 32)
  );
}

const payload = {
  flows: Object.fromEntries(flows.map((k) => [k, seededUuid(`flow:${k}`)])),
  procs: Object.fromEntries(processes.map((k) => [k, seededUuid(`proc:${k}`)])),
  srcs: Object.fromEntries(sources.map((k) => [k, seededUuid(`src:${k}`)])),
};

process.stdout.write(JSON.stringify(payload, null, 2) + '\n');
