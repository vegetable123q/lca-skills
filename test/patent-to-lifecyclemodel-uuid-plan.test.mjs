import test from 'node:test';
import assert from 'node:assert/strict';
import { buildPlanUuids } from '../patent-to-lifecyclemodel/scripts/uuid-plan.mjs';

const plan = {
  source: { id: 'CN123' },
  flows: {
    input: { name_en: 'Input', unit: 'kg' },
    output: { name_en: 'Output', unit: 'kg' },
  },
  processes: [{ key: 's01' }],
};

test('buildPlanUuids reuses existing UUIDs for stable remote overwrites', () => {
  const uuids = buildPlanUuids(plan, {
    existing: {
      flows: { input: 'existing-input' },
      procs: { s01: 'existing-process' },
      srcs: { patent: 'existing-source' },
    },
  });

  assert.equal(uuids.flows.input, 'existing-input');
  assert.equal(uuids.procs.s01, 'existing-process');
  assert.equal(uuids.srcs.patent, 'existing-source');
  assert.match(uuids.flows.output, /^[0-9a-f-]{36}$/u);
});

test('buildPlanUuids is deterministic from source id when no UUID file exists', () => {
  const first = buildPlanUuids(plan);
  const second = buildPlanUuids(plan);

  assert.deepEqual(second, first);
});
