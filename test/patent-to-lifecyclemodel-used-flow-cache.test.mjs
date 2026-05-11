import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  applyUsedFlowCacheToPlan,
  loadUsedFlowCache,
  unresolvedFlowKeysForRemoteScope,
  updateUsedFlowCacheFromResolution,
} from '../patent-to-lifecyclemodel/scripts/used-flow-cache.mjs';

test('used flow cache applies only exact name and unit matches', () => {
  const plan = {
    flows: {
      water: {
        name_en: 'Water, deionised',
        name_zh: '去离子水',
        unit: 'kg',
      },
      water_liters: {
        name_en: 'Water, deionised',
        unit: 'L',
      },
      product: {
        name_en: 'Patent-specific product',
        unit: 'kg',
      },
    },
  };
  const cache = {
    schema_version: 1,
    entries: [
      {
        id: 'flow-water',
        version: '01.00.000',
        name: 'Water, deionised',
        unit: 'kg',
        names: ['Water, deionised', '去离子水'],
      },
    ],
  };

  const result = applyUsedFlowCacheToPlan(plan, cache);

  assert.deepEqual(result.applied.map((entry) => entry.flow_key), ['water']);
  assert.equal(plan.flows.water.existing_flow_ref.id, 'flow-water');
  assert.deepEqual(result.unresolved.sort(), ['product', 'water_liters']);
  assert.deepEqual(unresolvedFlowKeysForRemoteScope(plan).sort(), ['product', 'water_liters']);
});

test('used flow cache does not apply ambiguous exact matches', () => {
  const plan = {
    flows: {
      water: {
        name_en: 'Water, deionised',
        unit: 'kg',
      },
    },
  };
  const cache = {
    schema_version: 1,
    entries: [
      { id: 'flow-1', version: '01.00.000', name: 'Water, deionised', unit: 'kg', names: [] },
      { id: 'flow-2', version: '01.00.000', name: 'Water, deionised', unit: 'kg', names: [] },
    ],
  };

  const result = applyUsedFlowCacheToPlan(plan, cache);

  assert.deepEqual(result.applied, []);
  assert.equal(plan.flows.water.existing_flow_ref, undefined);
  assert.deepEqual(result.unresolved, ['water']);
});

test('used flow cache stores only direct reusable database decisions', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-used-flow-cache-'));
  const cachePath = path.join(dir, 'used-flows.json');
  const plan = {
    flows: {
      water: {
        name_en: 'Water, deionised',
        name_zh: '去离子水',
        unit: 'kg',
      },
      hydrated: {
        name_en: 'Example hydrate',
        unit: 'kg',
      },
      new_product: {
        name_en: 'Patent product',
        unit: 'kg',
      },
    },
  };
  const resolution = {
    flows: {
      water: {
        decision: 'reuse_existing',
        reason: 'unique_exact_name_match',
        id: 'flow-water',
        version: '01.00.000',
        name: 'Water, deionised',
        unit: 'kg',
        source_unit: 'kg',
        amount_factor: 1,
      },
      hydrated: {
        decision: 'reuse_existing',
        reason: 'unique_exact_name_match',
        id: 'flow-hydrated',
        version: '01.00.000',
        name: 'Example hydrate',
        unit: 'kg',
        source_unit: 'g',
        amount_factor: 0.001,
      },
      new_product: {
        decision: 'create_new',
        reason: 'no_exact_name_match',
        id: 'generated-product',
        unit: 'kg',
      },
    },
  };

  updateUsedFlowCacheFromResolution(plan, resolution, cachePath);
  const cache = loadUsedFlowCache(cachePath);

  assert.deepEqual(cache.entries.map((entry) => entry.id), ['flow-water']);
  assert.deepEqual(cache.entries[0].names, ['Water, deionised', '去离子水']);
});
