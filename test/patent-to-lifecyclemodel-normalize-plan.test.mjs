import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

test('normalize-plan rejects non-gas emission flows on process inputs', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-normalize-'));
  const planPath = path.join(dir, 'plan.json');
  fs.writeFileSync(
    planPath,
    `${JSON.stringify(
      {
        source: { id: 'CN123' },
        flows: {
          soil_emission: {
            name_en: 'Emission to soil, non-agricultural soil',
            unit: 'kg',
            existing_flow_ref: {
              id: 'emission-flow',
              version: '03.00.004',
              name: '排放 > 排放到土壤 > 排放到非农业土壤',
              unit: 'kg',
            },
          },
          product: { name_en: 'Patent product', unit: 'kg' },
        },
        processes: [
          {
            key: 'make_product',
            reference_output_flow: 'product',
            inputs: [{ flow: 'soil_emission', amount: 1 }],
            outputs: [{ flow: 'product', amount: 1 }],
          },
        ],
      },
      null,
      2,
    )}\n`,
  );

  const result = spawnSync(
    process.execPath,
    ['patent-to-lifecyclemodel/scripts/normalize-plan.mjs', '--plan', planPath, '--json'],
    { encoding: 'utf8' },
  );

  assert.equal(result.status, 2);
  assert.match(result.stderr, /must not use emission or elementary flow/i);
});
