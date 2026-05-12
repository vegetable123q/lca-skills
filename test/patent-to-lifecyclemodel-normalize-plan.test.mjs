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
            technology: 'Example 1: product is generated from the listed feedstock under patent conditions.',
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

test('normalize-plan fills deterministic review fields and source refs', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-normalize-review-'));
  const planPath = path.join(dir, 'plan.json');
  fs.writeFileSync(
    planPath,
    `${JSON.stringify(
      {
        source: { id: 'CN123456A', title: 'Example patent' },
        flows: {
          precursor: { unit: 'kg' },
          product: { name_en: 'Patent product', unit: 'kg' },
        },
        processes: [
          {
            key: 'sinter_product',
            reference_output_flow: 'product',
            technology:
              'Example 1: precursor is calcined at 800 C for 10 h in oxygen, then cooled and crushed.',
            inputs: [{ flow: 'precursor', amount: 1.2, derivation: 'Measured' }],
            outputs: [{ flow: 'product', amount: 1, derivation: 'Measured' }],
          },
        ],
      },
      null,
      2,
    )}\n`,
  );

  const result = spawnSync(
    process.execPath,
    ['patent-to-lifecyclemodel/scripts/normalize-plan.mjs', '--plan', planPath, '--write', '--json'],
    { encoding: 'utf8' },
  );

  assert.equal(result.status, 0, result.stderr);
  const normalized = JSON.parse(fs.readFileSync(planPath, 'utf8'));
  const proc = normalized.processes[0];
  assert.equal(proc.step_id, 'S1');
  assert.equal(proc.name_en, 'sinter product');
  assert.equal(proc.scale, 'unspecified');
  assert.match(proc.comment, /Patent-derived unit process from CN123456A/u);
  assert.equal(normalized.flows.precursor.name_en, 'precursor');
  assert.equal(proc.inputs[0].source_ref, 'CN123456A S1');
  assert.equal(proc.outputs[0].source_ref, 'CN123456A S1');
});

test('normalize-plan requires technology source text for reviewable processes', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-normalize-review-missing-'));
  const planPath = path.join(dir, 'plan.json');
  fs.writeFileSync(
    planPath,
    `${JSON.stringify(
      {
        source: { id: 'CN123456A' },
        flows: {
          product: { name_en: 'Patent product', unit: 'kg' },
        },
        processes: [
          {
            key: 'make_product',
            reference_output_flow: 'product',
            inputs: [],
            outputs: [{ flow: 'product', amount: 1, derivation: 'Measured' }],
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
  assert.match(result.stderr, /technology.*处理、标准、路线/u);
});
