import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { findBuiltLifecyclemodelFile } from '../patent-to-lifecyclemodel/scripts/model-files.mjs';

test('findBuiltLifecyclemodelFile finds a source-specific lifecyclemodel model file', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-model-file-'));
  const modelFile = path.join(
    dir,
    'models',
    'CN109119591B-combined',
    'tidas_bundle',
    'lifecyclemodels',
    'model-id_01.01.000.json',
  );
  fs.mkdirSync(path.dirname(modelFile), { recursive: true });
  fs.writeFileSync(modelFile, '{}\n');

  assert.equal(findBuiltLifecyclemodelFile(dir), modelFile);
});

test('findBuiltLifecyclemodelFile rejects ambiguous model files', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-model-file-'));
  for (const runName of ['A-combined', 'B-combined']) {
    const modelFile = path.join(
      dir,
      'models',
      runName,
      'tidas_bundle',
      'lifecyclemodels',
      `${runName}_01.01.000.json`,
    );
    fs.mkdirSync(path.dirname(modelFile), { recursive: true });
    fs.writeFileSync(modelFile, '{}\n');
  }

  assert.throws(() => findBuiltLifecyclemodelFile(dir), /expected exactly one built lifecyclemodel/u);
});
