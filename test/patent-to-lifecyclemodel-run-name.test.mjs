import test from 'node:test';
import assert from 'node:assert/strict';
import { combinedRunNameFromSourceId } from '../patent-to-lifecyclemodel/scripts/run-names.mjs';

test('combinedRunNameFromSourceId makes source-specific lifecyclemodel run names', () => {
  assert.equal(combinedRunNameFromSourceId('CN109119591B'), 'CN109119591B-combined');
  assert.equal(combinedRunNameFromSourceId('CN109273701B'), 'CN109273701B-combined');
  assert.notEqual(
    combinedRunNameFromSourceId('CN109119591B'),
    combinedRunNameFromSourceId('CN109273701B'),
  );
});

test('combinedRunNameFromSourceId sanitizes source ids for path basenames', () => {
  assert.equal(combinedRunNameFromSourceId('CN 109/119:591B'), 'CN-109-119-591B-combined');
  assert.equal(combinedRunNameFromSourceId(''), 'source-combined');
});
