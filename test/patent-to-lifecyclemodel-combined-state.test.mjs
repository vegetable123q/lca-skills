import test from 'node:test';
import assert from 'node:assert/strict';
import { buildCombinedProcessFromFlowState } from '../patent-to-lifecyclemodel/scripts/combined-state.mjs';

test('buildCombinedProcessFromFlowState carries the final reference flow summary', () => {
  const plan = {
    source: { id: 'CN109119591B' },
    flows: {
      input: { name_en: 'Patent described inputs', name_zh: '专利描述输入', unit: 'item' },
      product: { name_en: 'Composite positive electrode', name_zh: '复合正极', unit: 'item' },
    },
    processes: [
      {
        key: 'prepare',
        reference_output_flow: 'product',
        inputs: [{ flow: 'input', amount: 1 }],
        outputs: [{ flow: 'product', amount: 1 }],
      },
    ],
  };
  const uuids = {
    flows: {
      input: 'input-flow-uuid',
      product: 'product-flow-uuid',
    },
  };

  const state = buildCombinedProcessFromFlowState(plan, uuids);

  assert.equal(state.flow_summary.uuid, 'product-flow-uuid');
  assert.equal(state.flow_summary.base_name_en, 'Composite positive electrode');
  assert.equal(state.flow_summary.base_name_zh, '复合正极');
  assert.equal(state.flow_summary.unit, 'item');
});
