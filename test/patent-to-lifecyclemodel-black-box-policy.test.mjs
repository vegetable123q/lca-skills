import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

test('patent-to-lifecyclemodel documents black-box as a last resort only', () => {
  const skill = fs.readFileSync('patent-to-lifecyclemodel/SKILL.md', 'utf8');
  const agent = fs.readFileSync('patent-to-lifecyclemodel/agents/openai.yaml', 'utf8');
  const template = fs.readFileSync('patent-to-lifecyclemodel/assets/plan.template.json', 'utf8');

  assert.match(skill, /black_box: true` only when/u);
  assert.match(skill, /Never mark a whole patent route black-box/u);
  assert.match(agent, /avoid black-box processes unless critical data are missing/u);
  assert.match(template, /Black-box is last resort/u);
});
