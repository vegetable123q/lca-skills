import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  ensureRemoteFlowScopeFile,
  hasRemoteFlowScopeEnv,
} from '../patent-to-lifecyclemodel/scripts/remote-flow-scope.mjs';

const remoteEnv = {
  TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co/functions/v1',
  TIANGONG_LCA_API_KEY: 'key',
  TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY: 'publishable',
};

test('hasRemoteFlowScopeEnv requires the Supabase read runtime env', () => {
  assert.equal(hasRemoteFlowScopeEnv(remoteEnv), true);
  assert.equal(hasRemoteFlowScopeEnv({ ...remoteEnv, TIANGONG_LCA_API_KEY: '' }), false);
});

test('ensureRemoteFlowScopeFile uses an explicit scope file without shelling out', () => {
  const explicitFile = path.resolve('/workspace/output/scope.json');
  const result = ensureRemoteFlowScopeFile({
    base: '/workspace/output/CN123',
    explicitFlowScopeFile: explicitFile,
    env: remoteEnv,
    spawnImpl: () => {
      throw new Error('should not spawn');
    },
  });

  assert.equal(result, explicitFile);
});

test('ensureRemoteFlowScopeFile materializes remote flow list when env is available', () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-remote-scope-'));
  const observed = [];
  const result = ensureRemoteFlowScopeFile({
    base,
    env: remoteEnv,
    repoRoot: '/workspace/lca-skills',
    pathExists: (candidate) => candidate.endsWith('/tiangong-cli/bin/tiangong.js'),
    spawnImpl: (command, args, options) => {
      observed.push([command, args]);
      fs.writeFileSync(
        options.stdio[1],
        JSON.stringify({
          schema_version: 1,
          rows: [
            {
              id: 'flow-1',
              version: '01.00.000',
              flow: { flowDataSet: { id: 'flow-1' } },
            },
          ],
        }),
      );
      return {
        status: 0,
        stderr: '',
      };
    },
  });

  const written = JSON.parse(fs.readFileSync(result, 'utf8'));

  assert.equal(result, path.join(base, 'flow-scope.json'));
  assert.equal(written.rows[0].id, 'flow-1');
  assert.deepEqual(observed[0][1].slice(-8), [
    '--state-code',
    '0',
    '--state-code',
    '100',
    '--all',
    '--page-size',
    '1000',
    '--json',
  ]);
  assert.equal(observed[0][1].includes('flow'), true);
  assert.equal(observed[0][1].includes('list'), true);
});
