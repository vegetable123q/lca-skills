import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  buildRemoteFlowScopeEnv,
  defaultRemoteFlowScopePath,
  ensureRemoteFlowScopeFile,
  hasRemoteFlowScopeEnv,
  requireRemoteFlowScopeFile,
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

test('ensureRemoteFlowScopeFile rejects explicit scope files', () => {
  const explicitFile = path.resolve('/workspace/output/scope.json');
  assert.throws(
    () =>
      ensureRemoteFlowScopeFile({
        base: '/workspace/output/CN123',
        explicitFlowScopeFile: explicitFile,
        env: remoteEnv,
        spawnImpl: () => {
          throw new Error('should not spawn');
        },
      }),
    /no longer accepts --flow-scope-file/u,
  );
});

test('requireRemoteFlowScopeFile rejects silent offline materialization without scope', () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-no-remote-scope-'));
  assert.throws(
    () =>
      requireRemoteFlowScopeFile({
        base,
        env: {},
        pathExists: () => false,
        spawnImpl: () => {
          throw new Error('should not spawn');
        },
      }),
    /remote flow scope credentials are required/u,
  );
});

test('buildRemoteFlowScopeEnv loads missing credentials from the local TianGong CLI .env', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-cli-env-'));
  const repoRoot = path.join(root, 'lca-skills');
  const cliDir = path.join(root, 'tiangong-cli');
  fs.mkdirSync(cliDir, { recursive: true });
  fs.writeFileSync(
    path.join(cliDir, '.env'),
    [
      'TIANGONG_LCA_API_BASE_URL=https://example.supabase.co/functions/v1',
      'TIANGONG_LCA_API_KEY=key-from-cli-env',
      'TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY=publishable-from-cli-env',
      '',
    ].join('\n'),
  );

  const result = buildRemoteFlowScopeEnv({
    repoRoot,
    env: {},
    pathExists: fs.existsSync,
  });

  assert.equal(result.cliDir, cliDir);
  assert.equal(result.env.TIANGONG_LCA_API_KEY, 'key-from-cli-env');
  assert.equal(hasRemoteFlowScopeEnv(result.env), true);
});

test('ensureRemoteFlowScopeFile materializes one repo-level remote flow list when env is available', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-remote-scope-'));
  const repoRoot = path.join(root, 'lca-skills');
  const observed = [];
  const result = ensureRemoteFlowScopeFile({
    base: path.join(repoRoot, 'output', 'CN123'),
    env: remoteEnv,
    repoRoot,
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

  assert.equal(result, defaultRemoteFlowScopePath(repoRoot));
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

test('ensureRemoteFlowScopeFile reuses the repo-level remote flow list without refetching', () => {
  const repoRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'ptl-remote-scope-cache-'));
  const scopeFile = defaultRemoteFlowScopePath(repoRoot);
  fs.mkdirSync(path.dirname(scopeFile), { recursive: true });
  fs.writeFileSync(scopeFile, JSON.stringify({ rows: [{ id: 'cached-flow' }] }));

  const result = ensureRemoteFlowScopeFile({
    base: path.join(repoRoot, 'output', 'CN123'),
    env: remoteEnv,
    repoRoot,
    spawnImpl: () => {
      throw new Error('should not refetch');
    },
  });

  assert.equal(result, scopeFile);
});
