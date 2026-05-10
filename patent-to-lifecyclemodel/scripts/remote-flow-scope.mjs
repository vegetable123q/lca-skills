import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { buildTiangongInvocation } from '../../scripts/lib/cli-launcher.mjs';

const REQUIRED_ENV = [
  'TIANGONG_LCA_API_BASE_URL',
  'TIANGONG_LCA_API_KEY',
  'TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY',
];

function nonEmpty(value) {
  return typeof value === 'string' && value.trim().length > 0;
}

export function hasRemoteFlowScopeEnv(env = process.env) {
  return REQUIRED_ENV.every((key) => nonEmpty(env[key]));
}

export function buildRemoteFlowListArgs(options = {}) {
  const pageSize = Number.isInteger(options.pageSize) ? options.pageSize : 1000;
  return [
    'flow',
    'list',
    '--state-code',
    '0',
    '--state-code',
    '100',
    '--all',
    '--page-size',
    String(pageSize),
    '--json',
  ];
}

export function ensureRemoteFlowScopeFile(options) {
  const explicitFlowScopeFile = options?.explicitFlowScopeFile;
  if (explicitFlowScopeFile) {
    return path.resolve(explicitFlowScopeFile);
  }

  const env = options?.env ?? process.env;
  if (!hasRemoteFlowScopeEnv(env)) {
    return null;
  }

  const base = path.resolve(options?.base);
  const outPath = path.join(base, 'flow-scope.json');
  const tempOutPath = `${outPath}.tmp`;
  const invocation = buildTiangongInvocation(buildRemoteFlowListArgs(options), {
    repoRoot: options?.repoRoot,
    cliDir: options?.cliDir,
    pathExists: options?.pathExists,
  });
  const spawnImpl = options?.spawnImpl ?? spawnSync;
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  const stdoutFd = fs.openSync(tempOutPath, 'w');
  let result;
  try {
    result = spawnImpl(invocation.command, invocation.args, {
      stdio: ['ignore', stdoutFd, 'pipe'],
      encoding: 'utf8',
      env,
    });
  } finally {
    fs.closeSync(stdoutFd);
  }

  if (result.error) {
    fs.rmSync(tempOutPath, { force: true });
    throw new Error(`remote flow scope fetch failed: ${result.error.message}`);
  }

  if (result.status !== 0) {
    fs.rmSync(tempOutPath, { force: true });
    const stderr = String(result.stderr || '').trim();
    throw new Error(
      ['remote flow scope fetch failed via `tiangong flow list`', stderr]
        .filter(Boolean)
        .join(': '),
    );
  }

  fs.renameSync(tempOutPath, outPath);
  return outPath;
}

export function requireRemoteFlowScopeFile(options) {
  const result = ensureRemoteFlowScopeFile(options);
  if (result) return result;

  throw new Error(
    'remote flow scope is required for patent-to-lifecyclemodel materialization; ' +
      'set TIANGONG_LCA_API_BASE_URL, TIANGONG_LCA_API_KEY, and ' +
      'TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY, pass --flow-scope-file, or pass ' +
      '--no-remote-flow-scope only for offline tests.',
  );
}
