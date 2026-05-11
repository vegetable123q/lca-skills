import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import {
  buildTiangongInvocation,
  defaultLocalCliDirCandidates,
  resolveDefaultLocalCliDir,
} from '../../scripts/lib/cli-launcher.mjs';

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

function normalizeCliDir(cliDir) {
  const trimmed = typeof cliDir === 'string' ? cliDir.trim() : '';
  return trimmed ? path.resolve(trimmed) : null;
}

function parseDotenv(textValue) {
  const env = {};
  for (const rawLine of textValue.split(/\r?\n/u)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const match = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/u.exec(line);
    if (!match) continue;
    const [, key, rawValue] = match;
    let value = rawValue.trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

export function resolveRemoteFlowScopeCliDir(options = {}) {
  const explicitOptionCliDir = normalizeCliDir(options.cliDir);
  if (explicitOptionCliDir) return explicitOptionCliDir;
  const env = options.env ?? process.env;
  const explicitCliDir = normalizeCliDir(env.TIANGONG_LCA_CLI_DIR);
  if (explicitCliDir) return explicitCliDir;
  const pathExists = options.pathExists ?? fs.existsSync;
  for (const candidate of defaultLocalCliDirCandidates(options.repoRoot)) {
    if (!pathExists(candidate)) continue;
    const dotenvPath = path.join(candidate, '.env');
    if (!fs.existsSync(dotenvPath)) continue;
    const dotenvEnv = parseDotenv(fs.readFileSync(dotenvPath, 'utf8'));
    if (hasRemoteFlowScopeEnv(dotenvEnv)) return candidate;
  }
  return resolveDefaultLocalCliDir({
    repoRoot: options.repoRoot,
    pathExists,
  });
}

export function buildRemoteFlowScopeEnv(options = {}) {
  const baseEnv = { ...(options.env ?? process.env) };
  const cliDir = resolveRemoteFlowScopeCliDir(options);
  const dotenvPath = cliDir ? path.join(cliDir, '.env') : null;
  if (dotenvPath && fs.existsSync(dotenvPath)) {
    const dotenvEnv = parseDotenv(fs.readFileSync(dotenvPath, 'utf8'));
    for (const key of REQUIRED_ENV) {
      if (!nonEmpty(baseEnv[key]) && nonEmpty(dotenvEnv[key])) {
        baseEnv[key] = dotenvEnv[key];
      }
    }
  }
  return { env: baseEnv, cliDir, dotenvPath };
}

export function defaultRemoteFlowScopePath(repoRoot) {
  return path.join(path.resolve(repoRoot), 'output', 'patent-to-lifecyclemodel-flow-scope.json');
}

export function ensureRemoteFlowScopeFile(options) {
  if (options?.explicitFlowScopeFile) {
    throw new Error(
      'patent-to-lifecyclemodel no longer accepts --flow-scope-file; ' +
        'materialization must fetch the live remote flow scope through the TianGong CLI.',
    );
  }

  const { env, cliDir, dotenvPath } = buildRemoteFlowScopeEnv(options);
  if (!hasRemoteFlowScopeEnv(env)) {
    throw new Error(
      'remote flow scope credentials are required; expected ' +
        'TIANGONG_LCA_API_BASE_URL, TIANGONG_LCA_API_KEY, and ' +
        'TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY in the current environment or in ' +
        `the TianGong CLI .env${dotenvPath ? ` at ${dotenvPath}` : ''}.`,
    );
  }

  const outPath = options?.flowScopePath
    ? path.resolve(options.flowScopePath)
    : defaultRemoteFlowScopePath(options?.repoRoot ?? process.cwd());
  if (fs.existsSync(outPath)) return outPath;

  const tempOutPath = `${outPath}.tmp`;
  const invocation = buildTiangongInvocation(buildRemoteFlowListArgs(options), {
    repoRoot: options?.repoRoot,
    cliDir,
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
  return ensureRemoteFlowScopeFile(options);
}
