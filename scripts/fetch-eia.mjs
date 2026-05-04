#!/usr/bin/env node
// Auto-discover and download Chinese EIA PDF reports for lithium-battery materials,
// into data/EIA/. See --help for full usage.

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, '..');
const DEFAULT_OUTPUT_DIR = path.join(ROOT, 'data/EIA');
const MANIFEST_NAME = '_fetch_manifest.json';

const HELP = `Usage: node scripts/fetch-eia.mjs [options]

Auto-discover and download Chinese EIA PDFs for lithium-battery material plants
(cathode, anode, precursor, electrolyte, separator).

Options:
  --query <q>           Add a search keyword (repeatable). Defaults to a curated list.
  --max <n>             Max successful downloads per run (default 20).
  --max-per-query <n>   Max candidates per query (default 8).
  --output-dir <dir>    Output directory (default data/EIA/).
  --min-size <kb>       Discard PDFs smaller than this (default 200 KB).
  --min-score <s>       Minimum relevance score (default 6).
  --dry-run             Resolve and score candidates but do not download.
  --user-agent <ua>     HTTP User-Agent string.
  --timeout <ms>        Per-request timeout (default 30000 ms).
  --sleep <ms>          Delay between search/download requests (default 800 ms).
  -h, --help            Show this message and exit.

Manifest written to: <output-dir>/_fetch_manifest.json
`;

const DEFAULT_QUERIES = [
  '环境影响报告 NCM 正极材料 filetype:pdf',
  '环境影响报告 锂离子电池 三元 正极 filetype:pdf',
  '环境影响报告 LFP 磷酸铁锂 正极 filetype:pdf',
  '环境影响报告 负极材料 石墨 filetype:pdf',
  '环境影响报告 锂电池 前驱体 三元 filetype:pdf',
  '环境影响报告 电解液 锂离子 filetype:pdf',
  '环境影响报告 隔膜 锂电 filetype:pdf',
  '环境影响报告书 锂离子 正极 公示 filetype:pdf',
  '环境影响报告 容百科技 NCM filetype:pdf',
  '环境影响报告 当升科技 正极 filetype:pdf',
  '环境影响报告 湖南裕能 LFP filetype:pdf',
  '环境影响报告 贝特瑞 负极 filetype:pdf',
  '环境影响报告 厦钨新能 正极 filetype:pdf',
  '环境影响报告 长远锂科 NCM filetype:pdf',
  '环境影响报告 巴莫科技 NCM filetype:pdf',
  '环境影响报告 富临精工 LFP filetype:pdf',
  '环境影响报告 万润新能 正极 filetype:pdf',
  '环境影响报告 杉杉 负极 filetype:pdf',
  '环境影响报告 璞泰来 负极 filetype:pdf',
  '环境影响报告 天赐 电解液 filetype:pdf',
  '环境影响报告 新宙邦 电解液 filetype:pdf',
  '环境影响报告 恩捷 隔膜 filetype:pdf',
  '环境影响报告 中伟 三元前驱体 filetype:pdf',
  '环境影响报告 格林美 三元前驱体 filetype:pdf',
  '环境影响报告 华友钴业 锂电材料 filetype:pdf',
  '建设项目环境影响报告表 锂电池正极 filetype:pdf',
  '建设项目环境影响报告书 三元正极 filetype:pdf',
];

const KW = {
  high: [
    '环境影响报告', '环境影响评价', '建设项目环评', '环境影响报告表',
    '环境影响报告书', '环评公示', '环评报告', '环境影响登记表',
  ],
  med: [
    '正极', '负极', '锂电', '锂离子', '锂电池', 'ncm', '三元', 'lfp',
    '磷酸铁锂', '前驱体', '电解液', '硫酸镍', '硫酸钴', '硫酸锰',
    'ncm622', 'ncm811', 'lco', 'lmo', 'nca', '钴酸锂', '锰酸锂',
    '钛酸锂', '碳酸锂', '氢氧化锂',
  ],
  low: [
    '新能源', '电池材料', '石墨', '隔膜', '动力电池', '储能',
    '锂', '钴', '镍', '锰',
  ],
};
const NEGATIVE = ['招标', '中标', '采购', '招投标', '广告'];

function parseArgs(argv) {
  const opts = {
    queries: [],
    max: 20,
    maxPerQuery: 8,
    outputDir: DEFAULT_OUTPUT_DIR,
    minSizeKb: 200,
    minScore: 6,
    dryRun: false,
    userAgent:
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    timeout: 30000,
    sleepMs: 800,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '-h' || a === '--help') { process.stdout.write(HELP); process.exit(0); }
    else if (a === '--query') opts.queries.push(argv[++i]);
    else if (a === '--max') opts.max = +argv[++i];
    else if (a === '--max-per-query') opts.maxPerQuery = +argv[++i];
    else if (a === '--output-dir') opts.outputDir = path.resolve(argv[++i]);
    else if (a === '--min-size') opts.minSizeKb = +argv[++i];
    else if (a === '--min-score') opts.minScore = +argv[++i];
    else if (a === '--dry-run') opts.dryRun = true;
    else if (a === '--user-agent') opts.userAgent = argv[++i];
    else if (a === '--timeout') opts.timeout = +argv[++i];
    else if (a === '--sleep') opts.sleepMs = +argv[++i];
    else { process.stderr.write(`Unknown argument: ${a}\n${HELP}`); process.exit(1); }
  }
  if (opts.queries.length === 0) opts.queries = [...DEFAULT_QUERIES];
  return opts;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithTimeout(url, init = {}, timeoutMs = 30000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: ctrl.signal, redirect: 'follow' });
  } finally {
    clearTimeout(t);
  }
}

function htmlDecode(s) {
  return s
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x2F;/g, '/');
}

async function searchDuckDuckGo(query, ua, timeoutMs) {
  const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': ua, 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.5' } },
    timeoutMs,
  );
  if (!res.ok) throw new Error(`DDG HTTP ${res.status}`);
  const html = await res.text();
  const out = [];
  const re = /<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    let raw = htmlDecode(m[1]);
    const title = htmlDecode(m[2].replace(/<[^>]+>/g, '')).trim();
    const uddg = /[?&]uddg=([^&]+)/.exec(raw);
    const finalUrl = uddg ? decodeURIComponent(uddg[1]) : raw;
    out.push({ title, url: finalUrl, source: 'ddg' });
  }
  return out;
}

async function searchBrave(query, ua, timeoutMs) {
  const url = `https://search.brave.com/search?q=${encodeURIComponent(query)}`;
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': ua, 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.5' } },
    timeoutMs,
  );
  if (!res.ok) throw new Error(`Brave HTTP ${res.status}`);
  const html = await res.text();
  const out = [];
  const seen = new Set();
  const re = /<a[^>]*href="(https?:\/\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const url = htmlDecode(m[1]);
    if (seen.has(url)) continue;
    if (/(?:^|\.)brave\.com\//i.test(url)) continue;
    if (/(?:^|\.)duckduckgo\.com\//i.test(url)) continue;
    if (/^https:\/\/search\./i.test(url)) continue;
    const title = htmlDecode(m[2].replace(/<[^>]+>/g, '')).trim();
    if (!title) continue;
    seen.add(url);
    out.push({ title, url, source: 'brave' });
  }
  return out;
}

function scoreCandidate(c) {
  const txt = (c.title + ' ' + c.url).toLowerCase();
  let s = 0;
  for (const k of KW.high) if (txt.includes(k.toLowerCase())) s += 5;
  for (const k of KW.med) if (txt.includes(k.toLowerCase())) s += 2;
  for (const k of KW.low) if (txt.includes(k.toLowerCase())) s += 1;
  for (const k of NEGATIVE) if (txt.includes(k.toLowerCase())) s -= 3;
  if (/\.pdf(\?|#|$)/i.test(c.url)) s += 5;
  if (/\.gov\.cn(\/|$)/i.test(c.url)) s += 4;
  if (/\.com\.cn(\/|$)/i.test(c.url)) s += 1;
  return s;
}

function safeName(s) {
  return s.replace(/[\\\/:*?"<>|\x00-\x1f]/g, '_').replace(/\s+/g, ' ').trim().slice(0, 180);
}

function inferFilename(url, title) {
  let base = '';
  try {
    const u = new URL(url);
    base = path.basename(u.pathname);
  } catch { /* ignore */ }
  if (!base || !/\.pdf$/i.test(base)) {
    base = (safeName(title) || 'eia') + '.pdf';
  }
  base = decodeURIComponent(base);
  return safeName(base);
}

async function downloadPdf(url, ua, timeoutMs, minBytes) {
  const res = await fetchWithTimeout(
    url,
    { headers: { 'User-Agent': ua, Accept: 'application/pdf,*/*' } },
    timeoutMs,
  );
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  if (buf.length < minBytes) throw new Error(`too small: ${buf.length} bytes`);
  if (buf.slice(0, 5).toString() !== '%PDF-') throw new Error('not a PDF (no %PDF- header)');
  return buf;
}

const sha256 = (buf) => createHash('sha256').update(buf).digest('hex');

async function readManifest(p) {
  try {
    return JSON.parse(await fs.promises.readFile(p, 'utf8'));
  } catch {
    return { fetched_at: null, entries: [] };
  }
}

async function writeManifest(p, m) {
  m.fetched_at = new Date().toISOString();
  await fs.promises.writeFile(p, JSON.stringify(m, null, 2));
}

async function hashExistingPdfs(dir) {
  const map = new Map();
  if (!fs.existsSync(dir)) return map;
  const items = await fs.promises.readdir(dir);
  for (const f of items) {
    if (!f.toLowerCase().endsWith('.pdf')) continue;
    const fp = path.join(dir, f);
    try {
      const buf = await fs.promises.readFile(fp);
      map.set(sha256(buf), fp);
    } catch { /* skip unreadable */ }
  }
  return map;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  await fs.promises.mkdir(opts.outputDir, { recursive: true });
  const manifestPath = path.join(opts.outputDir, MANIFEST_NAME);
  const manifest = await readManifest(manifestPath);
  const existingHashes = await hashExistingPdfs(opts.outputDir);
  const knownHashes = new Set([...existingHashes.keys(), ...manifest.entries.map((e) => e.sha256)]);
  const knownUrls = new Set(manifest.entries.map((e) => e.url));

  console.log(
    `[fetch-eia] queries=${opts.queries.length} max=${opts.max} dry=${opts.dryRun} ` +
      `existing_pdfs=${existingHashes.size} -> ${path.relative(ROOT, opts.outputDir)}`,
  );

  const seenUrls = new Set();
  const candidates = [];
  for (const q of opts.queries) {
    let pool = [];
    for (const fn of [searchDuckDuckGo, searchBrave]) {
      try {
        const r = await fn(q, opts.userAgent, opts.timeout);
        pool = pool.concat(r);
        await sleep(opts.sleepMs);
      } catch (e) {
        console.warn(`[search] ${fn.name} '${q}' -> ${e.message}`);
      }
    }
    let n = 0;
    for (const r of pool) {
      if (!r.url || seenUrls.has(r.url)) continue;
      seenUrls.add(r.url);
      const score = scoreCandidate(r);
      candidates.push({ ...r, score, query: q });
      if (++n >= opts.maxPerQuery) break;
    }
  }
  candidates.sort((a, b) => b.score - a.score);
  console.log(`[fetch-eia] candidates=${candidates.length}`);

  const downloaded = [];
  for (const c of candidates) {
    if (downloaded.length >= opts.max) break;
    if (c.score < opts.minScore) continue;
    if (knownUrls.has(c.url)) {
      console.log(`[skip] manifest: ${c.url}`);
      continue;
    }
    console.log(`[try ] (${c.score}) ${c.title.slice(0, 80)}`);
    console.log(`       ${c.url}`);
    if (opts.dryRun) {
      downloaded.push({ ...c, dry: true });
      continue;
    }
    try {
      const buf = await downloadPdf(c.url, opts.userAgent, opts.timeout, opts.minSizeKb * 1024);
      const hash = sha256(buf);
      if (knownHashes.has(hash)) {
        console.log(`[dup ] ${hash.slice(0, 12)} matches existing file`);
        continue;
      }
      const filename = inferFilename(c.url, c.title);
      const outPath = path.join(opts.outputDir, filename);
      let finalPath = outPath;
      let n = 1;
      while (fs.existsSync(finalPath)) {
        const ext = path.extname(filename) || '.pdf';
        const stem = filename.slice(0, -ext.length);
        finalPath = path.join(opts.outputDir, `${stem}__${n++}${ext}`);
      }
      await fs.promises.writeFile(finalPath, buf);
      const entry = {
        title: c.title,
        url: c.url,
        query: c.query,
        score: c.score,
        sha256: hash,
        size_bytes: buf.length,
        local_path: path.relative(ROOT, finalPath),
        downloaded_at: new Date().toISOString(),
      };
      manifest.entries.push(entry);
      knownHashes.add(hash);
      knownUrls.add(c.url);
      downloaded.push(entry);
      console.log(`[ok  ] ${path.basename(finalPath)} (${(buf.length / 1024).toFixed(0)} KB)`);
      await sleep(opts.sleepMs);
    } catch (e) {
      console.warn(`[fail] ${e.message}`);
    }
  }

  if (!opts.dryRun) await writeManifest(manifestPath, manifest);
  console.log(`[done] downloaded=${downloaded.length} manifest_total=${manifest.entries.length}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
