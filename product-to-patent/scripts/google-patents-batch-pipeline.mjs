#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import {
  downloadPatentFigureImages,
  extractPatentFigureImageLinks,
} from './google-patents-download-fulltext.mjs';

const googlePatentsOrigin = 'https://patents.google.com';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function fail(msg) {
  console.error(`Error: ${msg}`);
  process.exit(2);
}

function extractJinaContent(text) {
  const marker = 'Markdown Content:';
  const index = text.indexOf(marker);
  if (index === -1) return text;
  return text.slice(index + marker.length).trim();
}

function isGoogleBlock(text) {
  return /automated queries|We're sorry|Sorry\.\.\./i.test(text);
}

function extractPatentNumbers(text) {
  const numbers = new Set();
  const patterns = [
    /patent\/([A-Z]{2}[A-Z0-9]+)\/[a-z]{2}/giu,
    /\[([A-Z]{2}[A-Z0-9]+)\s+\(en\)\]/giu,
    /\b([A-Z]{2}\d{6,12}[AB]\d?)\b/gu,
  ];
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      const num = match[1];
      if (num && num.length >= 6 && num.length <= 16) numbers.add(num);
    }
  }
  return [...numbers];
}

function extractSection(text, heading) {
  const pattern = new RegExp(`^##\\s+${heading.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}[^\\n]*$`, 'imu');
  const startMatch = pattern.exec(text);
  if (!startMatch) return '';
  const start = startMatch.index + startMatch[0].length;
  const nextHeading = text.slice(start).search(/^##\s+/imu);
  return nextHeading === -1 ? text.slice(start) : text.slice(start, start + nextHeading);
}

function extractCitationLinks(text) {
  const cited = extractPatentNumbers(extractSection(text, 'Patent Citations'));
  const citedBy = extractPatentNumbers(extractSection(text, 'Cited By'));
  const similar = extractPatentNumbers(extractSection(text, 'Similar Documents'));
  return { cited, citedBy, similar };
}

function extractMetaFromText(text) {
  const firstLine = text.split('\n')[0] || '';
  const titleMatch = firstLine.match(/^[A-Z]{2}[A-Z0-9]+\s+-\s+(.+?)\s+-\s+Google Patents/iu);
  const title = titleMatch?.[1] || '';
  const pubNumMatch = text.match(/Publication number\s+([A-Z]{2}[A-Z0-9]+)/i);
  const publication_number = pubNumMatch?.[1] || firstLine.match(/^([A-Z]{2}[A-Z0-9]+)/)?.[1] || '';
  const pdMatch = text.match(/Priority date\s+(\d{4}-\d{2}-\d{2})/i);
  const fdMatch = text.match(/Filing date\s+(\d{4}-\d{2}-\d{2})/i);
  const pubdMatch = text.match(/Publication date\s+(\d{4}-\d{2}-\d{2})/i);
  const gdMatch = text.match(/Grant date\s+(\d{4}-\d{2}-\d{2})/i);
  const inventorMatch = text.match(/Inventor\s+([\p{L}\s]+?)(?:Current Assignee|Original Assignee|Priority date|$)/iu);
  const assigneeMatch = text.match(/Current Assignee[^]*?(\w[\w\s&,.]+?)(?:Original Assignee|Priority date|$)/i);
  const pdfMatch = text.match(/\[Download PDF\]\((https:\/\/patentimages\.storage\.googleapis\.com\/[^)]+)\)/i);
  const abstractMatch = text.match(/Abstract\s+(.*?)(?:Images|Classifications|Description|Claims)/is);
  const abstract = abstractMatch?.[1]?.replace(/\s+/g, ' ').trim().slice(0, 500) || '';
  const cpcMatches = [...text.matchAll(/H01M\d+\/\d+/g)];
  const cpc_codes = [...new Set(cpcMatches.map(m => m[0]))];
  return {
    publication_number,
    title,
    assignee: assigneeMatch?.[1]?.trim() || '',
    inventor: inventorMatch?.[1]?.trim() || '',
    priority_date: pdMatch?.[1] || '',
    filing_date: fdMatch?.[1] || '',
    publication_date: pubdMatch?.[1] || '',
    grant_date: gdMatch?.[1] || '',
    pdf_link: pdfMatch?.[1] || '',
    abstract,
    cpc_codes,
  };
}

function loadSeedFiles(outDir) {
  const seeds = [];
  const dirs = ['data', outDir];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) continue;
    for (const file of fs.readdirSync(dir)) {
      if (!file.endsWith('.txt') && !file.endsWith('.md')) continue;
      const pubNum = file.replace(/\.(txt|md)$/, '');
      if (/^[A-Z]{2}[A-Z0-9]+$/i.test(pubNum)) {
        seeds.push(pubNum);
      }
    }
  }
  return [...new Set(seeds)];
}

function loadSeedPlan(planPath) {
  if (!fs.existsSync(planPath)) return [];
  const plan = JSON.parse(fs.readFileSync(planPath, 'utf8'));
  return (plan.seed_publications ?? []).map(s => s.publication_number);
}

async function downloadViaJina(pubNum) {
  const url = `https://r.jina.ai/https://patents.google.com/patent/${pubNum}/en`;
  const res = await fetch(url, {
    headers: { accept: 'text/plain', 'user-agent': 'tiangong-lca-skills/batch-pipeline' },
    signal: AbortSignal.timeout(90000),
  });
  const text = await res.text();
  if (!res.ok) return { ok: false, error: `HTTP ${res.status}`, content: '' };
  if (isGoogleBlock(text)) return { ok: false, error: 'Google block', content: '' };
  const content = extractJinaContent(text);
  if (content.length < 300) return { ok: false, error: `too short (${content.length}B)`, content: '' };
  return { ok: true, content };
}

async function downloadViaJinaNative(pubNum) {
  const lang = pubNum.startsWith('CN') ? 'zh' : pubNum.startsWith('KR') ? 'ko' : null;
  if (!lang) return { ok: false, error: 'no native lang', content: '' };
  const url = `https://r.jina.ai/https://patents.google.com/patent/${pubNum}/${lang}`;
  const res = await fetch(url, {
    headers: { accept: 'text/plain', 'user-agent': 'tiangong-lca-skills/batch-pipeline' },
    signal: AbortSignal.timeout(90000),
  });
  const text = await res.text();
  if (!res.ok) return { ok: false, error: `HTTP ${res.status}`, content: '' };
  if (isGoogleBlock(text)) return { ok: false, error: 'Google block', content: '' };
  const content = extractJinaContent(text);
  if (content.length < 300) return { ok: false, error: `too short (${content.length}B)`, content: '' };
  return { ok: true, content };
}

async function run(options) {
  const { outDir, targetCount, downloadDelay, maxDepth, skipExisting, downloadImages, imageMode } = options;
  fs.mkdirSync(outDir, { recursive: true });

  const seedPaths = [
    path.join('product-to-patent', 'assets', 'ncm811-query-plan.json'),
  ];
  let seeds = loadSeedFiles(outDir);
  for (const p of seedPaths) {
    seeds.push(...loadSeedPlan(p));
  }
  seeds = [...new Set(seeds)];

  console.log(`\n=== NCM811 Batch Pipeline ===`);
  console.log(`Seeds: ${seeds.length} | Target: ${targetCount} | Max depth: ${maxDepth} | Delay: ${downloadDelay}s`);
  console.log(`Images: ${downloadImages ? imageMode : 'no'}`);
  console.log(`Output: ${outDir}/\n`);

  const visited = new Set(seeds);
  const queue = seeds.map(s => ({ pubNum: s, depth: 0, parent: 'seed' }));
  const results = [];
  let ok = 0;
  let failed = 0;
  let skipped = 0;
  let discoveredLinks = 0;

  while (queue.length > 0 && results.length < targetCount) {
    const { pubNum, depth, parent } = queue.shift();

    const txtFile = path.join(outDir, `${pubNum}.txt`);
    let content = '';
    let status = '';
    let downloadSource = '';
    let meta = {};
    let citations = { cited: [], citedBy: [], similar: [] };
    let figureImages = [];

    if (skipExisting && fs.existsSync(txtFile) && fs.statSync(txtFile).size > 500) {
      content = fs.readFileSync(txtFile, 'utf8');
      meta = extractMetaFromText(content);
      citations = extractCitationLinks(content);
      status = 'skipped';
      downloadSource = 'existing';
      skipped++;
      if (downloadImages) {
        const figureImageLinks = extractPatentFigureImageLinks(content, { mode: imageMode });
        figureImages = await downloadPatentFigureImages(pubNum, figureImageLinks, outDir);
      }
    } else {
      let result = await downloadViaJina(pubNum);

      if (!result.ok && pubNum.startsWith('CN')) {
        await sleep(2000);
        result = await downloadViaJinaNative(pubNum);
      }

      if (!result.ok) {
        status = 'failed';
        downloadSource = '';
        failed++;
        if (failed <= 50) {
          console.log(`  ✗ ${pubNum}: ${result.error}`);
        }
      } else {
        content = result.content;
        fs.writeFileSync(txtFile, content);
        meta = extractMetaFromText(content);
        citations = extractCitationLinks(content);
        status = 'ok';
        downloadSource = 'jina';
        ok++;
        if (downloadImages) {
          const figureImageLinks = extractPatentFigureImageLinks(content, { mode: imageMode });
          figureImages = await downloadPatentFigureImages(pubNum, figureImageLinks, outDir);
        }
      }
    }

    results.push({
      publication_number: pubNum,
      ...meta,
      link: `https://patents.google.com/patent/${pubNum}/en`,
      status,
      file: status !== 'failed' ? `${pubNum}.txt` : '',
      bytes: status !== 'failed' ? Buffer.byteLength(content) : 0,
      download_source: downloadSource,
      discovery_depth: depth,
      discovery_parent: parent,
      cited_count: citations.cited.length,
      cited_by_count: citations.citedBy.length,
      similar_count: citations.similar.length,
      figure_image_count: figureImages.filter(image => image.status === 'ok').length,
      figure_images: figureImages,
    });

    if (status !== 'failed' && depth < maxDepth) {
      const newLinks = [...citations.cited, ...citations.citedBy, ...citations.similar];
      for (const link of newLinks) {
        if (!visited.has(link)) {
          visited.add(link);
          queue.push({ pubNum: link, depth: depth + 1, parent: pubNum });
          discoveredLinks++;
        }
      }
    }

    const total = ok + failed + skipped;
    if (total % 50 === 0 || total === 1) {
      console.log(
        `[${total}/${targetCount}] ok=${ok} skip=${skipped} fail=${failed} queue=${queue.length} depth=${depth}`,
      );
    }

    if (status !== 'skipped' && results.length < targetCount) {
      await sleep(downloadDelay * 1000);
    }
  }

  console.log(`\n=== Complete: ${ok} ok, ${skipped} skipped, ${failed} failed of ${results.length} total ===`);
  console.log(`Discovered ${discoveredLinks} citation links, ${queue.length} remaining in queue`);

  const summary = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    product: 'NCM811 cathode active material',
    pipeline: {
      seeds: seeds.length,
      max_depth: maxDepth,
      target_count: targetCount,
      download_delay: downloadDelay,
      download_images: downloadImages,
      image_mode: imageMode,
    },
    totals: {
      total: results.length,
      succeeded: ok + skipped,
      downloaded: ok,
      skipped,
      failed,
      citation_links_discovered: discoveredLinks,
    },
    results,
  };

  const summaryPath = path.join(outDir, 'download-summary.json');
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(`Summary → ${summaryPath}`);
}

function parseArgs(rawArgs) {
  const options = {
    outDir: 'output/raw',
    targetCount: 800,
    downloadDelay: 6,
    maxDepth: 2,
    skipExisting: true,
    downloadImages: false,
    imageMode: 'flow',
  };

  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i];
    switch (arg) {
      case '-h':
      case '--help':
        options.help = true;
        break;
      case '--out-dir':
      case '--target-count':
      case '--download-delay':
      case '--max-depth':
      case '--image-mode':
        if (i + 1 >= rawArgs.length) throw new Error(`${arg} requires a value`);
        options[arg.slice(2).replace(/-([a-z])/gu, (_, c) => c.toUpperCase())] = rawArgs[++i];
        break;
      case '--download-images':
        options.downloadImages = true;
        break;
      case '--no-skip-existing':
        options.skipExisting = false;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  options.targetCount = Number.parseInt(options.targetCount, 10);
  options.downloadDelay = Number.parseInt(options.downloadDelay, 10);
  options.maxDepth = Number.parseInt(options.maxDepth, 10);
  if (!['flow', 'all'].includes(options.imageMode)) {
    throw new Error('--image-mode must be "flow" or "all"');
  }

  return options;
}

function printHelp() {
  console.log(`
Usage:
  node product-to-patent/scripts/google-patents-batch-pipeline.mjs [options]

NCM811 Batch Pipeline using citation-chain BFS discovery.

Starts from seed patents (existing data/ files, output/raw/ files, and
assets/ncm811-query-plan.json seeds). Downloads each patent via Jina Reader,
extracts full text, metadata, and citation links. Follows cited/citing/similar
patent links to discover more. Continues until target count is reached.

Output:
  <out-dir>/<PUBNUM>.txt           Full text for each patent
  <out-dir>/download-summary.json  Metadata + discovery info for every patent

Options:
  --out-dir <dir>          Output directory (default: output/raw)
  --target-count <n>       Target number of patents (default: 800)
  --download-delay <s>     Seconds between downloads (default: 6)
  --max-depth <n>          Citation chain depth: 0=seeds only, 1=direct, 2=indirect (default: 2)
  --download-images        Download patent figure images discovered in downloaded text
  --image-mode <mode>      Image selection mode: flow or all (default: flow)
  --no-skip-existing       Re-download already-present .txt files
  -h, --help               Show this help
`.trim());
}

if (import.meta.url === `file://${process.argv[1]}`) {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    fail(error instanceof Error ? error.message : String(error));
  }

  if (options.help) {
    printHelp();
    process.exit(0);
  }

  run(options).catch(error => {
    fail(error instanceof Error ? error.message : String(error));
  });
}
