#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const googlePatentsOrigin = 'https://patents.google.com';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function fail(msg) {
  console.error(`Error: ${msg}`);
  process.exit(2);
}

function printHelp() {
  console.log(`
Usage:
  node product-to-patent/scripts/google-patents-download-fulltext.mjs [options]

Sources (at least one required):
  --metadata-file <path>   Read publications from google-patents-metadata.json,
                           ncm811-crawl-summary.json, or reviewed-candidates JSON
  --publications <list>    Comma-separated publication numbers,
                           e.g. CN113264560A,CN113224310A
  --seed-plan <path>       Read seed publications from a query plan JSON

Options:
  --out-dir <dir>          Output directory (default: output/raw)
  --delay <seconds>        Delay between requests (default: 25)
  --retries <n>            Retries per strategy (default: 2)
  --no-pdf                 Skip PDF download attempts
  --skip-existing          Skip publications that already have a non-empty file
  -h, --help               Show this help

Strategies (tried in order per publication):
  1. Jina Reader → patents.google.com/patent/{pub}/en
  2. Jina Reader → patents.google.com/patent/{pub}/{native-lang} (zh for CN, ko for KR)
  3. Direct fetch → patents.google.com/patent/{pub}/en (may be blocked)
  4. Jina Reader → patents.google.com/xhr/result?id=patent/{pub}/en
  5. PDF download (if URL available or discoverable)
`.trim());
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

function getNativeLang(pub) {
  if (pub.startsWith('CN')) return 'zh';
  if (pub.startsWith('KR')) return 'ko';
  if (pub.startsWith('JP')) return 'ja';
  if (pub.startsWith('DE')) return 'de';
  return 'en';
}

function extractPdfLinkFromText(text) {
  const patterns = [
    /\[Download PDF\]\((https:\/\/patentimages\.storage\.googleapis\.com\/[^)]+)\)/i,
    /href=["'](https:\/\/patentimages\.storage\.googleapis\.com\/[^"']+\.pdf)["']/i,
    /\((https:\/\/patentimages\.storage\.googleapis\.com\/[^)]+\.pdf)\)/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) return match[1];
  }
  return '';
}

function resolvePublications(options) {
  const seen = new Set();
  const publications = [];

  function add(pub) {
    const num = pub.publication_number;
    if (!num || seen.has(num)) return;
    seen.add(num);
    publications.push(pub);
  }

  if (options.publications) {
    for (const pub of options.publications.split(',').map(s => s.trim()).filter(Boolean)) {
      add({ publication_number: pub, pdf_url: '', title: '' });
    }
  }

  if (options.seedPlan) {
    const plan = JSON.parse(fs.readFileSync(options.seedPlan, 'utf8'));
    for (const seed of plan.seed_publications ?? []) {
      add({
        publication_number: seed.publication_number,
        pdf_url: '',
        title: seed.title_hint ?? '',
      });
    }
  }

  if (options.metadataFile) {
    const raw = JSON.parse(fs.readFileSync(options.metadataFile, 'utf8'));

    if (raw.results) {
      for (const r of raw.results) {
        add({
          publication_number: r.publication_number,
          pdf_url: r.pdf_link ?? r.pdf_url ?? '',
          title: r.title ?? '',
        });
      }
    }

    if (raw.candidates) {
      for (const c of raw.candidates) {
        add({
          publication_number: c.publication_number,
          pdf_url: c.pdf_link ?? c.pdf_url ?? '',
          title: c.title ?? '',
        });
      }
    }

    if (raw.selected_families) {
      for (const f of raw.selected_families) {
        add({
          publication_number: f.representative_publication_number,
          pdf_url: f.pdf_url ?? '',
          title: f.title ?? '',
        });
      }
    }
  }

  return publications;
}

async function fetchWithRetry(url, fetchOptions = {}, retries = 2, backoffMs = 30000) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    const response = await fetch(url, {
      signal: AbortSignal.timeout(fetchOptions.timeoutMs ?? 90000),
      ...fetchOptions,
    });
    const text = await response.text();
    if (response.ok) return { ok: true, text, status: response.status };
    if (response.status === 429 && attempt < retries) {
      const wait = backoffMs * (attempt + 1);
      console.log(`    429 rate limited, waiting ${wait / 1000}s (attempt ${attempt + 1}/${retries})...`);
      await sleep(wait);
      continue;
    }
    return { ok: false, text, status: response.status };
  }
  return { ok: false, text: '', status: 0 };
}

async function tryJinaPage(pubNum, lang) {
  const url = `https://r.jina.ai/https://patents.google.com/patent/${pubNum}/${lang}`;
  console.log(`  [jina-${lang}] fetching ${pubNum}...`);

  const { ok, text, status } = await fetchWithRetry(
    url,
    {
      headers: {
        accept: 'text/plain,*/*;q=0.8',
        'user-agent': 'tiangong-lca-skills/product-to-patent fulltext',
      },
    },
    2,
    25000,
  );

  if (!ok) return { ok: false, error: `HTTP ${status}`, content: '' };
  if (isGoogleBlock(text)) return { ok: false, error: 'Google block via Jina', content: '' };

  const content = extractJinaContent(text);
  if (content.length < 500) {
    return { ok: false, error: `too short (${content.length}B)`, content: '' };
  }

  const pdfUrl = extractPdfLinkFromText(text);
  return { ok: true, content, format: 'md', source: `jina-${lang}`, pdfUrl };
}

async function tryDirectPage(pubNum) {
  const url = `https://patents.google.com/patent/${pubNum}/en`;
  console.log(`  [direct] fetching ${pubNum}...`);

  const { ok, text, status } = await fetchWithRetry(
    url,
    {
      headers: {
        accept: 'text/html,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        referer: 'https://patents.google.com/',
        'user-agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
      },
      timeoutMs: 60000,
    },
    1,
    15000,
  );

  if (!ok) return { ok: false, error: `HTTP ${status}`, content: '' };
  if (/<title>\s*Sorry\.\.\.\s*<\/title>/i.test(text)) {
    return { ok: false, error: 'Google Sorry page', content: '' };
  }
  if (text.length < 1000) return { ok: false, error: `too short (${text.length}B)`, content: '' };

  const pdfUrl = extractPdfLinkFromText(text);
  return { ok: true, content: text, format: 'html', source: 'direct', pdfUrl };
}

async function tryJinaXhr(pubNum) {
  const xhrUrl = `${googlePatentsOrigin}/xhr/result?id=patent/${pubNum}/en`;
  const url = `https://r.jina.ai/${xhrUrl}`;
  console.log(`  [jina-xhr] fetching ${pubNum}...`);

  const { ok, text, status } = await fetchWithRetry(
    url,
    {
      headers: {
        accept: 'text/plain,*/*;q=0.8',
        'user-agent': 'tiangong-lca-skills/product-to-patent fulltext',
      },
    },
    1,
    20000,
  );

  if (!ok) return { ok: false, error: `HTTP ${status}`, content: '' };
  if (isGoogleBlock(text)) return { ok: false, error: 'Google block via Jina XHR', content: '' };

  const content = extractJinaContent(text);
  if (content.length < 500) {
    return { ok: false, error: `too short (${content.length}B)`, content: '' };
  }

  const pdfUrl = extractPdfLinkFromText(text);
  return { ok: true, content, format: 'md', source: 'jina-xhr', pdfUrl };
}

async function tryPdfDownload(pdfUrl) {
  if (!pdfUrl) return { ok: false, error: 'no PDF URL' };
  console.log(`  [pdf] downloading ${pdfUrl.slice(0, 80)}...`);

  const response = await fetch(pdfUrl, {
    signal: AbortSignal.timeout(120000),
    headers: { 'user-agent': 'tiangong-lca-skills/product-to-patent pdf' },
  });
  if (!response.ok) return { ok: false, error: `HTTP ${response.status}` };

  const buffer = Buffer.from(await response.arrayBuffer());
  if (buffer.length < 1000) return { ok: false, error: `too small (${buffer.length}B)` };
  return { ok: true, buffer, format: 'pdf', source: 'pdf' };
}

async function downloadPublication(pub, options) {
  const pubNum = pub.publication_number;
  const nativeLang = getNativeLang(pubNum);
  const delayMs = options.delay * 1000;
  const errors = [];
  let discoveredPdfUrl = pub.pdf_url || '';

  const textStrategies = [
    { name: `jina-en`, run: () => tryJinaPage(pubNum, 'en') },
    ...(nativeLang !== 'en'
      ? [
          {
            name: `jina-${nativeLang}`,
            run: () => tryJinaPage(pubNum, nativeLang),
          },
        ]
      : []),
    { name: 'direct', run: () => tryDirectPage(pubNum) },
    { name: 'jina-xhr', run: () => tryJinaXhr(pubNum) },
  ];

  for (const strategy of textStrategies) {
    const result = await strategy.run();
    if (result.ok) {
      if (result.pdfUrl && !discoveredPdfUrl) discoveredPdfUrl = result.pdfUrl;
      return { ...result, discoveredPdfUrl };
    }
    errors.push(`${strategy.name}: ${result.error}`);
    await sleep(delayMs);
  }

  if (options.downloadPdf && discoveredPdfUrl) {
    const pdfResult = await tryPdfDownload(discoveredPdfUrl);
    if (pdfResult.ok) return { ...pdfResult, discoveredPdfUrl, errors };
    errors.push(`pdf: ${pdfResult.error}`);
  }

  return { ok: false, error: errors.join('; '), content: '', discoveredPdfUrl, errors };
}

async function run(options) {
  const publications = resolvePublications(options);
  if (publications.length === 0) {
    fail('No publications to download. Provide --metadata-file, --publications, or --seed-plan.');
  }

  const outDir = options.outDir;
  fs.mkdirSync(outDir, { recursive: true });

  console.log(`Downloading full text for ${publications.length} publications → ${outDir}`);
  console.log(`Delay: ${options.delay}s | Retries: ${options.retries} | PDF: ${options.downloadPdf ? 'yes' : 'no'}`);
  console.log('');

  const results = [];

  for (let i = 0; i < publications.length; i++) {
    const pub = publications[i];
    const pubNum = pub.publication_number;
    const hint = pub.title ? ` — ${pub.title.slice(0, 60)}` : '';
    console.log(`[${i + 1}/${publications.length}] ${pubNum}${hint}`);

    if (options.skipExisting) {
      const mdPath = path.join(outDir, `${pubNum}.md`);
      const htmlPath = path.join(outDir, `${pubNum}.html`);
      if (
        (fs.existsSync(mdPath) && fs.statSync(mdPath).size > 1000) ||
        (fs.existsSync(htmlPath) && fs.statSync(htmlPath).size > 1000)
      ) {
        console.log('  skipped (already downloaded)');
        results.push({ publication_number: pubNum, status: 'skipped', source: 'existing', bytes: 0 });
        continue;
      }
    }

    const result = await downloadPublication(pub, options);

    if (result.ok) {
      const ext = result.format;
      const filePath = path.join(outDir, `${pubNum}.${ext}`);
      if (result.format === 'pdf') {
        fs.writeFileSync(filePath, result.buffer);
      } else {
        fs.writeFileSync(filePath, result.content);
      }
      const bytes = result.format === 'pdf' ? result.buffer.length : Buffer.byteLength(result.content);
      console.log(`  ✓ ${bytes} bytes via ${result.source} → ${pubNum}.${ext}`);

      if (result.discoveredPdfUrl && options.downloadPdf) {
        const pdfPath = path.join(outDir, `${pubNum}.pdf`);
        if (!fs.existsSync(pdfPath)) {
          const pdfResult = await tryPdfDownload(result.discoveredPdfUrl);
          if (pdfResult.ok) {
            fs.writeFileSync(pdfPath, pdfResult.buffer);
            console.log(`  ✓ PDF ${pdfResult.buffer.length} bytes → ${pubNum}.pdf`);
          }
        }
      }

      results.push({
        publication_number: pubNum,
        status: 'ok',
        source: result.source,
        format: result.format,
        bytes,
      });
    } else {
      console.log(`  ✗ failed: ${result.error}`);
      results.push({
        publication_number: pubNum,
        status: 'failed',
        source: '',
        bytes: 0,
        errors: [result.error],
      });
    }

    if (i < publications.length - 1) {
      console.log(`  waiting ${options.delay}s...\n`);
      await sleep(options.delay * 1000);
    }
  }

  const summary = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    total: results.length,
    succeeded: results.filter(r => r.status === 'ok').length,
    skipped: results.filter(r => r.status === 'skipped').length,
    failed: results.filter(r => r.status === 'failed').length,
    options: { delay: options.delay, retries: options.retries, download_pdf: options.downloadPdf },
    results,
  };

  const summaryPath = path.join(outDir, 'download-summary.json');
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);

  console.log(`\n${summary.succeeded} succeeded / ${summary.skipped} skipped / ${summary.failed} failed of ${summary.total}`);
  console.log(`Summary → ${summaryPath}`);

  return summary;
}

function parseArgs(rawArgs) {
  const options = {
    outDir: 'output/raw',
    delay: 25,
    retries: 2,
    downloadPdf: true,
    skipExisting: false,
  };

  for (let i = 0; i < rawArgs.length; i++) {
    const arg = rawArgs[i];
    switch (arg) {
      case '-h':
      case '--help':
        options.help = true;
        break;
      case '--metadata-file':
      case '--publications':
      case '--seed-plan':
      case '--out-dir':
      case '--delay':
      case '--retries':
        if (i + 1 >= rawArgs.length) throw new Error(`${arg} requires a value`);
        options[arg.slice(2).replace(/-([a-z])/gu, (_, c) => c.toUpperCase())] = rawArgs[++i];
        break;
      case '--no-pdf':
        options.downloadPdf = false;
        break;
      case '--skip-existing':
        options.skipExisting = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  options.delay = Number.parseInt(options.delay, 10);
  options.retries = Number.parseInt(options.retries, 10);
  return options;
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

  if (!options.metadataFile && !options.publications && !options.seedPlan) {
    fail('Provide --metadata-file, --publications, or --seed-plan.');
  }

  run(options).catch(error => {
    fail(error instanceof Error ? error.message : String(error));
  });
}
