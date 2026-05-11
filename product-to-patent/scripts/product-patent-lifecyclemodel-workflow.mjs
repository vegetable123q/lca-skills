#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import {
  buildPatentSourceFromGooglePatentsResult,
} from '../../patent-to-lifecyclemodel/scripts/patent-metadata.mjs';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const skillDir = path.resolve(scriptDir, '..');
const repoRoot = path.resolve(skillDir, '..');

function fail(message) {
  console.error(`Error: ${message}`);
  process.exit(2);
}

function printHelp() {
  console.log(`
Usage:
  node product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs --query <query> [options]

Options:
  --query <text>              Product/process query for Google Patents
  --product-name <name>       Product label for workflow metadata
  --out-dir <dir>             Workflow output dir (default: output/product-to-patent-lifecyclemodel/<query-slug>)
  --max-results <n>           Number of Google Patents candidates (default: 10)
  --fetcher <mode>            Metadata fetcher: auto, direct, or jina (default: auto)
  --delay <seconds>           Full-text download delay (default: 25)
  --download-images           Download process-flow candidate patent figures
  --image-mode <mode>         Image mode: flow or all (default: flow)
  --skip-existing             Reuse existing downloaded patent text
  --run-lifecyclemodels       Run patent-to-lifecyclemodel for patents with an authored plan.json
  --json                      Print workflow manifest JSON
  -h, --help                  Show this help

Outputs:
  workflow-manifest.json
  metadata/google-patents-metadata.json
  raw/download-summary.json
  patents/<PUBLICATION>/source-metadata.json
  patents/<PUBLICATION>/plan-source.json
`.trim());
}

function slugify(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-|-$/gu, '')
    .slice(0, 80) || 'product-patent-lifecyclemodel';
}

function toPosix(value) {
  return value.split(path.sep).join('/');
}

function relativePath(fromDir, targetPath) {
  return toPosix(path.relative(fromDir, targetPath));
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => {
      if (Array.isArray(entry)) return entry.length > 0;
      return entry !== undefined && entry !== null && entry !== '';
    }),
  );
}

function existingTextFileName(rawDir, publicationNumber) {
  for (const extension of ['md', 'html', 'txt']) {
    const fileName = `${publicationNumber}.${extension}`;
    if (fs.existsSync(path.join(rawDir, fileName))) return fileName;
  }
  return '';
}

function downloadTextFileName(result, rawDir, publicationNumber) {
  if (!result || result.status === 'failed') return '';
  if (result.file) return result.file;
  if (result.format) return `${result.publication_number}.${result.format}`;
  return existingTextFileName(rawDir, publicationNumber);
}

function buildDownloadInfo({ publicationNumber, downloadResult, outDir, rawDir }) {
  const status = downloadResult?.status || 'not_downloaded';
  const textFileName = downloadTextFileName(downloadResult, rawDir, publicationNumber);
  const figureImageFiles = (downloadResult?.figure_images || [])
    .filter((image) => image.status === 'ok' && image.file)
    .map((image) => relativePath(outDir, path.join(rawDir, image.file)));

  return compactObject({
    status,
    source: downloadResult?.source || '',
    text_file: textFileName ? relativePath(outDir, path.join(rawDir, textFileName)) : '',
    pdf_file: fs.existsSync(path.join(rawDir, `${publicationNumber}.pdf`))
      ? relativePath(outDir, path.join(rawDir, `${publicationNumber}.pdf`))
      : '',
    figure_image_files: figureImageFiles,
  });
}

export function buildLifecyclemodelWorkflowManifest({
  productName,
  query,
  metadata,
  downloadSummary = {},
  outDir,
  metadataFile,
  rawDir,
}) {
  const downloadByPublication = new Map(
    (downloadSummary.results || []).map((result) => [result.publication_number, result]),
  );

  const patents = (metadata.results || []).map((result) => {
    const publicationNumber = result.publication_number;
    const source = buildPatentSourceFromGooglePatentsResult(result, {
      query: query || metadata.query,
      productName,
    });
    const patentDir = path.join('patents', publicationNumber);
    const lifecyclemodelDir = path.join(patentDir, 'lifecyclemodel');
    const download = buildDownloadInfo({
      publicationNumber,
      downloadResult: downloadByPublication.get(publicationNumber),
      outDir,
      rawDir,
    });

    return {
      publication_number: publicationNumber,
      source,
      files: {
        source_metadata: toPosix(path.join(patentDir, 'source-metadata.json')),
        plan_source: toPosix(path.join(patentDir, 'plan-source.json')),
      },
      google_patents: compactObject({
        url: source.url,
        pdf_url: source.pdf_url,
        metadata_rank: source.google_patents_rank,
      }),
      download,
      lifecyclemodel: {
        status: 'needs_plan',
        base_dir: toPosix(lifecyclemodelDir),
        plan_file: toPosix(path.join(lifecyclemodelDir, 'plan.json')),
      },
    };
  });

  return {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    product: compactObject({
      name: productName || '',
      query: query || metadata.query || '',
    }),
    search: compactObject({
      query: query || metadata.query || '',
      public_url: metadata.public_url || '',
      metadata_file: metadataFile ? relativePath(outDir, metadataFile) : '',
    }),
    downloads: compactObject({
      raw_dir: rawDir ? relativePath(outDir, rawDir) : '',
      summary_file: rawDir ? relativePath(outDir, path.join(rawDir, 'download-summary.json')) : '',
    }),
    patents,
  };
}

function parseArgs(rawArgs) {
  const options = {
    maxResults: 10,
    fetcher: 'auto',
    delay: 25,
    downloadImages: false,
    imageMode: 'flow',
    skipExisting: false,
    runLifecyclemodels: false,
    json: false,
  };

  for (let index = 0; index < rawArgs.length; index += 1) {
    const arg = rawArgs[index];
    switch (arg) {
      case '-h':
      case '--help':
        options.help = true;
        break;
      case '--query':
      case '--product-name':
      case '--out-dir':
      case '--max-results':
      case '--fetcher':
      case '--delay':
      case '--image-mode':
        if (index + 1 >= rawArgs.length) throw new Error(`${arg} requires a value`);
        options[arg.slice(2).replace(/-([a-z])/gu, (_match, char) => char.toUpperCase())] =
          rawArgs[index + 1];
        index += 1;
        break;
      case '--download-images':
        options.downloadImages = true;
        break;
      case '--skip-existing':
        options.skipExisting = true;
        break;
      case '--run-lifecyclemodels':
        options.runLifecyclemodels = true;
        break;
      case '--json':
        options.json = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  options.maxResults = Number.parseInt(options.maxResults, 10);
  options.delay = Number.parseInt(options.delay, 10);
  if (!Number.isInteger(options.maxResults) || options.maxResults < 1) {
    throw new Error('--max-results must be a positive integer');
  }
  if (!Number.isInteger(options.delay) || options.delay < 0) {
    throw new Error('--delay must be a non-negative integer');
  }
  if (!['auto', 'direct', 'jina'].includes(options.fetcher)) {
    throw new Error('--fetcher must be auto, direct, or jina');
  }
  if (!['flow', 'all'].includes(options.imageMode)) {
    throw new Error('--image-mode must be flow or all');
  }
  return options;
}

function runNode(label, args, { jsonMode = false } = {}) {
  if (!jsonMode) console.error(`[${label}]`);
  const result = spawnSync(process.execPath, args, {
    cwd: repoRoot,
    stdio: jsonMode ? 'pipe' : 'inherit',
  });
  if (result.status !== 0) {
    if (jsonMode && result.stdout) process.stderr.write(result.stdout.toString());
    if (jsonMode && result.stderr) process.stderr.write(result.stderr.toString());
    throw new Error(`${label} failed`);
  }
  return result;
}

function writePatentHandoffFiles(manifest, outDir) {
  for (const patent of manifest.patents) {
    const patentDir = path.join(outDir, 'patents', patent.publication_number);
    fs.mkdirSync(patentDir, { recursive: true });
    fs.mkdirSync(path.join(patentDir, 'lifecyclemodel'), { recursive: true });
    fs.writeFileSync(
      path.join(patentDir, 'source-metadata.json'),
      `${JSON.stringify(patent, null, 2)}\n`,
    );
    fs.writeFileSync(
      path.join(patentDir, 'plan-source.json'),
      `${JSON.stringify({
        schema_version: 1,
        source: patent.source,
        product: manifest.product,
        downloaded_text: patent.download.text_file || '',
        figure_image_files: patent.download.figure_image_files || [],
        lifecyclemodel_plan_file: patent.lifecyclemodel.plan_file,
        instruction:
          'Author lifecyclemodel/plan.json from the downloaded patent text, preserving this source object under plan.source.',
      }, null, 2)}\n`,
    );
  }
}

export function runAuthoredLifecyclemodelPlans(
  manifest,
  outDir,
  { jsonMode = false, runner = runNode } = {},
) {
  for (const patent of manifest.patents) {
    const planPath = path.join(outDir, patent.lifecyclemodel.plan_file);
    const baseDir = path.join(outDir, patent.lifecyclemodel.base_dir);
    if (!fs.existsSync(planPath)) continue;
    runner(`patent-to-lifecyclemodel:${patent.publication_number}`, [
      path.join('patent-to-lifecyclemodel', 'scripts', 'run-patent-to-lifecyclemodel.mjs'),
      '--plan', planPath,
      '--base', baseDir,
      '--all',
      '--json',
    ], { jsonMode });
    patent.lifecyclemodel.status = 'completed';
  }
}

async function run(options) {
  const outDir = path.resolve(
    repoRoot,
    options.outDir || path.join('output', 'product-to-patent-lifecyclemodel', slugify(options.query)),
  );
  const productName = options.productName || options.query;
  const metadataDir = path.join(outDir, 'metadata');
  const rawDir = path.join(outDir, 'raw');
  const metadataFile = path.join(metadataDir, 'google-patents-metadata.json');
  const downloadSummaryFile = path.join(rawDir, 'download-summary.json');

  fs.mkdirSync(outDir, { recursive: true });

  runNode('google-patents metadata', [
    path.join('product-to-patent', 'scripts', 'google-patents-metadata.mjs'),
    '--query', options.query,
    '--max-results', String(options.maxResults),
    '--fetcher', options.fetcher,
    '--out-dir', metadataDir,
  ], { jsonMode: options.json });

  runNode('google-patents fulltext', [
    path.join('product-to-patent', 'scripts', 'google-patents-download-fulltext.mjs'),
    '--metadata-file', metadataFile,
    '--out-dir', rawDir,
    '--delay', String(options.delay),
    ...(options.downloadImages ? ['--download-images', '--image-mode', options.imageMode] : []),
    ...(options.skipExisting ? ['--skip-existing'] : []),
  ], { jsonMode: options.json });

  const metadata = JSON.parse(fs.readFileSync(metadataFile, 'utf8'));
  const downloadSummary = fs.existsSync(downloadSummaryFile)
    ? JSON.parse(fs.readFileSync(downloadSummaryFile, 'utf8'))
    : {};
  const manifest = buildLifecyclemodelWorkflowManifest({
    productName,
    query: options.query,
    metadata,
    downloadSummary,
    outDir,
    metadataFile,
    rawDir,
  });

  writePatentHandoffFiles(manifest, outDir);
  if (options.runLifecyclemodels) {
    runAuthoredLifecyclemodelPlans(manifest, outDir, { jsonMode: options.json });
  }

  const manifestPath = path.join(outDir, 'workflow-manifest.json');
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

  if (options.json) {
    console.log(JSON.stringify(manifest, null, 2));
  } else {
    console.log(`Wrote product patent lifecyclemodel workflow to ${manifestPath}`);
  }
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
  if (!options.query) {
    fail('pass --query');
  }

  run(options).catch((error) => {
    fail(error instanceof Error ? error.message : String(error));
  });
}
