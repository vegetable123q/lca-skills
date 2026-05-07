#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const googlePatentsOrigin = 'https://patents.google.com';
const patentImagesOrigin = 'https://patentimages.storage.googleapis.com';
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const skillDir = path.resolve(scriptDir, '..');

function fail(message) {
  console.error(`Error: ${message}`);
  process.exit(2);
}

function printHelp() {
  console.log(
    `
Usage:
  node product-to-patent/scripts/google-patents-metadata.mjs --query <query> [options]
  node product-to-patent/scripts/google-patents-metadata.mjs --url <google-patents-url> [options]

Options:
  --query <text>        Google Patents search query, for example '"NCM811" cathode'
  --url <url>           Existing https://patents.google.com search URL
  --out-dir <dir>       Output directory (default: output/product-to-patent/<query-slug>)
  --max-results <n>     Number of search results to enrich (default: 20)
  --sort <relevance|new|old>
  --page <n>            Search result page number, zero-based in the xhr endpoint
  --fetcher <mode>      auto, direct, or jina (default: auto)
  --no-detail           Skip per-patent xhr/result detail enrichment
  --json                Print the final metadata JSON to stdout
  -h, --help            Show this help

Outputs:
  google-patents-query.json
  google-patents-metadata.json
  google-patents-candidates.jsonl
`.trim(),
  );
}

function stripTags(text = '') {
  return decodeHtml(String(text).replace(/<[^>]*>/gu, ' ').replace(/\s+/gu, ' ').trim());
}

function decodeHtml(text = '') {
  return String(text)
    .replace(/&quot;/gu, '"')
    .replace(/&#39;|&apos;/gu, "'")
    .replace(/&amp;/gu, '&')
    .replace(/&lt;/gu, '<')
    .replace(/&gt;/gu, '>')
    .replace(/&hellip;/gu, '...')
    .replace(/&#x([0-9a-f]+);/giu, (_match, hex) => String.fromCodePoint(Number.parseInt(hex, 16)))
    .replace(/&#([0-9]+);/gu, (_match, value) => String.fromCodePoint(Number.parseInt(value, 10)));
}

function normalizePatentId(value = '') {
  return value.replace(/^patent\//u, '').replace(/\/[a-z]{2}$/u, '');
}

function patentLinkFromId(id) {
  return id.startsWith('http') ? id : `${googlePatentsOrigin}/${id.replace(/^\/+/u, '')}`;
}

function pdfLinkFromPath(pdf) {
  if (!pdf) {
    return '';
  }
  return pdf.startsWith('http') ? pdf : `${patentImagesOrigin}/${pdf.replace(/^\/+/u, '')}`;
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => {
      if (Array.isArray(entry)) {
        return entry.length > 0;
      }
      return entry !== undefined && entry !== null && entry !== '';
    }),
  );
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function firstMatch(text, patterns) {
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) {
      return stripTags(match[1]);
    }
  }
  return '';
}

function extractItemprop(html, prop) {
  const patterns = [
    new RegExp(`<meta\\s+[^>]*itemprop=["']${prop}["'][^>]*content=["']([^"']+)["'][^>]*>`, 'iu'),
    new RegExp(`<a\\s+[^>]*itemprop=["']${prop}["'][^>]*href=["']([^"']+)["'][^>]*>`, 'iu'),
    new RegExp(`<a\\s+[^>]*href=["']([^"']+)["'][^>]*itemprop=["']${prop}["'][^>]*>`, 'iu'),
    new RegExp(`<[^>]+itemprop=["']${prop}["'][^>]*>([\\s\\S]*?)<\\/[^>]+>`, 'iu'),
  ];
  for (const pattern of patterns) {
    const match = html.match(pattern);
    if (match?.[1]) {
      return stripTags(match[1]);
    }
  }
  return '';
}

function sectionAfterHeading(html, heading) {
  const headingPattern = new RegExp(`<h2[^>]*>\\s*${heading.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}[^<]*<\\/h2>`, 'iu');
  const startMatch = headingPattern.exec(html);
  if (!startMatch) {
    return '';
  }
  const start = startMatch.index + startMatch[0].length;
  const nextHeading = html.slice(start).search(/<h2\b/iu);
  return nextHeading === -1 ? html.slice(start) : html.slice(start, start + nextHeading);
}

function extractPatentNumbersFromHtml(html) {
  const numbers = [];
  const hrefPattern = /href=["']\/?patent\/([A-Z]{2}[A-Z0-9]+)\/[a-z]{2}["']/giu;
  for (const match of html.matchAll(hrefPattern)) {
    numbers.push(match[1]);
  }
  const markdownLinkPattern = /\]\((?:https?:\/\/)?patents\.google\.com\/patent\/([A-Z]{2}[A-Z0-9]+)\/[a-z]{2}\)/giu;
  for (const match of html.matchAll(markdownLinkPattern)) {
    numbers.push(match[1]);
  }
  return unique(numbers);
}

function extractApplications(html) {
  const applicationsSection = html.match(/<section[^>]+itemprop=["']applications["'][^>]*>([\s\S]*?)<\/section>/iu);
  if (!applicationsSection) {
    return [];
  }
  return extractPatentNumbersFromHtml(applicationsSection[1]);
}

function extractJinaSection(text, heading) {
  const headingPattern = new RegExp(`^##\\s+${heading.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&')}[^\\n]*$`, 'imu');
  const startMatch = headingPattern.exec(text);
  if (!startMatch) {
    return '';
  }
  const start = startMatch.index + startMatch[0].length;
  const nextHeading = text.slice(start).search(/^##\s+/imu);
  return nextHeading === -1 ? text.slice(start) : text.slice(start, start + nextHeading);
}

function extractJinaFamilyMembers(text) {
  return unique(
    [
      ...extractPatentNumbersFromHtml(extractJinaSection(text, 'Applications Claiming Priority')),
      ...extractPatentNumbersFromHtml(extractJinaSection(text, 'Applications')),
      ...extractPatentNumbersFromHtml(extractJinaSection(text, 'Worldwide Applications')),
    ],
  );
}

export function extractJinaReaderContent(text) {
  const marker = 'Markdown Content:';
  const index = text.indexOf(marker);
  if (index === -1) {
    return text;
  }
  return text.slice(index + marker.length).trim();
}

export function buildGooglePatentsSearchUrl({ query, sort = '', page = '' }) {
  if (!query?.trim()) {
    throw new Error('query is required');
  }

  const params = new URLSearchParams();
  params.set('q', query.trim());
  params.set('dups', 'language');
  if (sort) {
    params.set('sort', sort);
  }
  if (page !== '' && page !== null && page !== undefined) {
    params.set('page', String(page));
  }

  const queryString = params.toString();
  return {
    publicUrl: `${googlePatentsOrigin}/?${queryString}`,
    xhrUrl: `${googlePatentsOrigin}/xhr/query?url=${encodeURIComponent(queryString)}&exp=`,
  };
}

function buildSearchUrlsFromPublicUrl(publicUrl) {
  const url = new URL(publicUrl);
  if (url.hostname !== 'patents.google.com') {
    throw new Error('--url must point to patents.google.com');
  }
  return {
    publicUrl: url.toString(),
    xhrUrl: `${googlePatentsOrigin}/xhr/query?url=${encodeURIComponent(url.searchParams.toString())}&exp=`,
  };
}

export function flattenGooglePatentsResults(payload) {
  const clusters = payload?.results?.cluster ?? [];
  const rows = [];

  for (const cluster of clusters) {
    for (const result of cluster.result ?? []) {
      const patent = result.patent ?? {};
      const publicationNumber = patent.publication_number || normalizePatentId(result.id || '');
      rows.push({
        id: result.id ?? '',
        rank: Number.isFinite(result.rank) ? result.rank : rows.length,
        publication_number: publicationNumber,
        title: stripTags(patent.title),
        assignee: stripTags(patent.assignee),
        inventor: stripTags(patent.inventor),
        priority_date: patent.priority_date ?? '',
        filing_date: patent.filing_date ?? '',
        publication_date: patent.publication_date ?? '',
        grant_date: patent.grant_date ?? '',
        language: patent.language ?? '',
        link: patentLinkFromId(result.id ?? `patent/${publicationNumber}/en`),
        pdf_link: pdfLinkFromPath(patent.pdf ?? ''),
        snippet: stripTags(patent.snippet),
      });
    }
  }

  return rows;
}

export function parsePatentDetailHtml(html) {
  const content = extractJinaReaderContent(html);
  const patentCitations = sectionAfterHeading(html, 'Patent Citations');
  const citedBy = sectionAfterHeading(html, 'Cited By');
  const similar = sectionAfterHeading(html, 'Similar Documents');
  const markdownTitle = firstMatch(content, [
    /^#\s+[A-Z]{2}[A-Z0-9]+\s+-\s+(.+?)\s+-\s+Google Patents\s*$/imu,
    /^(.+?)\s+\[Download PDF\]/imu,
  ]);
  const markdownPdf = firstMatch(content, [/\[Download PDF\]\((https?:\/\/[^)]+)\)/iu]);
  const markdownPublication = firstMatch(content, [
    /Publication number\s+([A-Z]{2}[A-Z0-9]+)/iu,
    /^#\s+([A-Z]{2}[A-Z0-9]+)\s+-/imu,
  ]);

  return {
    publication_number: extractItemprop(html, 'publicationNumber') || markdownPublication,
    title: extractItemprop(html, 'title') || markdownTitle,
    pdf_link: extractItemprop(html, 'pdfLink') || markdownPdf,
    family_members: extractApplications(html).length
      ? extractApplications(html)
      : extractJinaFamilyMembers(content),
    cited_patents: extractPatentNumbersFromHtml(
      patentCitations || extractJinaSection(content, 'Patent Citations'),
    ),
    cited_by_patents: extractPatentNumbersFromHtml(citedBy || extractJinaSection(content, 'Cited By')),
    similar_documents: extractPatentNumbersFromHtml(
      similar || extractJinaSection(content, 'Similar Documents'),
    ),
  };
}

function buildJinaReaderUrl(url) {
  const parsed = new URL(url);
  parsed.protocol = 'http:';
  return `https://r.jina.ai/http://${parsed.host}${parsed.pathname}${parsed.search}`;
}

function buildJinaReaderUrls(url) {
  return [
    `https://r.jina.ai/http://r.jina.ai/http://${url}`,
    buildJinaReaderUrl(url),
  ];
}

function isAutomatedQueryBlock(text) {
  return /automated queries|We're sorry/iu.test(text);
}

export function relaxGooglePatentsQuery(query) {
  const trimmed = query.trim();
  const withoutProcessPhrase = trimmed
    .replace(/\s+"preparation method"/iu, '')
    .replace(/\s+"preparing method"/iu, '')
    .replace(/\s+"method of preparation"/iu, '')
    .replace(/\s+preparation method/iu, '')
    .trim();
  return withoutProcessPhrase !== trimmed ? withoutProcessPhrase : '';
}

async function fetchWithTimeout(url, options = {}) {
  return fetch(url, {
    signal: AbortSignal.timeout(options.timeoutMs ?? 45000),
    ...options,
  });
}

function isGoogleSorryFailure(error) {
  return /Google Patents returned an HTML Sorry page/u.test(error instanceof Error ? error.message : String(error));
}

async function fetchJsonDirect(url) {
  const response = await fetch(url, {
    headers: {
      accept: 'application/json,text/html;q=0.9,*/*;q=0.8',
      'accept-language': 'en-US,en;q=0.9',
      referer: 'https://patents.google.com/',
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    },
  });
  if (!response.ok) {
    throw new Error(
      formatFetchFailure({
        url,
        status: response.status,
        contentType: response.headers.get('content-type') ?? '',
        body: await response.text(),
      }),
    );
  }
  return response.json();
}

async function fetchTextDirect(url) {
  const response = await fetchWithTimeout(url, {
    headers: {
      accept: 'text/html,*/*;q=0.8',
      'accept-language': 'en-US,en;q=0.9',
      referer: 'https://patents.google.com/',
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    },
  });
  if (!response.ok) {
    throw new Error(
      formatFetchFailure({
        url,
        status: response.status,
        contentType: response.headers.get('content-type') ?? '',
        body: await response.text(),
      }),
    );
  }
  return response.text();
}

async function fetchTextJina(url) {
  let lastError = null;
  for (const jinaUrl of buildJinaReaderUrls(url)) {
    const response = await fetchWithTimeout(jinaUrl, {
      headers: {
        accept: 'text/plain,*/*;q=0.8',
        'user-agent': 'tiangong-lca-skills/product-to-patent metadata helper',
      },
      timeoutMs: 90000,
    });
    const text = await response.text();
    if (!response.ok) {
      lastError = new Error(
        formatFetchFailure({
          url: jinaUrl,
          status: response.status,
          contentType: response.headers.get('content-type') ?? '',
          body: text,
        }),
      );
      continue;
    }
    if (isAutomatedQueryBlock(text)) {
      lastError = new Error(
        `Jina Reader got a Google automated-query block for ${url}: ${extractJinaReaderContent(text)
          .replace(/\s+/gu, ' ')
          .slice(0, 180)}`,
      );
      continue;
    }
    return text;
  }
  throw lastError ?? new Error(`Jina Reader could not fetch ${url}`);
}

async function fetchJsonJina(url) {
  const text = await fetchTextJina(url);
  const content = extractJinaReaderContent(text);
  try {
    return JSON.parse(content);
  } catch (error) {
    const snippet = content.replace(/\s+/gu, ' ').slice(0, 180);
    throw new Error(`Jina Reader did not return Google Patents JSON for ${url}: ${snippet}`);
  }
}

async function fetchJsonForMode(url, fetcher) {
  if (fetcher === 'direct') {
    return fetchJsonDirect(url);
  }
  if (fetcher === 'jina') {
    return fetchJsonJina(url);
  }
  try {
    return await fetchJsonJina(url);
  } catch (error) {
    if (isGoogleSorryFailure(error)) {
      return fetchJsonDirect(url);
    }
    throw error;
  }
}

async function fetchTextForMode(url, fetcher) {
  if (fetcher === 'direct') {
    return fetchTextDirect(url);
  }
  if (fetcher === 'jina') {
    return fetchTextJina(url);
  }
  try {
    return await fetchTextJina(url);
  } catch (error) {
    if (isGoogleSorryFailure(error)) {
      return fetchTextDirect(url);
    }
    throw error;
  }
}

export function formatFetchFailure({ url, status, contentType = '', body = '' }) {
  const prefix = `GET ${url} failed with ${status}${contentType ? ` (${contentType})` : ''}.`;
  if (status === 503 && /<title>\s*Sorry\.\.\.\s*<\/title>/iu.test(body)) {
    return `${prefix} Google Patents returned an HTML Sorry page; retry later, reduce request volume, or open the public search URL for manual CSV download.`;
  }
  return prefix;
}

function parseArgs(rawArgs) {
  const options = {
    maxResults: 20,
    sort: '',
    page: '',
    detail: true,
    fetcher: 'auto',
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
      case '--url':
      case '--out-dir':
      case '--max-results':
      case '--sort':
      case '--page':
      case '--fetcher':
        if (index + 1 >= rawArgs.length) {
          throw new Error(`${arg} requires a value`);
        }
        options[arg.slice(2).replace(/-([a-z])/gu, (_match, char) => char.toUpperCase())] =
          rawArgs[index + 1];
        index += 1;
        break;
      case '--no-detail':
        options.detail = false;
        break;
      case '--json':
        options.json = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  options.maxResults = Number.parseInt(options.maxResults, 10);
  if (!Number.isFinite(options.maxResults) || options.maxResults < 1) {
    throw new Error('--max-results must be a positive integer');
  }
  if (!['auto', 'direct', 'jina'].includes(options.fetcher)) {
    throw new Error('--fetcher must be auto, direct, or jina');
  }

  return options;
}

function slugify(value) {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, '-')
    .replace(/^-|-$/gu, '')
    .slice(0, 80) || 'google-patents-query';
}

export function sanitizeDetailTextFilename(value) {
  const basename = String(value)
    .trim()
    .replace(/[^A-Za-z0-9]+/gu, '-')
    .replace(/^-|-$/gu, '')
    .slice(0, 120);
  return `${basename || 'patent-detail'}.md`;
}

function seedRowsForQuery(query) {
  if (!/\bNCM\s*811\b|\bNMC\s*811\b|LiNi0\.?8Co0\.?1Mn0\.?1O2|nickel-rich.+\b811\b|\b811\b.+cathode/iu.test(query)) {
    return [];
  }
  const seedFile = path.join(skillDir, 'assets', 'ncm811-query-plan.json');
  const seedPlan = JSON.parse(fs.readFileSync(seedFile, 'utf8'));
  return (seedPlan.seed_publications ?? []).map((seed, index) => ({
    id: `patent/${seed.publication_number}/en`,
    rank: index,
    publication_number: seed.publication_number,
    title: seed.title_hint ?? '',
    assignee: '',
    inventor: '',
    priority_date: '',
    filing_date: '',
    publication_date: '',
    grant_date: '',
    language: 'en',
    link: `${googlePatentsOrigin}/patent/${seed.publication_number}/en`,
    pdf_link: '',
    snippet: 'Seeded fallback candidate from product-to-patent NCM811 query plan.',
    seed_source: 'assets/ncm811-query-plan.json',
  }));
}

async function run(options) {
  let urls = options.url
    ? buildSearchUrlsFromPublicUrl(options.url)
    : buildGooglePatentsSearchUrl({
        query: options.query,
        sort: options.sort,
        page: options.page,
      });
  const requestedQuery = options.query ?? new URL(urls.publicUrl).searchParams.get('q') ?? 'google-patents';
  let queryLabel = requestedQuery;
  const outDir =
    options.outDir ?? path.join('output', 'product-to-patent', slugify(queryLabel));

  let relaxedFrom = '';
  let searchPayload;
  let seededFallback = false;
  let seedRows = [];
  try {
    searchPayload = await fetchJsonForMode(urls.xhrUrl, options.fetcher);
  } catch (error) {
    const relaxedQuery = options.query ? relaxGooglePatentsQuery(options.query) : '';
    if (!relaxedQuery || !/automated-query block|automated queries|We're sorry/iu.test(String(error))) {
      seedRows = options.query ? seedRowsForQuery(options.query) : [];
      if (!seedRows.length) {
        throw error;
      }
      seededFallback = true;
      searchPayload = { results: { total_num_results: null, cluster: [] } };
    } else {
      relaxedFrom = options.query;
      queryLabel = relaxedQuery;
      urls = buildGooglePatentsSearchUrl({
        query: relaxedQuery,
        sort: options.sort,
        page: options.page,
      });
      try {
        searchPayload = await fetchJsonForMode(urls.xhrUrl, options.fetcher);
      } catch (relaxedError) {
        seedRows = seedRowsForQuery(options.query);
        if (!seedRows.length) {
          throw relaxedError;
        }
        seededFallback = true;
        searchPayload = { results: { total_num_results: null, cluster: [] } };
      }
    }
  }
  const flattened = (seededFallback ? seedRows : flattenGooglePatentsResults(searchPayload)).slice(
    0,
    options.maxResults,
  );
  const enriched = [];
  const detailsDir = path.join(outDir, 'details');
  fs.mkdirSync(detailsDir, { recursive: true });

  for (const row of flattened) {
    if (!options.detail) {
      enriched.push(row);
      continue;
    }
    const detailUrl = `${googlePatentsOrigin}/xhr/result?id=${encodeURIComponent(row.id)}`;
    let detailHtml = '';
    let detailFetchUrl = detailUrl;
    let detailError = '';
    try {
      detailHtml = await fetchTextForMode(detailUrl, options.fetcher);
    } catch (error) {
      try {
        detailFetchUrl = row.link;
        detailHtml = await fetchTextForMode(row.link, options.fetcher);
      } catch (fallbackError) {
        detailError = fallbackError instanceof Error ? fallbackError.message : String(fallbackError);
      }
    }
    const detail = detailHtml ? parsePatentDetailHtml(detailHtml) : {};
    const publicationNumber = detail.publication_number || row.publication_number;
    const detailTextFile = detailHtml
      ? path.join('details', sanitizeDetailTextFilename(publicationNumber))
      : '';
    if (detailHtml) {
      fs.writeFileSync(path.join(outDir, detailTextFile), `${detailHtml.trim()}\n`);
    }
    enriched.push(
      compactObject({
        ...row,
        publication_number: publicationNumber,
        title: detail.title || row.title,
        pdf_link: detail.pdf_link || row.pdf_link,
        detail_text_file: detailTextFile,
        detail_xhr_url: detailUrl,
        detail_fetch_url: detailFetchUrl,
        detail_error: detailError,
        detail,
      }),
    );
  }

  const metadata = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    source: 'Google Patents',
    query: queryLabel,
    requested_query: requestedQuery,
    query_relaxed_from: relaxedFrom,
    seeded_fallback: seededFallback,
    public_url: urls.publicUrl,
    xhr_query_url: urls.xhrUrl,
    total_num_results: searchPayload?.results?.total_num_results ?? null,
    exported_result_count: enriched.length,
    notes: [
      'Google Patents search results are family-collapsed by default; inspect detail.family_members and citation links before selecting one representative patent.',
      'Use this metadata as a triage aid, not as legal-status authority.',
    ],
    results: enriched,
  };

  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(path.join(outDir, 'google-patents-query.json'), `${JSON.stringify(urls, null, 2)}\n`);
  fs.writeFileSync(
    path.join(outDir, 'google-patents-metadata.json'),
    `${JSON.stringify(metadata, null, 2)}\n`,
  );
  fs.writeFileSync(
    path.join(outDir, 'google-patents-candidates.jsonl'),
    `${enriched.map((row) => JSON.stringify(row)).join('\n')}\n`,
  );

  if (options.json) {
    console.log(JSON.stringify(metadata, null, 2));
  } else {
    console.log(`Wrote ${enriched.length} Google Patents candidates to ${outDir}`);
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
  if (!options.query && !options.url) {
    fail('pass --query or --url');
  }

  run(options).catch((error) => {
    fail(error instanceof Error ? error.message : String(error));
  });
}
