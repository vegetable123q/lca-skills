import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  buildLifecyclemodelWorkflowManifest,
  runAuthoredLifecyclemodelPlans,
} from '../product-to-patent/scripts/product-patent-lifecyclemodel-workflow.mjs';
import {
  buildPatentSourceFromGooglePatentsResult,
} from '../patent-to-lifecyclemodel/scripts/patent-metadata.mjs';

test('buildPatentSourceFromGooglePatentsResult preserves company, dates, links, and family metadata', () => {
  const source = buildPatentSourceFromGooglePatentsResult(
    {
      rank: 3,
      publication_number: 'CN113264560A',
      title: 'Double-coated NCM811 cathode material and preparation method thereof',
      assignee: 'Example Battery Materials Co., Ltd.',
      inventor: 'Wang Jia Tai',
      priority_date: '2021-05-17',
      filing_date: '2021-05-17',
      publication_date: '2021-08-17',
      grant_date: '',
      link: 'https://patents.google.com/patent/CN113264560A/en',
      pdf_link: 'https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf',
      detail: {
        family_members: ['CN113264560A', 'CN113264560B'],
        cited_patents: ['US20150104708A1'],
      },
    },
    { query: '"NCM811" cathode "preparation method"', productName: 'NCM811 cathode active material' },
  );

  assert.deepEqual(source, {
    type: 'patent',
    id: 'CN113264560A',
    title: 'Double-coated NCM811 cathode material and preparation method thereof',
    assignee: 'Example Battery Materials Co., Ltd.',
    inventor: 'Wang Jia Tai',
    priority_date: '2021-05-17',
    filing_date: '2021-05-17',
    publication_date: '2021-08-17',
    year: '2021',
    url: 'https://patents.google.com/patent/CN113264560A/en',
    pdf_url: 'https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf',
    family_members: ['CN113264560A', 'CN113264560B'],
    cited_patents: ['US20150104708A1'],
    google_patents_rank: 3,
    source_query: '"NCM811" cathode "preparation method"',
    product_name: 'NCM811 cathode active material',
  });
});

test('runAuthoredLifecyclemodelPlans suppresses child JSON when parent is in json mode', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'product-patent-workflow-'));
  const planFile = path.join(tempDir, 'patents', 'CN113264560A', 'lifecyclemodel', 'plan.json');
  fs.mkdirSync(path.dirname(planFile), { recursive: true });
  fs.writeFileSync(planFile, '{"source":{"id":"CN113264560A"}}\n');

  const calls = [];
  const manifest = {
    patents: [
      {
        publication_number: 'CN113264560A',
        lifecyclemodel: {
          status: 'needs_plan',
          base_dir: 'patents/CN113264560A/lifecyclemodel',
          plan_file: 'patents/CN113264560A/lifecyclemodel/plan.json',
        },
      },
    ],
  };

  runAuthoredLifecyclemodelPlans(manifest, tempDir, {
    jsonMode: true,
    runner: (label, args, options) => {
      calls.push({ label, args, options });
    },
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.jsonMode, true);
  assert.equal(manifest.patents[0].lifecyclemodel.status, 'completed');
});

test('buildLifecyclemodelWorkflowManifest organizes metadata, downloads, and conversion queue', () => {
  const metadata = {
    schema_version: 1,
    query: '"NCM811" cathode "preparation method"',
    public_url: 'https://patents.google.com/?q=NCM811',
    results: [
      {
        rank: 0,
        publication_number: 'CN113264560A',
        title: 'Double-coated NCM811 cathode material',
        assignee: 'Example Battery Materials Co., Ltd.',
        priority_date: '2021-05-17',
        publication_date: '2021-08-17',
        link: 'https://patents.google.com/patent/CN113264560A/en',
        pdf_link: 'https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf',
        detail: { family_members: ['CN113264560A'] },
      },
      {
        rank: 1,
        publication_number: 'CN113224310A',
        title: 'Coating NCM811 cathode material',
        assignee: 'Example Institute',
        publication_date: '2021-08-06',
        link: 'https://patents.google.com/patent/CN113224310A/en',
      },
    ],
  };
  const downloadSummary = {
    schema_version: 1,
    results: [
      {
        publication_number: 'CN113264560A',
        status: 'ok',
        source: 'jina-en',
        format: 'md',
        figure_images: [
          { status: 'ok', file: 'CN113264560A-images/01-flow.png' },
        ],
      },
    ],
  };

  const manifest = buildLifecyclemodelWorkflowManifest({
    productName: 'NCM811 cathode active material',
    query: metadata.query,
    metadata,
    downloadSummary,
    outDir: '/workspace/output/product-to-patent-lifecyclemodel/ncm811',
    metadataFile: '/workspace/output/product-to-patent-lifecyclemodel/ncm811/metadata/google-patents-metadata.json',
    rawDir: '/workspace/output/product-to-patent-lifecyclemodel/ncm811/raw',
  });

  assert.equal(manifest.schema_version, 1);
  assert.equal(manifest.product.name, 'NCM811 cathode active material');
  assert.equal(manifest.patents.length, 2);
  assert.deepEqual(manifest.patents[0].source.assignee, 'Example Battery Materials Co., Ltd.');
  assert.equal(manifest.patents[0].download.text_file, 'raw/CN113264560A.md');
  assert.deepEqual(manifest.patents[0].download.figure_image_files, [
    'raw/CN113264560A-images/01-flow.png',
  ]);
  assert.equal(manifest.patents[0].lifecyclemodel.status, 'needs_plan');
  assert.equal(manifest.patents[0].lifecyclemodel.plan_file, 'patents/CN113264560A/lifecyclemodel/plan.json');
  assert.equal(manifest.patents[1].download.status, 'not_downloaded');
  assert.equal(manifest.patents[1].source.year, '2021');
});

test('buildLifecyclemodelWorkflowManifest keeps existing text and pdf paths for skipped downloads', () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'product-patent-workflow-'));
  const rawDir = path.join(tempDir, 'raw');
  fs.mkdirSync(rawDir, { recursive: true });
  fs.writeFileSync(path.join(rawDir, 'CN113264560A.md'), '# patent text\n');
  fs.writeFileSync(path.join(rawDir, 'CN113264560A.pdf'), 'pdf bytes');

  const manifest = buildLifecyclemodelWorkflowManifest({
    productName: 'NCM811 cathode active material',
    query: '"NCM811" cathode',
    metadata: {
      query: '"NCM811" cathode',
      results: [
        {
          publication_number: 'CN113264560A',
          title: 'Double-coated NCM811 cathode material',
          publication_date: '2021-08-17',
          link: 'https://patents.google.com/patent/CN113264560A/en',
        },
      ],
    },
    downloadSummary: {
      results: [
        { publication_number: 'CN113264560A', status: 'skipped', source: 'existing' },
      ],
    },
    outDir: tempDir,
    metadataFile: path.join(tempDir, 'metadata', 'google-patents-metadata.json'),
    rawDir,
  });

  assert.equal(manifest.patents[0].download.text_file, 'raw/CN113264560A.md');
  assert.equal(manifest.patents[0].download.pdf_file, 'raw/CN113264560A.pdf');
});
