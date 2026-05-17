import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildPatentLifecyclemodelManifest,
  buildPatentSourceMetadata,
} from '../patent-to-lifecyclemodel/scripts/patent-metadata.mjs';

test('buildPatentSourceMetadata carries patent company and year fields', () => {
  const plan = {
    source: {
      id: 'CN111725499B',
      title: 'Lithium ion battery cathode preparation method',
      assignee: 'Contemporary Amperex Technology Co., Limited',
      priority_date: '2019-12-31',
      filing_date: '2019-12-31',
      publication_date: '2020-06-26',
      grant_date: '2022-03-04',
      url: 'https://patents.google.com/patent/CN111725499B/en',
      family_members: ['CN111725499A', 'CN111725499B'],
    },
    reference_year: '2019',
  };

  assert.deepEqual(buildPatentSourceMetadata(plan), {
    source_type: 'patent',
    source_id: 'CN111725499B',
    title: 'Lithium ion battery cathode preparation method',
    assignee: 'Contemporary Amperex Technology Co., Limited',
    priority_date: '2019-12-31',
    filing_date: '2019-12-31',
    publication_date: '2020-06-26',
    grant_date: '2022-03-04',
    year: '2020',
    reference_year: '2019',
    extra_metadata: {
      url: 'https://patents.google.com/patent/CN111725499B/en',
      family_members: ['CN111725499A', 'CN111725499B'],
    },
  });
});

test('buildPatentSourceMetadata flattens source.extra_metadata into lifecyclemodel source metadata', () => {
  const plan = {
    source: {
      id: 'CN112209449A',
      title: 'NCM811 preparation method',
      publication_date: '2021-01-12',
      extra_metadata: {
        jurisdiction: 'CN',
        parameter_profile_file: 'output/CN112209449A/patent-parameters.json',
        patent_parameter_summary: 'claim ranges and embodiment performance',
      },
    },
    reference_year: '2020',
  };

  assert.deepEqual(buildPatentSourceMetadata(plan).extra_metadata, {
    jurisdiction: 'CN',
    parameter_profile_file: 'output/CN112209449A/patent-parameters.json',
    patent_parameter_summary: 'claim ranges and embodiment performance',
  });
});

test('buildPatentLifecyclemodelManifest pre-fills basic patent info for CLI intake', () => {
  const combinedDir = '/workspace/output/CN111725499B/runs/CN111725499B-combined';
  const plan = {
    source: {
      id: 'CN111725499B',
      title: 'Lithium ion battery cathode preparation method',
      assignee: 'Contemporary Amperex Technology Co., Limited',
      publication_date: '2020-06-26',
    },
    goal: {
      name: 'NCM cathode material',
      functional_unit: { amount: 1, unit: 'kg' },
      boundary: 'cradle-to-gate',
    },
    geography: 'CN',
    reference_year: '2019',
  };

  const manifest = buildPatentLifecyclemodelManifest(plan, combinedDir);

  assert.equal(manifest.basic_info.name, 'NCM cathode material');
  assert.deepEqual(manifest.basic_info.functional_unit, { amount: 1, unit: 'kg' });
  assert.deepEqual(manifest.basic_info.source, {
    source_type: 'patent',
    source_id: 'CN111725499B',
    title: 'Lithium ion battery cathode preparation method',
    assignee: 'Contemporary Amperex Technology Co., Limited',
    publication_date: '2020-06-26',
    year: '2020',
    reference_year: '2019',
  });
  assert.equal(manifest.local_runs[0], combinedDir);
  assert.match(manifest.selection.decision_factors[0], /CN111725499B/u);
  assert.match(manifest.selection.decision_factors[0], /Contemporary Amperex/u);
  assert.match(manifest.selection.decision_factors[0], /2020/u);
});
