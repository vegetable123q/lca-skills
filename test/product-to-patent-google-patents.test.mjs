import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildGooglePatentsSearchUrl,
  extractJinaReaderContent,
  flattenGooglePatentsResults,
  formatFetchFailure,
  parsePatentDetailHtml,
  relaxGooglePatentsQuery,
  sanitizeDetailTextFilename,
} from '../product-to-patent/scripts/google-patents-metadata.mjs';

test('buildGooglePatentsSearchUrl creates stable public and xhr URLs', () => {
  const urls = buildGooglePatentsSearchUrl({
    query: '"NCM811" cathode "preparation method"',
    sort: 'new',
    page: 2,
  });

  assert.equal(
    urls.publicUrl,
    'https://patents.google.com/?q=%22NCM811%22+cathode+%22preparation+method%22&dups=language&sort=new&page=2',
  );
  assert.equal(
    urls.xhrUrl,
    'https://patents.google.com/xhr/query?url=q%3D%2522NCM811%2522%2Bcathode%2B%2522preparation%2Bmethod%2522%26dups%3Dlanguage%26sort%3Dnew%26page%3D2&exp=',
  );
});

test('flattenGooglePatentsResults keeps links, ranks, and bibliographic metadata', () => {
  const payload = {
    results: {
      total_num_results: 2,
      cluster: [
        {
          result: [
            {
              id: 'patent/CN113264560A/en',
              rank: 0,
              patent: {
                title: 'Double-coated NCM811 cathode material and preparation method thereof',
                assignee: 'Example Institute',
                inventor: 'Wang Jia Tai',
                publication_number: 'CN113264560A',
                priority_date: '2021-05-17',
                filing_date: '2021-05-17',
                publication_date: '2021-08-17',
                language: 'en',
                pdf: 'cb/54/04/CN113264560A.pdf',
                snippet: 'A double-coated <b>NCM811</b> cathode material.',
              },
            },
          ],
        },
      ],
    },
  };

  assert.deepEqual(flattenGooglePatentsResults(payload), [
    {
      id: 'patent/CN113264560A/en',
      rank: 0,
      publication_number: 'CN113264560A',
      title: 'Double-coated NCM811 cathode material and preparation method thereof',
      assignee: 'Example Institute',
      inventor: 'Wang Jia Tai',
      priority_date: '2021-05-17',
      filing_date: '2021-05-17',
      publication_date: '2021-08-17',
      grant_date: '',
      language: 'en',
      link: 'https://patents.google.com/patent/CN113264560A/en',
      pdf_link: 'https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf',
      snippet: 'A double-coated NCM811 cathode material.',
    },
  ]);
});

test('parsePatentDetailHtml extracts family and citation signals from result html', () => {
  const html = `
    <article>
      <dd itemprop="publicationNumber">CN113264560A</dd>
      <span itemprop="title">Double-coated NCM811 cathode material</span>
      <a href="https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf" itemprop="pdfLink">Download PDF</a>
      <section itemprop="applications">
        <span>2021-05-17</span>
        <a href="/patent/CN113264560A/en">CN113264560A</a>
      </section>
      <h2>Patent Citations (2)</h2>
      <a href="/patent/US20150104708A1/en">US20150104708A1</a>
      <h2>Cited By (1)</h2>
      <a href="/patent/CN114400320A/en">CN114400320A</a>
      <h2>Similar Documents</h2>
      <a href="/patent/CN113224310A/en">CN113224310A</a>
    </article>
  `;

  assert.deepEqual(parsePatentDetailHtml(html), {
    publication_number: 'CN113264560A',
    title: 'Double-coated NCM811 cathode material',
    pdf_link: 'https://patentimages.storage.googleapis.com/cb/54/04/CN113264560A.pdf',
    family_members: ['CN113264560A'],
    cited_patents: ['US20150104708A1'],
    cited_by_patents: ['CN114400320A'],
    similar_documents: ['CN113224310A'],
  });
});

test('formatFetchFailure explains Google Patents 503 html responses', () => {
  assert.equal(
    formatFetchFailure({
      url: 'https://patents.google.com/xhr/query?url=q%3DNCM811&exp=',
      status: 503,
      contentType: 'text/html; charset=UTF-8',
      body: '<html><head><title>Sorry...</title></head><body>blocked</body></html>',
    }),
    'GET https://patents.google.com/xhr/query?url=q%3DNCM811&exp= failed with 503 (text/html; charset=UTF-8). Google Patents returned an HTML Sorry page; retry later, reduce request volume, or open the public search URL for manual CSV download.',
  );
});

test('extractJinaReaderContent unwraps markdown content for xhr json', () => {
  const wrapped = [
    'Title: ',
    '',
    'URL Source: http://patents.google.com/xhr/query?url=q=%22NCM811%22',
    '',
    'Markdown Content:',
    '{"results":{"total_num_results":1}}',
  ].join('\n');

  assert.equal(extractJinaReaderContent(wrapped), '{"results":{"total_num_results":1}}');
});

test('parsePatentDetailHtml also extracts signals from Jina markdown', () => {
  const markdown = `
Title:

URL Source: http://patents.google.com/xhr/result?id=patent/CN113264560A/en

Markdown Content:
# CN113264560A - Double-coated NCM811 cathode material and preparation method thereof - Google Patents

Double-coated NCM811 cathode material and preparation method thereof [Download PDF](https://patentimages.storage.googleapis.com/cb/54/04/d94ebba9313f88/CN113264560A.pdf)

Publication number CN113264560A Authority CN China

## Applications Claiming Priority (1)
| Application | Priority date | Filing date | Title |
| CN202110532646.4A [CN113264560A (en)](http://patents.google.com/patent/CN113264560A/en) | 2021-05-17 | 2021-05-17 | Double-coated NCM811 cathode material |

## Patent Citations (20)
| [US20150104708A1 (en)](http://patents.google.com/patent/US20150104708A1/en) | 2012-06-21 |

## Cited By (6)
| [CN114400320A (en)](http://patents.google.com/patent/CN114400320A/en) | 2022-01-04 |

## Similar Documents
| [CN113224310A (en)](http://patents.google.com/patent/CN113224310A/en) | 2021-08-06 |
  `;

  assert.deepEqual(parsePatentDetailHtml(markdown), {
    publication_number: 'CN113264560A',
    title: 'Double-coated NCM811 cathode material and preparation method thereof',
    pdf_link: 'https://patentimages.storage.googleapis.com/cb/54/04/d94ebba9313f88/CN113264560A.pdf',
    family_members: ['CN113264560A'],
    cited_patents: ['US20150104708A1'],
    cited_by_patents: ['CN114400320A'],
    similar_documents: ['CN113224310A'],
  });
});

test('relaxGooglePatentsQuery drops process phrases after product terms', () => {
  assert.equal(
    relaxGooglePatentsQuery('"NCM811" cathode "preparation method"'),
    '"NCM811" cathode',
  );
  assert.equal(relaxGooglePatentsQuery('"NCM811" cathode'), '');
});

test('sanitizeDetailTextFilename keeps publication numbers path-safe', () => {
  assert.equal(sanitizeDetailTextFilename('CN 113/264:560 A'), 'CN-113-264-560-A.md');
  assert.equal(sanitizeDetailTextFilename(''), 'patent-detail.md');
});
