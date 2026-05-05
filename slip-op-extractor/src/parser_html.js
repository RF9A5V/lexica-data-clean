/**
 * HTML slip-opinion parser.
 *
 * Handles both LRB layouts:
 *   - 'html-modern'  — `current/3dseries/` URL pattern, semantic CSS classes.
 *   - 'html-legacy'  — older `3dseries/` URL pattern, yellow-table metadata.
 *
 * The two layouts diverge mainly in *where* the metadata lives (a styled div
 * vs. a yellow table) but agree on the schema of what's there: case name,
 * slip-op cite, decision date, court. The extractor branches on layout to
 * find the right elements, then merges into a single normalised case object.
 */

import * as cheerio from 'cheerio';
import { parseSlipOpCiteFromText } from './extract_slip_cite.js';
import {
  classifyCourt,
  parseLrbDate,
  findParallelCite,
  slugify,
  slipOpCurie,
  slipOpCite,
} from './shared.js';

/**
 * Public entry point. Returns a normalised case object (single-element list
 * because the JSON contract is `cases: [...]`) or null on parse failure.
 */
export function parseHtml(html, layout, { sourceUrlOverride } = {}) {
  const $ = cheerio.load(html);

  const sourceUrl = sourceUrlOverride
    ?? extractSavedFromUrl(html)
    ?? null;

  const meta = layout === 'html-modern'
    ? extractMetaModern($)
    : extractMetaLegacy($);

  if (!meta || !meta.slipOp) return null;

  const court = classifyCourt(meta.courtText);
  const opinions = layout === 'html-modern'
    ? extractOpinionsModern($)
    : extractOpinionsLegacy($);

  const curie = slipOpCurie({
    year: meta.slipOp.year,
    slipOpNumber: meta.slipOp.slipOpNumber,
    isUnreported: meta.slipOp.isUnreported,
  });
  if (!curie) return null;

  const citeStr = slipOpCite({
    year: meta.slipOp.year,
    slipOpNumber: meta.slipOp.slipOpNumber,
    isUnreported: meta.slipOp.isUnreported,
  });

  const parallel = meta.parallelCite ? [meta.parallelCite.raw] : [];

  return {
    case_curie: curie,
    name: meta.fullCaption || meta.title || null,
    name_abbreviation: meta.title || null,
    caption_text: meta.fullCaption || meta.title || null,
    decision_date: meta.decisionDate || null,
    docket_number: meta.docketNumber || null,
    first_page: null,
    last_page: null,
    source_url: sourceUrl,
    court_department: court.department,
    citation: citeStr,
    parallel_cites: parallel,
    captions: [],
    opinions,
    _routing: {
      reporter: court.reporter,
      source_ref: court.source_ref,
    },
  };
}

function extractSavedFromUrl(html) {
  const m = html.match(/saved from url=\([^)]+\)([^\s>]+)/);
  return m ? m[1] : null;
}

// ---------------------------------------------------------------------------
// Modern layout (current/3dseries)
// ---------------------------------------------------------------------------

function extractMetaModern($) {
  const info = $('div.case-info');
  if (!info.length) return null;

  const title = info.find('h1').first().text().trim() || null;

  // The case-info div has a sequence of <p> elements: slip cite, decision
  // date, court — in that order across observed samples. We look up by
  // position relative to the slip-cite paragraph since the elements are
  // unlabeled.
  const ps = info.find('p').toArray().map(el => $(el).text().trim());
  let slipOp = null;
  let dateLine = null;
  let courtLine = null;
  for (let i = 0; i < ps.length; i++) {
    const parsed = parseSlipOpCiteFromText(ps[i]);
    if (parsed) {
      slipOp = parsed;
      dateLine = ps[i + 1] || null;
      courtLine = ps[i + 2] || null;
      break;
    }
  }
  if (!slipOp) return null;

  const decisionDate = parseLrbDate(dateLine) || parseLrbDate(
    $('p.current-legal-small-center')
      .filter((_, el) => /Decided and Entered/i.test($(el).text()))
      .first().text()
  );

  const docketLine = $('p.current-legal-small-center')
    .filter((_, el) => /Index No\.|Case No\.|Appeal No\./.test($(el).text()))
    .first().text();
  const docketNumber = pickDocketNumber(docketLine);

  const fullCaption = extractCaptionModern($);
  const parallelCite = findParallelCite($('div.case-info').text());

  return {
    title,
    fullCaption,
    slipOp,
    decisionDate,
    docketNumber,
    courtText: courtLine,
    parallelCite,
  };
}

function extractCaptionModern($) {
  const partyLines = $('div.parties p')
    .toArray()
    .map(el => $(el).text().trim())
    .filter(Boolean);
  if (!partyLines.length) return null;
  return partyLines.join(' ').replace(/\s+/g, ' ').trim();
}

function extractOpinionsModern($) {
  // The opinion body is whatever <p> elements live under <main>/<div
  // class="current-legal-document"> after the case-info / parties / counsel
  // blocks. Page markers are <span class="page">[*N]</span> spans we keep
  // inline so authors of downstream consumers can still see them.
  const root = $('div.current-legal-document').length
    ? $('div.current-legal-document')
    : $('main#main');
  if (!root.length) return [];

  const text = root.find('> p, > div > p, > span.page')
    .toArray()
    .map(el => {
      const $el = $(el);
      if ($el.hasClass('parties')) return '';
      if ($el.parents('div.case-info, div.parties, div.current-counsel-block').length) return '';
      return $el.text().trim();
    })
    .filter(Boolean)
    .join('\n\n');

  if (!text) return [];

  return [{
    opinion_index: 0,
    opinion_type: detectOpinionType(text),
    author: null,
    text,
    page_breaks: [],
    footnotes: [],
  }];
}

// ---------------------------------------------------------------------------
// Legacy layout (3dseries, yellow-table metadata)
// ---------------------------------------------------------------------------

function extractMetaLegacy($) {
  // The first centered table holds case name + slip cite + decision date +
  // court, one per row. We pick by row index since cells aren't labeled.
  const yellowTable = $('table[bgcolor="#FFFF80"], table[bgcolor="FFFF80"]').first();
  if (!yellowTable.length) return null;

  const rowTexts = yellowTable.find('tr').toArray()
    .map(tr => $(tr).text().trim())
    .filter(Boolean);
  if (rowTexts.length < 2) return null;

  const title = rowTexts[0] || null;
  const slipOp = parseSlipOpCiteFromText(rowTexts[1]);
  const decisionDate = parseLrbDate(rowTexts[2]) ||
                       parseLrbDate(rowTexts.find(r => /^Decided/.test(r)));
  const courtText = rowTexts[3] || null;
  const parallelCite = findParallelCite(rowTexts[1]);

  // Full caption lives in the second (cyan) table. Cheerio's .text() drops
  // <br> entirely, which would smash "Murray, Respondent,<br>v<br>Glenn S.
  // Goord, ..." into "Murray, Respondent,vGlenn S. Goord, ...". Replace
  // <br> with a space first, then text-extract.
  const cyanTable = $('table[bgcolor="#99cccc"], table[bgcolor="99cccc"]').first();
  let fullCaption = null;
  if (cyanTable.length) {
    cyanTable.find('br').replaceWith(' ');
    fullCaption = cyanTable.text().trim().replace(/\s+/g, ' ');
  }

  // Docket: legacy layout sometimes has a standalone token like "2024-801 K C"
  // immediately before the cyan table — search nearby text for it.
  const docketNumber = extractDocketLegacy($);

  if (!slipOp) return null;

  return {
    title,
    fullCaption,
    slipOp,
    decisionDate,
    docketNumber,
    courtText,
    parallelCite,
  };
}

function extractDocketLegacy($) {
  // Legacy AD-Term decisions place a docket-style token (e.g. "2024-801 K C")
  // as a free-floating text node between the metadata table and the parties
  // block. Search the body's text content for the canonical Term/CC docket
  // shape: YYYY-NNN followed by 1–2 single-letter county codes.
  const bodyText = $('body').text();
  const m = bodyText.match(/(?:^|\n)\s*(\d{4}-\d{1,5}\s+[A-Z](?:\s+[A-Z])?)\s*(?:\n|$)/);
  if (m) return m[1].replace(/\s+/g, ' ').trim();
  return null;
}

function extractOpinionsLegacy($) {
  // Legacy bodies are a flat soup of <p> and <br>. We strip the metadata
  // tables, then take all remaining paragraph text.
  const $clone = $.root().clone();
  $clone.find('table').remove();
  $clone.find('script, style, form').remove();

  const text = $clone.find('p').toArray()
    .map(el => $(el).text().replace(/\s+/g, ' ').trim())
    .filter(Boolean)
    .join('\n\n');

  if (!text) return [];

  return [{
    opinion_index: 0,
    opinion_type: detectOpinionType(text),
    author: detectOpinionAuthor(text),
    text,
    page_breaks: [],
    footnotes: [],
  }];
}

// ---------------------------------------------------------------------------
// Shared text-mining helpers
// ---------------------------------------------------------------------------

function detectOpinionType(text) {
  if (/^\s*PER CURIAM\b/m.test(text) || /\bper curiam\b/i.test(text.slice(0, 400))) return 'per_curiam';
  if (/MEMORANDUM\b/.test(text.slice(0, 400)) || /MEMORANDUM AND ORDER/.test(text.slice(0, 400))) return 'memorandum';
  if (/OPINION OF THE COURT/i.test(text.slice(0, 400))) return 'majority';
  return 'opinion';
}

function detectOpinionAuthor(text) {
  // "Opinion by Read, J." (CoA) or "By READ, J." in older text.
  const m = text.slice(0, 800).match(/Opinion by\s+([A-Z][A-Za-z\.\- ]+?,\s*[A-Z]{1,3}\.?)\.?/);
  if (m) return m[1].trim();
  const upperMatch = text.slice(0, 800).match(/^By\s+([A-Z][A-Z\.\- ]{2,}?,\s*[A-Z]{1,3}\.?)\.?\s*$/m);
  if (upperMatch) return upperMatch[1].trim();
  return null;
}

function pickDocketNumber(line) {
  if (!line) return null;
  // Modern layout uses `Index No. X|Appeal No. Y|Case No. Z|` — pick Case No.
  // when present, else fall through to Appeal No. or Index No.
  const caseNoMatch = line.match(/Case No\.?\s*([\w\-]+)/i);
  if (caseNoMatch) return caseNoMatch[1].trim();
  const appealNoMatch = line.match(/Appeal No\.?\s*([\w\-]+)/i);
  if (appealNoMatch) return appealNoMatch[1].trim();
  const indexNoMatch = line.match(/Index No\.?\s*([\w\-\/]+)/i);
  if (indexNoMatch) return indexNoMatch[1].trim();
  return null;
}
