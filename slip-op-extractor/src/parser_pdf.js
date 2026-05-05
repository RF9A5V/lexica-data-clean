/**
 * PDF slip-opinion parser.
 *
 * Strategy: shell out to `pdftotext -layout` (system poppler binary) and run
 * the resulting plaintext through pattern-based metadata extraction. Slip-op
 * PDFs from nycourts.gov have a canonical cover page that we mine for cite,
 * date, court, and docket — the opinion body lives below.
 *
 * `pdftotext` is a hard dep (we don't ship a PDF parser). Install via the
 * system package manager (`apt-get install poppler-utils` on Debian/Ubuntu).
 */

import { execFileSync } from 'child_process';
import path from 'path';
import { parseSlipOpCiteFromText } from './extract_slip_cite.js';
import {
  classifyCourt,
  parseLrbDate,
  findParallelCite,
  slipOpCurie,
  slipOpCite,
  pdfPublicUrl,
} from './shared.js';

export function parsePdf(filePath) {
  const text = runPdfToText(filePath);
  if (!text) return null;

  const slipOp = findSlipOpInText(text);
  if (!slipOp) return null;

  const courtText = findCourtLine(text);
  const court = classifyCourt(courtText);
  const decisionDate = findDecisionDate(text);
  const docketNumber = findDocketNumber(text);

  const curie = slipOpCurie({
    year: slipOp.year,
    slipOpNumber: slipOp.slipOpNumber,
    isUnreported: slipOp.isUnreported,
  });
  if (!curie) return null;

  const cite = slipOpCite({
    year: slipOp.year,
    slipOpNumber: slipOp.slipOpNumber,
    isUnreported: slipOp.isUnreported,
  });

  const parallel = findParallelCite(text);
  const parallelCites = parallel ? [parallel.raw] : [];

  const sourceUrl = pdfPublicUrl({
    year: slipOp.year,
    slipOpNumber: slipOp.slipOpNumber,
  });

  // Heuristic for case caption: the cover page's first non-page-number text
  // line is typically the case name (e.g. "Manculich v Five Riverside Towers
  // Owners, Inc."). Take everything up to the first "20XX NY Slip Op" line.
  const titleLine = findTitleLine(text);

  // Opinion body: drop everything up to the second occurrence of the page-1
  // boundary (the cover page repeats some metadata, then the body starts).
  const bodyText = extractOpinionBody(text);

  return {
    case_curie: curie,
    name: titleLine || null,
    name_abbreviation: titleLine || null,
    caption_text: titleLine || null,
    decision_date: decisionDate || null,
    docket_number: docketNumber || null,
    first_page: null,
    last_page: null,
    source_url: sourceUrl,
    court_department: court.department,
    citation: cite,
    parallel_cites: parallelCites,
    captions: [],
    opinions: bodyText ? [{
      opinion_index: 0,
      opinion_type: detectOpinionType(text),
      author: null,
      text: bodyText,
      page_breaks: [],
      footnotes: [],
    }] : [],
    _routing: {
      reporter: court.reporter,
      source_ref: court.source_ref,
    },
  };
}

function runPdfToText(filePath) {
  try {
    return execFileSync('pdftotext', ['-layout', '-enc', 'UTF-8', filePath, '-'], {
      encoding: 'utf8',
      maxBuffer: 32 * 1024 * 1024,
    });
  } catch (e) {
    throw new Error(`pdftotext failed for ${path.basename(filePath)}: ${e.message}`);
  }
}

function findSlipOpInText(text) {
  // Search the first ~3000 characters; cover page is dense, body comes after.
  return parseSlipOpCiteFromText(text.slice(0, 3000));
}

function findCourtLine(text) {
  // Cover page lists court between the slip-op cite and the docket. Pull the
  // first line matching the "Court of Appeals / Appellate Division / Supreme
  // Court / etc" idiom from the top of the document.
  const head = text.slice(0, 3000);
  const lineRe = /(?:Court of Appeals|Appellate Division[, ]+\w+\s+Department|Appellate Term[, ]+[\w ]+Department|Supreme Court[,\.\s][^\n]{0,80}|County Court[^\n]{0,80}|Civil Court[^\n]{0,80}|Surrogate'?s Court[^\n]{0,80}|Family Court[^\n]{0,80}|Criminal Court[^\n]{0,80})/i;
  const m = head.match(lineRe);
  return m ? m[0].trim() : null;
}

function findDecisionDate(text) {
  // The cover page typically lists the decision date right after the slip
  // cite. Best-effort: look for any "<Month> <day>, <year>" in the head.
  const head = text.slice(0, 4000);
  const m = head.match(/[A-Z][a-z]+\s+\d{1,2},\s*\d{4}/);
  if (!m) return null;
  return parseLrbDate(m[0]);
}

const DOCKET_PATTERNS = [
  /Docket Number:\s*(.+)$/im,
  /Index No\.?\s*([\w\-\/]+)/i,
  /Case No\.?\s*([\w\-]+)/i,
];
function findDocketNumber(text) {
  const head = text.slice(0, 5000);
  for (const re of DOCKET_PATTERNS) {
    const m = head.match(re);
    if (m && m[1]) return cleanDocket(m[1]);
  }
  return null;
}

/**
 * Slip-op cover pages sometimes write the docket as "Index No. EFCA…" inside
 * the "Docket Number:" line; the inner prefix is decoration, not part of the
 * docket value. Strip it.
 */
function cleanDocket(raw) {
  return String(raw)
    .trim()
    .replace(/^(Index No\.?|Case No\.?|Docket No\.?|Appeal No\.?)\s+/i, '')
    .trim();
}

function findTitleLine(text) {
  // First non-empty trimmed line of the document is the case caption on the
  // cover page (the cover page's centred header). We strip the right-padding
  // spaces from `pdftotext -layout` output before returning.
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line) continue;
    if (/^\d{4}\s*NY\s*Slip\s*Op/i.test(line)) return null;
    if (line.length > 250) continue;
    return line;
  }
  return null;
}

function extractOpinionBody(text) {
  // Drop the first page (the cover sheet) — pdftotext separates pages with a
  // form-feed (\f). The opinion body starts on page 2.
  const parts = text.split('\f');
  if (parts.length <= 1) return text.trim();
  return parts.slice(1).join('\n\n').trim();
}

function detectOpinionType(text) {
  const head = text.slice(0, 800);
  if (/PER CURIAM/i.test(head)) return 'per_curiam';
  if (/MEMORANDUM/i.test(head)) return 'memorandum';
  if (/DECISION AND ORDER/i.test(head)) return 'decision_and_order';
  return 'opinion';
}
