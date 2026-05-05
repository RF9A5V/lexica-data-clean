/**
 * Shared utilities for the slip-op-extractor parsers.
 *
 * All three parsers (html-A, html-B, pdf) emit cases through this module so
 * the bulk-ingest payload shape stays consistent — mirrors the shape produced
 * by `co-data/bound-volume-extractor` so co-collection's inserter can ingest
 * either kind of source uniformly.
 */

import crypto from 'crypto';

export const PARSER_VERSION = '0.1.0';

/**
 * Strip leading zeros from a slip-op number while preserving its string form.
 * "02720" → "2720", "50206" → "50206".
 */
export function canonicalSlipOpNumber(s) {
  const numStr = String(s ?? '').trim();
  if (!/^\d+$/.test(numStr)) return null;
  return String(parseInt(numStr, 10));
}

/**
 * Generate the slip-op CURIE used as the primary dedup key.
 *
 * Format mirrors `generateSlipOpCurie` in co-collection's curieGeneration.js:
 *   `nyslopop:<year>:<num>` for reported, `nyslopopu:<year>:<num>` for
 *   unreported (the (U) variant). Reported and unreported number sequences
 *   may overlap, so the prefix carries the reported flag rather than relying
 *   on the number alone.
 */
export function slipOpCurie({ year, slipOpNumber, isUnreported }) {
  const num = canonicalSlipOpNumber(slipOpNumber);
  if (!Number.isInteger(year) || !num) return null;
  return `${isUnreported ? 'nyslopopu' : 'nyslopop'}:${year}:${num}`;
}

/**
 * Reconstruct the canonical slip-op cite string from its components,
 * preserving the (U) marker for unreported decisions and zero-padding the
 * number to 5 digits (the form used in nycourts.gov URL paths and citation
 * lookups).
 */
export function slipOpCite({ year, slipOpNumber, isUnreported }) {
  const padded = String(slipOpNumber).padStart(5, '0');
  return `${year} NY Slip Op ${padded}${isUnreported ? '(U)' : ''}`;
}

/**
 * Map a court description (free-text from the document) to the synthetic
 * "reporter" name that co-collection's inserter.js consults via COURT_DEFAULTS,
 * AND to the source-DB ref the case should land in. The court-name parsing is
 * intentionally tolerant — the LRB renders the same court several ways across
 * formats and decades.
 *
 * Returns { reporter, source_ref, department } where:
 *   reporter   ∈ 'NY3d' | 'AD3d' | 'Misc 3d' (matches inserter COURT_DEFAULTS)
 *   source_ref ∈ 'ny_supreme' | 'ny_appellate' | 'ny_trial'
 *   department ∈ 1..4 | null  (only set for AD)
 */
export function classifyCourt(courtText) {
  const t = String(courtText || '').toLowerCase().replace(/\s+/g, ' ').trim();

  if (!t) return { reporter: null, source_ref: null, department: null };

  if (/court of appeals/.test(t)) {
    return { reporter: 'NY3d', source_ref: 'ny_supreme', department: null };
  }

  if (/appellate division/.test(t)) {
    const dept = parseDepartment(t);
    return { reporter: 'AD3d', source_ref: 'ny_appellate', department: dept };
  }

  // Appellate Term, Supreme Court (trial-level), Civil Court, Surrogate's,
  // County, Family, Criminal Court — all land in ny_trial as Misc 3d.
  return { reporter: 'Misc 3d', source_ref: 'ny_trial', department: null };
}

const DEPT_WORDS = { first: 1, second: 2, third: 3, fourth: 4 };
function parseDepartment(text) {
  const m = text.match(/\b(first|second|third|fourth)\b\s*department/);
  if (m) return DEPT_WORDS[m[1]];
  return null;
}

/**
 * Court-of-Appeals departments don't exist; 'Appellate Term' uses department
 * vocabulary but routes to ny_trial — only carry the number when the document
 * says "Appellate Division, Nth Department".
 */

/**
 * Compute a stable digest of the source document for the JSON output's audit
 * field. SHA-256, hex-encoded.
 */
export function sha256OfBuffer(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

/**
 * Best-effort date parser for "April 30, 2026" / "October 28, 2003" /
 * "February 6, 2026" — returns ISO YYYY-MM-DD or null. Slip-op HTML formats
 * are inconsistent enough that we don't depend on the JS Date parser here.
 */
const MONTHS = {
  january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
  july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
  jan: 1, feb: 2, mar: 3, apr: 4, jun: 6, jul: 7, aug: 8, sep: 9, sept: 9,
  oct: 10, nov: 11, dec: 12,
};
export function parseLrbDate(text) {
  if (!text) return null;
  const m = String(text)
    .replace(/\s+/g, ' ')
    .match(/([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})/);
  if (!m) return null;
  const month = MONTHS[m[1].toLowerCase()];
  if (!month) return null;
  const day = parseInt(m[2], 10);
  const year = parseInt(m[3], 10);
  if (!day || !year) return null;
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

/**
 * Detect a parallel reporter cite embedded in slip-op metadata.
 * Examples in the wild:
 *   "2003 NY Slip Op 17890 [1 NY3d 29]"        → 1 NY3d 29
 *   "2026 NY Slip Op 50206(U) [88 Misc 3d 129(A)]" → 88 Misc 3d 129(A)
 *
 * Returns { volume, reporter, page, raw } or null.
 */
const PARALLEL_RE = /\[\s*(\d+)\s+(N\.?Y\.?\s*\d?d?|A\.?D\.?\s*\d?d?|Misc\.?\s*\d?d?(?:\s*[A-Z])?)\s+(\d+(?:\s*\([A-Z]\))?)\s*\]/i;
export function findParallelCite(text) {
  if (!text) return null;
  const m = String(text).match(PARALLEL_RE);
  if (!m) return null;
  const volume = parseInt(m[1], 10);
  const reporter = m[2].replace(/\./g, '').replace(/\s+/g, ' ').trim();
  const page = m[3].replace(/\s+/g, '').trim();
  return {
    volume,
    reporter,
    page,
    raw: m[0].replace(/^\[|\]$/g, '').trim(),
  };
}

/**
 * Reconstruct the public source URL.
 *  - HTML: prefer the URL embedded in the document's `saved from url=...`
 *    comment (caller passes it in).
 *  - PDF: deterministic — `https://www.nycourts.gov/reporter/pdfs/<year>/<file>.pdf`.
 *  Returns `null` when the inputs aren't sufficient to reconstruct a URL.
 */
export function pdfPublicUrl({ year, slipOpNumber }) {
  if (!Number.isInteger(year) || !slipOpNumber) return null;
  const padded = String(slipOpNumber).padStart(5, '0');
  return `https://www.nycourts.gov/reporter/pdfs/${year}/${year}_${padded}.pdf`;
}

/**
 * Slugify a name into the lower-hyphen-joined form used by case CURIEs and
 * file paths. Mirrors `slugNameAbbreviation` in co-collection.
 */
export function slugify(s) {
  if (!s) return '';
  return String(s).toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
