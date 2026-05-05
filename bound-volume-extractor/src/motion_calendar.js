/**
 * Motion-calendar entry walker.
 *
 * Two NY3d layouts contain short, line-based motion-calendar entries that the
 * parallel-cite-driven walker in case_boundaries.js skips entirely:
 *
 *  1. The dedicated "Motions for Leave to Appeal" section (printed pp. 901-924
 *     in 30 NY3d). Each printed page lists 15-25 entries; each is one or two
 *     visual lines with the shape:
 *
 *       <caption_lines…>     <below-court info>     <disposition>[*]
 *
 *     Entries are clustered under "Decided <Month Day, Year>" sub-headers and
 *     share a `*` footnote ("Motion for poor person relief dismissed as
 *     academic or denied.") at the foot of the section's last page.
 *
 *  2. NY3d motion-calendar entries interleaved within the Memoranda section
 *     (e.g., motions for amici-curiae briefs, motions to reargue, single-line
 *     reconsideration denials at pp. 947-955 of 30 NY3d). These share printed
 *     pages with substantive memos that DO get parallel-cite blocks; the
 *     case_boundaries walker emits the substantive memo and folds the rest
 *     into its body. We re-extract the line-based entries from the memo
 *     section's text here.
 *
 * Output: a list of fully-formed case objects (same shape as parser.js's main
 * cases.push) ready to be merged into the parser's output. CURIEs are assigned
 * downstream by `assignCuries`.
 *
 * Reporter-gated to NY3d. AD3d / Misc 3d don't use this layout — AD3d memos
 * use numbered prefixes (handled in case_boundaries.js); Misc 3d trial cases
 * don't have a Court-of-Appeals-style motion calendar at all.
 */

import { caseCurieBase, slugName } from './curie.js';
import { normalizeMatterOf } from './toc_parser.js';

// --- shared regexes -------------------------------------------------------

// Disposition word at end of an entry (with optional `*` footnote marker).
// Word-anchored at end of trimmed line so we don't false-match body-text
// usage like "her motion was denied by the trial court".
const DISPOSITION_END_RE = /\b(denied|granted|dismissed|withdrawn)(\*)?$/i;

// "Decided <Month Day, Year>" cluster header. Used to group entries by the
// date the Court of Appeals issued the order. Comma is optional in some
// volumes ("Decided October 12 2017").
const DATE_CLUSTER_RE = /^Decided\s+([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})\s*$/;

// Page-runhead recognizers — we strip these BEFORE entry walking so they
// don't get folded into captions. The motions section uses both forms
// observed in 30 NY3d: the section banner with page number on right pages
// (`MOTIONS FOR LEAVE TO APPEAL 903`) and the verso running head
// (`902 30 NEW YORK REPORTS, 3d SERIES`).
const RUNHEAD_RE = new RegExp(
  '^(?:' +
    'MOTIONS\\s+FOR\\s+LEAVE\\s+TO\\s+APPEAL(?:\\s+\\d+)?' +
    '|' +
    'GRANTED\\s+OR\\s+DENIED' +
    '|' +
    '\\d+\\s+\\d+\\s*NEW\\s*YORK\\s+REPORTS,?\\s+3d\\s+SERIES' +
    '|' +
    'MEMORANDA(?:\\s+\\d+)?' +
  ')$',
  'i'
);

// Footnote text that lives at the bottom of motion section's last page.
// Filtered out so it doesn't confuse the disposition-detection walker
// (it contains "dismissed" and "denied").
const FOOTNOTE_PREFIX_RE = /^\*\s*Motion\s+for\s+poor\s+person/i;

// Bare-page-number-only line (e.g., "901" at the bottom of the first
// motion-section page). Strip these.
const BARE_PAGE_RE = /^\d{1,5}$/;

// "Below-court" boundary patterns. Once we see one of these, everything
// before it on the entry's joined line is the caption; everything from the
// pattern through the disposition is "court info + below-cite + disposition".
// NY ordinal abbreviations use "d" (not "rd"/"nd") — "2d Dept", "3d Dept" —
// alongside "1st" / "4th". Accept all four.
const NY_ORDINAL = '(?:st|nd|rd|th|d)';
const BELOW_COURT_RE = new RegExp(
  '\\b(\\d+' + NY_ORDINAL + '\\s+Dept|App\\s+Div|App\\s+Term|Sup\\s+Ct|Ct\\s+Cl|Surrogate\'s\\s+Ct|Family\\s+Ct|City\\s+Ct|Civ\\s+Ct|Crim\\s+Ct|Dist\\s+Ct)\\b',
  'i'
);

// Department parser for `1st Dept` / `2d Dept` / etc. Returns 1-4 or null.
function parseDept(text) {
  const m = text.match(new RegExp('(\\d+)' + NY_ORDINAL + '\\s+Dept', 'i'));
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return n >= 1 && n <= 4 ? n : null;
}

// Decision-date normalizer: "October 12, 2017" → "2017-10-12".
const MONTHS = {
  january: '01', february: '02', march: '03', april: '04',
  may: '05', june: '06', july: '07', august: '08',
  september: '09', october: '10', november: '11', december: '12',
  // Sometimes abbreviated in headers (we accept these defensively).
  jan: '01', feb: '02', mar: '03', apr: '04', jun: '06', jul: '07',
  aug: '08', sept: '09', sep: '09', oct: '10', nov: '11', dec: '12',
};
function normalizeDateToISO(text) {
  if (!text) return null;
  const m = text.match(/^([A-Z][a-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})$/);
  if (!m) return null;
  const month = MONTHS[m[1].toLowerCase()];
  if (!month) return null;
  return `${m[3]}-${month}-${String(parseInt(m[2], 10)).padStart(2, '0')}`;
}

// --- printed page detector -----------------------------------------------

// The printed page number for a motion-section page is found in one of two
// running heads: `MOTIONS FOR LEAVE TO APPEAL <N>` (recto) or `<N> 30 NEW
// YORK REPORTS, 3d SERIES` (verso). The very first motion page (PDF index
// 830 in 30 NY3d) has neither — its printed page is on the BOTTOM as a
// bare digit (e.g., "901"). Try, in order: recto runhead, verso runhead,
// trailing bare-digit line.
function detectPrintedPage(page) {
  const lines = (page.text_raw || '').split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) return null;

  // Recto: "MOTIONS FOR LEAVE TO APPEAL 903"
  for (const l of lines.slice(0, 2)) {
    const m = l.match(/^MOTIONS\s+FOR\s+LEAVE\s+TO\s+APPEAL\s+(\d+)$/i);
    if (m) return parseInt(m[1], 10);
  }
  // Verso: "902 30 NEW YORK REPORTS, 3d SERIES"
  for (const l of lines.slice(0, 2)) {
    const m = l.match(/^(\d+)\s+\d+\s*NEW\s*YORK\s+REPORTS,?\s+3d\s+SERIES$/i);
    if (m) return parseInt(m[1], 10);
  }
  // Trailing bare-digit (first page of section, no runhead).
  for (let i = lines.length - 1; i >= Math.max(0, lines.length - 3); i--) {
    if (BARE_PAGE_RE.test(lines[i])) return parseInt(lines[i], 10);
  }
  return null;
}

// --- entry walker --------------------------------------------------------

/**
 * Walk one page's text, returning a list of raw entry blocks:
 *   { lines: string[], decisionDate: string|null }
 *
 * Each block ends with a line whose trim matches DISPOSITION_END_RE.
 * `decisionDate` is the ISO date from the most-recent `Decided <X>` cluster
 * header on this OR any preceding page (caller threads this state through).
 */
function walkPageEntries(page, state) {
  const text = page.text_raw || '';
  const lines = text.split('\n').map(l => l.trim());

  const entries = [];
  let pending = [];

  for (const line of lines) {
    if (!line) continue;
    if (RUNHEAD_RE.test(line)) continue;
    if (FOOTNOTE_PREFIX_RE.test(line)) continue;
    if (BARE_PAGE_RE.test(line)) continue;

    const dateHdr = line.match(DATE_CLUSTER_RE);
    if (dateHdr) {
      // Date headers boundary entries: a date can land mid-stream when one
      // cluster closes and another opens on the same page. Anything we've
      // accumulated without a disposition is a partial entry — drop it
      // rather than mis-attributing to the new date.
      pending = [];
      state.decisionDate = normalizeDateToISO(dateHdr[1]) || state.decisionDate;
      continue;
    }

    pending.push(line);
    if (DISPOSITION_END_RE.test(line)) {
      entries.push({ lines: pending, decisionDate: state.decisionDate });
      pending = [];
    }
  }
  // Anything left in `pending` after the page ends carries forward to the
  // next page (caller appends it to that page's first entry's lines).
  return { entries, carryover: pending };
}

// --- entry parser (one block of lines → structured fields) ---------------

function parseEntryBlock(block) {
  // Join the entry's lines with single spaces. This collapses caption-wraps
  // and below-cite wraps into a single string we can dissect by regex.
  const joined = block.lines.join(' ').replace(/\s+/g, ' ').trim();

  // Disposition is at the END.
  const dispMatch = joined.match(DISPOSITION_END_RE);
  if (!dispMatch) return null;
  const disposition = dispMatch[1].toLowerCase();
  const dispNote = !!dispMatch[2];
  const beforeDisp = joined.slice(0, dispMatch.index).trim();

  // Below-court boundary splits caption from court-info+cite.
  const courtMatch = beforeDisp.match(BELOW_COURT_RE);
  let caption, belowInfo;
  if (courtMatch) {
    caption = beforeDisp.slice(0, courtMatch.index).trim().replace(/[,\s]+$/, '');
    belowInfo = beforeDisp.slice(courtMatch.index).trim();
  } else {
    // Some entries have no recognizable below-court phrase (rare). Fall
    // back to "everything before disposition is caption".
    caption = beforeDisp;
    belowInfo = '';
  }

  if (!caption) return null;

  // Court department from the below-info string.
  const courtDepartment = parseDept(belowInfo);

  return {
    caption,
    belowInfo,
    courtDepartment,
    disposition,
    dispNote,
    decisionDate: block.decisionDate,
  };
}

// --- case-object assembly ------------------------------------------------

function buildMotionCase(parsed, page, printedPage, volumeMeta, pdfUrlBase, sectionLabel) {
  const reporter = volumeMeta.reporter; // 'NY3d'
  const vol = volumeMeta.volume;
  const citation = `${vol} ${reporter} ${printedPage}`;

  const dispText = parsed.dispNote
    ? `${parsed.disposition} (motion for poor person relief dismissed as academic or denied)`
    : parsed.disposition;
  const opinionText = parsed.belowInfo
    ? `Motion for leave to appeal ${dispText}. Reported below: ${parsed.belowInfo}.`
    : `Motion for leave to appeal ${dispText}.`;

  const sourceUrl = pdfUrlBase
    ? `${pdfUrlBase}#page=${page.page_index + 1}`
    : null;

  const normalizedCaption = normalizeMatterOf(parsed.caption);
  return {
    name: normalizedCaption,
    toc_name: null,
    caption_text: parsed.caption,
    running_head_name: null,
    captions: [{
      caption_index: 0,
      name: parsed.caption,
      name_abbreviation: normalizedCaption,
      docket_number: null,
    }],
    decision_date: parsed.decisionDate,
    argued_date: null,
    argued_or_submitted: 'decided_only',
    court_department: parsed.courtDepartment,
    docket_number: null,
    first_page: printedPage,
    last_page: null,
    citation,
    parallel_cites: [],
    court_name: 'Court of Appeals',
    source_url: sourceUrl,
    disposition_line: opinionText,
    opinions: [{
      opinion_index: 0,
      opinion_type: 'memorandum',
      author: null,
      start_page_index: page.page_index,
      end_page_index: page.page_index,
      text: opinionText,
      footnotes: [],
      page_breaks: [],
    }],
    footnotes: [],
    provenance: {
      section: sectionLabel,
      start_page_index: page.page_index,
      end_page_index: page.page_index,
    },
  };
}

// --- public entry points -------------------------------------------------

/**
 * Walk the dedicated "Motions for Leave to Appeal" section. Returns case
 * objects ready to merge into the parser's output. NY3d only.
 */
export function walkMotionsSection(pages, classification, volumeMeta) {
  if (volumeMeta?.reporter !== 'NY3d') return [];

  // Filter to motion-section pages, in original order.
  const inSection = pages.filter((_, i) => classification[i].section === 'motions');
  if (!inSection.length) return [];

  const reporterToken = String(volumeMeta.reporter).replace(/\s+/g, '');
  const pdfUrlBase = volumeMeta.volume != null
    ? `https://nycourts.gov/reporter/files/bv/${volumeMeta.volume}${reporterToken}.pdf`
    : null;

  const cases = [];
  const state = { decisionDate: null };
  let carryover = [];

  for (const page of inSection) {
    const printedPage = detectPrintedPage(page);
    if (!printedPage) continue;

    const { entries, carryover: nextCarry } = walkPageEntries(page, state);

    // Prepend any carryover lines from the previous page to the first entry
    // on THIS page (cross-page entry-wrap case). The disposition lives on
    // this side, so first_page (and citation) is this page's printed number.
    if (carryover.length && entries.length > 0) {
      entries[0].lines = [...carryover, ...entries[0].lines];
    }
    carryover = nextCarry;

    for (const block of entries) {
      const parsed = parseEntryBlock(block);
      if (!parsed) continue;
      cases.push(buildMotionCase(parsed, page, printedPage, volumeMeta, pdfUrlBase, 'motions'));
    }
  }
  return cases;
}

// Substantive memo body-open signal. Each substantive NY3d memo carries a
// parallel-cite block `[<vol> NE3d <pg>, <vol> NYS3d <pg>]` immediately above
// its caption, OR (motion-calendar form) at the end of its caption block. The
// SAME regex anchors both. We use the parallel cite as the body-start anchor
// because every substantive memo has one, and motion-calendar entries (which
// the walker below tries to extract) never do.
//
// We also accept `SUMMARY` / `HEADNOTES?` / `OPINION OF THE COURT` as a
// secondary body-open signal — for the rare case where the parallel cite was
// detected by the main walker but the close phrase isn't present (e.g.,
// per-curiam dispositions that say "in a per curiam opinion" instead of "in a
// memorandum").
const BODY_OPEN_PARALLEL = /\[\s*\d+\s*NE3d\s*\d+\s*,\s*\d+\s*NYS3d\s*\d+\s*\]/g;
const BODY_OPEN_BANNER = /^(?:SUMMARY|HEADNOTES?|APPEARANCES OF COUNSEL|OPINION OF THE COURT)\b/gm;

// Substantive memo body-close signal. Variants observed:
//   "Order affirmed, in a memorandum."
//   "Order, insofar as appealed from, affirmed, in a memorandum."
//   "Order reversed, in a memorandum."
//   "Judgment affirmed, in a memorandum."
// `[\s\S]` (not `[^.\n]`) lets the "in a memorandum" tail wrap across a
// pdfplumber-injected newline, which happens when the typesetter splits the
// phrase across lines. Bounded to 300 chars to keep the regex from running
// away on a runaway no-period stretch.
const BODY_CLOSE_RE = /\b(?:Order|Judgment|Decree)[\s\S]{0,300}?in\s+a\s+memoran-?\s*dum\./g;

// Multi-line motion-entry date marker. NY3d motion-calendar entries always
// have either "Submitted X; decided Y" or "Decided Y" right after the caption.
// "Argued X; decided Y" is also valid for motion entries that came up on the
// argument calendar before being decided as motion-form orders.
const MEMO_MOTION_DATE_RE =
  /(?:^|\n)\s*(?:Submitted[\s\S]{0,100}?;\s*decided|Argued[\s\S]{0,100}?;\s*decided|Decided)\s+[A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4}/g;

// Single-line motion-calendar entry. Two-line layout dominant in 30 NY3d
// pages 947-955 / 1012-1023 / etc.:
//
//   People v Agard (Kenith)  1st Dept: 151 AD3d 456 (NY)  denied 9/18/17
//   (Stein, J.)
//
// Below-cite info can wrap onto line 2 before the judge:
//
//   People v Aguilar App Div, 1st Dept: 2017 NY Slip Op denied 9/7/17
//   63269(U) (NY) (Rivera, J.)
//
// Anchor: a line ending `<disposition>\s+M/D/YY` (or M/D/YYYY) where the
// disposition word is at end of the visible content. Line N+1 carries the
// judge in parentheses (`(Surname, J.)` / `(Surname, Ch. J.)` / `(Surname,
// Ch. J. and Surname, J.)`).
const SINGLE_LINE_END_RE =
  /^(.+?)\s+\b(denied|granted|dismissed)\b\s+(\d{1,2}\/\d{1,2}\/\d{2,4})\s*$/;
const JUDGE_LINE_RE =
  /\(([A-Z][A-Za-z]+(?:[\s,]+(?:Ch\.\s+)?[A-Z][a-z]+)?\s*,?\s*(?:Ch\.\s+)?J\.?(?:\s+and[^)]+)?)\)/;

/**
 * Detect the character spans of substantive memo bodies in the concatenated
 * memoranda text. Each span runs from the substantive memo's anchor (its
 * parallel-cite block, OR a body banner if no parallel cite is nearby) BACK
 * through the memo's caption + date header (so the motion walker doesn't
 * mistake that date line for a motion entry), then FORWARD to the matching
 * `Order ... in a memorandum.` close.
 *
 * Returns sorted, non-overlapping `{start, end}` ranges.
 */
function detectBodySpans(text) {
  // Collect all parallel-cite block positions and body-banner positions.
  const anchors = [];
  BODY_OPEN_PARALLEL.lastIndex = 0;
  let m;
  while ((m = BODY_OPEN_PARALLEL.exec(text)) !== null) {
    anchors.push({ index: m.index, kind: 'parallel' });
  }
  BODY_OPEN_BANNER.lastIndex = 0;
  while ((m = BODY_OPEN_BANNER.exec(text)) !== null) {
    anchors.push({ index: m.index, kind: 'banner' });
  }
  anchors.sort((a, b) => a.index - b.index);

  const spans = [];
  for (const anchor of anchors) {
    // De-dup against existing spans (multiple anchors per memo are common —
    // SUMMARY + HEADNOTE + APPEARANCES + OPINION all live inside one body).
    const prev = spans[spans.length - 1];
    if (prev && anchor.index < prev.end) continue;

    // Body span starts at the anchor itself (parallel-cite block or banner).
    // We do NOT walk back — that would absorb adjacent motion-calendar
    // entries that happen to live right above a substantive memo's parallel
    // cite (e.g., the Prometheus + Eric White amici motions on 30 NY3d 941
    // sit immediately above Campbell's parallel-cite).
    //
    // For NY3d substantive memos in regular layout (parallel cite → caption
    // → "Argued/Submitted/Decided" → SUMMARY → body → "Order ... in a
    // memorandum."), the cite-at-top means the date line and headers are
    // already inside the span, so the motion walker won't see them.
    const startOffset = anchor.index;

    // Walk forward to the closing phrase. If none is found before EOF,
    // mask through the end as a safety net (rare; would imply the close
    // phrase typesetting changed).
    BODY_CLOSE_RE.lastIndex = anchor.index;
    const close = BODY_CLOSE_RE.exec(text);
    const endOffset = close ? close.index + close[0].length : text.length;
    spans.push({ start: startOffset, end: endOffset });
  }
  return spans;
}

/**
 * Subtract `bodySpans` from `[0, textLength)`. Returns sorted, non-overlapping
 * `{start, end}` ranges that represent motion-calendar territory.
 */
function complementSpans(textLength, bodySpans) {
  const out = [];
  let cursor = 0;
  for (const span of bodySpans) {
    if (span.start > cursor) out.push({ start: cursor, end: span.start });
    cursor = Math.max(cursor, span.end);
  }
  if (cursor < textLength) out.push({ start: cursor, end: textLength });
  return out;
}

/**
 * Walk back from `dateOffset` (start of a Submitted/Decided line) through the
 * lines above to find the caption block. Stops at:
 *   - non-caption lines (concur/order/ruling text via NON_CAPTION_RE in
 *     case_boundaries.js — re-implemented here to keep this module decoupled)
 *   - section banners (MEMORANDA <n>, <n> 30 NEW YORK REPORTS...)
 *   - parallel-cite blocks `[<vol> NE3d <pg>, <vol> NYS3d <pg>]`
 *   - the previous date line (set as `lowerBound`)
 *
 * Returns the caption start offset (relative to `text`) or -1 if no caption
 * was found.
 */
const NON_CAPTION_KEYWORDS = /\b(?:concur|denied|granted|dismissed|affirmed|reversed|withdrawn|adjudged|taking\s+no\s+part|sua\s+sponte|Ordered\s+that|Judgment\s+entered|in\s+a\s+memorandum)\b/i;
const SECTION_RUNHEAD = /^(?:MEMORANDA|MOTION\s+DECISIONS?|MOTIONS|MEMORANDUM)\s+\d+\s*$|^\d+\s+\d+\s*NEW\s*YORK\s+REPORTS,?\s+3d\s+SERIES\s*$/i;
const PARALLEL_CITE = /\[\s*\d+\s*NE3d\s*\d+\s*,\s*\d+\s*NYS3d\s*\d+\s*\]/;
const ROLE_END = /\b(?:Appellants?|Respondents?|Plaintiffs?|Defendants?|Petitioners?|Intervenors?)\b\.?\s*$/;
const MATTER_OF = /^In\s+the\s+Matter\s+of\b/i;

function looksLikeCaptionLine(line) {
  const l = line.trim();
  if (!l) return false;
  if (MATTER_OF.test(l)) return true;
  if (ROLE_END.test(l)) return true;
  // All-caps small-caps fragment line ("ROMETHEUS EALTY ORP", "RIC HITE")
  if (/^[A-Z][A-Z\s.\-,'’()&]*$/.test(l) && !/[a-z]/.test(l)) return true;
  // Mixed case starting with capital, contains comma — caption-shape.
  if (/^[A-Z]/.test(l) && /,/.test(l) && !NON_CAPTION_KEYWORDS.test(l)) return true;
  return false;
}

function findCaptionStart(text, dateOffset, lowerBound) {
  const window = text.slice(lowerBound, dateOffset);
  const lines = window.split('\n');
  let captionStart = lines.length;
  let sawDefiniteCaptionLine = false;
  for (let i = lines.length - 1; i >= 0; i--) {
    const l = lines[i].trim();
    if (!l) {
      if (captionStart < lines.length) captionStart = i;
      continue;
    }
    if (NON_CAPTION_KEYWORDS.test(l)) break;
    if (SECTION_RUNHEAD.test(l)) break;
    if (PARALLEL_CITE.test(l)) break;
    if (looksLikeCaptionLine(l)) {
      captionStart = i;
      if (MATTER_OF.test(l) || ROLE_END.test(l)) sawDefiniteCaptionLine = true;
      continue;
    }
    break; // Non-caption, non-keyword: stop here.
  }
  if (!sawDefiniteCaptionLine || captionStart >= lines.length) return -1;
  // Convert line index back to character offset in `text`.
  const linesBefore = lines.slice(0, captionStart).join('\n');
  return lowerBound + linesBefore.length + (captionStart > 0 ? 1 : 0);
}

/**
 * Reduce a multi-line caption block to a single-line display string. Joins
 * with single spaces and strips section runheads / blank lines.
 */
function flattenCaption(text) {
  return text
    .split('\n')
    .map(l => l.trim())
    .filter(l => l && !SECTION_RUNHEAD.test(l) && !RUNHEAD_RE.test(l))
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Best-effort disposition extraction from the ruling text. Returns lowercase
 * keyword if a clean phrase is present; null otherwise. Used to populate the
 * disposition_line field for the resulting case.
 */
function extractDisposition(rulingText) {
  // Common ruling-end patterns that signal disposition cleanly:
  //   "...granted and the proposed brief is accepted..."
  //   "...is denied."
  //   "Motion ... dismissed..."
  //   "Appeal dismissed..."
  const m = rulingText.match(/\b(granted|denied|dismissed|withdrawn|accepted|stricken)\b/i);
  return m ? m[1].toLowerCase() : null;
}

/**
 * Look up which page record (in pageBounds) contains the given concat-text
 * offset. Returns the entry or null.
 */
function pageForOffset(pageBounds, offset) {
  for (const b of pageBounds) {
    if (offset >= b.start && offset < b.end) return b;
  }
  // Also include the right-boundary of the last page.
  const last = pageBounds[pageBounds.length - 1];
  if (last && offset === last.end) return last;
  return null;
}

/**
 * Parse multi-line motion-calendar entries within an unmasked region of the
 * memoranda section. Each entry: caption + "Submitted X; decided Y" (or
 * "Decided Y") + ruling text (multiple sentences).
 *
 * Returns case objects ready to merge into the parser's output.
 */
function parseEntriesInRegion(text, regionStart, regionEnd, pageBounds, volumeMeta, pdfUrlBase) {
  const segment = text.slice(regionStart, regionEnd);
  if (!segment.trim()) return [];

  // Find every date line. Use lastIndex re-init pattern; the regex has /g.
  MEMO_MOTION_DATE_RE.lastIndex = 0;
  const dates = [];
  let m;
  while ((m = MEMO_MOTION_DATE_RE.exec(segment)) !== null) {
    // m.index points at the leading newline (or 0); the actual date phrase
    // starts after the newline + optional whitespace. We capture both
    // bounds in concat coordinates.
    const phraseStartLocal = m[0].search(/\S/) === -1 ? m.index : m.index + m[0].search(/\S/);
    dates.push({
      phraseStart: regionStart + phraseStartLocal,
      phraseEnd: regionStart + m.index + m[0].length,
      raw: m[0].trim(),
    });
  }
  if (!dates.length) return [];

  const cases = [];
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const lowerBound = i > 0 ? dates[i - 1].phraseEnd : regionStart;

    const captionStart = findCaptionStart(text, date.phraseStart, lowerBound);
    if (captionStart === -1) continue;

    const captionRaw = text.slice(captionStart, date.phraseStart);
    const caption = flattenCaption(captionRaw);
    if (!caption) continue;

    // Ruling: from end of date line to caption start of NEXT entry, OR
    // end of region for the last one.
    let rulingEnd;
    if (i + 1 < dates.length) {
      const nextCap = findCaptionStart(text, dates[i + 1].phraseStart, date.phraseEnd);
      rulingEnd = nextCap !== -1 ? nextCap : dates[i + 1].phraseStart;
    } else {
      rulingEnd = regionEnd;
    }
    const rulingText = text.slice(date.phraseEnd, rulingEnd).trim();

    // Determine the entry's first_page from the page record that contains
    // captionStart in concat coordinates.
    const page = pageForOffset(pageBounds, captionStart);
    if (!page) continue;
    const printedPage = detectPrintedPage(page.page);
    if (!printedPage) continue;

    // Reduce ruling text to a single-line summary for disposition_line.
    const flatRuling = rulingText.replace(/\s+/g, ' ').trim();
    const disposition = extractDisposition(flatRuling);

    // Parse decision date from "...; decided <Month Day, Year>" or
    // "Decided <Month Day, Year>" — this is the OFFICIAL date for the entry.
    const decidedMatch = date.raw.match(/decided\s+([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})$/i)
                      || date.raw.match(/^Decided\s+([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})$/i);
    const decisionDate = decidedMatch ? normalizeDateToISO(decidedMatch[1]) : null;

    const reporter = volumeMeta.reporter;
    const vol = volumeMeta.volume;
    const citation = `${vol} ${reporter} ${printedPage}`;

    const opinionText = disposition
      ? `${flatRuling}`
      : flatRuling;

    const normalizedCaption = normalizeMatterOf(caption);
    cases.push({
      name: normalizedCaption,
      toc_name: null,
      caption_text: caption,
      running_head_name: null,
      captions: [{
        caption_index: 0,
        name: caption,
        name_abbreviation: normalizedCaption,
        docket_number: null,
      }],
      decision_date: decisionDate,
      argued_date: null,
      argued_or_submitted: /^Argued/i.test(date.raw)
        ? 'argued'
        : /^Submitted/i.test(date.raw) ? 'submitted' : 'decided_only',
      court_department: null,
      docket_number: null,
      first_page: printedPage,
      last_page: null,
      citation,
      parallel_cites: [],
      court_name: 'Court of Appeals',
      source_url: pdfUrlBase
        ? `${pdfUrlBase}#page=${page.page.page_index + 1}`
        : null,
      disposition_line: disposition ? `Motion ${disposition}.` : null,
      opinions: [{
        opinion_index: 0,
        opinion_type: 'memorandum',
        author: null,
        start_page_index: page.page.page_index,
        end_page_index: page.page.page_index,
        text: opinionText,
        footnotes: [],
        page_breaks: [],
      }],
      footnotes: [],
      provenance: {
        section: 'memoranda-motion',
        start_page_index: page.page.page_index,
        end_page_index: page.page.page_index,
      },
    });
  }
  return cases;
}

/**
 * Parse the single-line denial/grant/dismissal entries within an unmasked
 * memoranda region. Each entry is a 2-line block whose first line ends with
 * "<disposition> <M/D/YY>" and whose second line carries the judge name in
 * parentheses. The caption + court-info-and-cite is everything before the
 * disposition word on line 1.
 *
 * Pattern is distinct enough from substantive memo body text that we don't
 * need additional masking — substantive memo bodies don't end lines with
 * "<denied|granted|dismissed> M/D/YY".
 */
function parseSingleLineEntries(text, regionStart, regionEnd, pageBounds, volumeMeta, pdfUrlBase) {
  const segment = text.slice(regionStart, regionEnd);
  if (!segment.trim()) return [];

  // Walk by physical line. Track current printed page from running heads.
  const lines = segment.split('\n');
  const cases = [];
  let currentPrintedPage = null;

  // Compute char offset of each line so we can map back to pages.
  const lineOffsets = [];
  let off = 0;
  for (const l of lines) {
    lineOffsets.push(off);
    off += l.length + 1;
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    // Track printed page from running heads.
    let m = line.match(/^MEMORANDA\s+(\d+)\s*$/i)
         || line.match(/^(\d+)\s+\d+\s*NEW\s*YORK\s+REPORTS,?\s+3d\s+SERIES\s*$/i);
    if (m) {
      currentPrintedPage = parseInt(m[1], 10);
      continue;
    }
    if (RUNHEAD_RE.test(line)) continue;
    if (BARE_PAGE_RE.test(line)) continue;

    // Single-line entry END: "<everything> <disposition> M/D/YY"
    const end = line.match(SINGLE_LINE_END_RE);
    if (!end) continue;

    const beforeDisp = end[1];
    const disposition = end[2].toLowerCase();
    const dispDate = end[3];

    // Below-court boundary inside `beforeDisp` separates caption from
    // court-info+cite. If absent, this line probably isn't a motion entry —
    // skip to avoid false positives on body text that happens to end in
    // "denied 9/13/17" by coincidence.
    const courtMatch = beforeDisp.match(BELOW_COURT_RE);
    if (!courtMatch) continue;
    const captionPart = beforeDisp.slice(0, courtMatch.index).trim().replace(/[,\s]+$/, '');
    const courtCitePart = beforeDisp.slice(courtMatch.index).trim();
    if (!captionPart) continue;

    // Caption sanity: must start with a known caption prefix. Avoids matching
    // body-text lines that contain "1st Dept" mid-sentence.
    if (!/^(?:People|Matter of|In the Matter of|In re|[A-Z][\w.'’&-]*(?:\s+[A-Z][\w.'’&-]*)*\s+v\s+|[A-Z])/i.test(captionPart)) continue;

    // Court department from "1st Dept" / "2d Dept" / etc.
    const courtDepartment = parseDept(courtCitePart);

    // Decision date from M/D/YY (assume 20YY for YY < 50, else 19YY — bound
    // volumes are recent so this is safe; revisit if we ever ingest pre-2000
    // data).
    const dateMatch = dispDate.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    let decisionDate = null;
    if (dateMatch) {
      const yy = parseInt(dateMatch[3], 10);
      const year = dateMatch[3].length === 4 ? yy : (yy < 50 ? 2000 + yy : 1900 + yy);
      decisionDate = `${year}-${String(parseInt(dateMatch[1],10)).padStart(2,'0')}-${String(parseInt(dateMatch[2],10)).padStart(2,'0')}`;
    }

    // Page lookup: line's offset → segment offset → concat offset → page.
    const concatOffset = regionStart + lineOffsets[i];
    const page = pageForOffset(pageBounds, concatOffset);
    if (!page) continue;
    // Prefer the printed-page from the most-recent running head we saw, but
    // fall back to per-page detection if we haven't seen one yet (e.g., the
    // very first entry on a page where the running head is at the very top).
    let printedPage = currentPrintedPage;
    if (!printedPage) printedPage = detectPrintedPage(page.page);
    if (!printedPage) continue;

    // Optional next-line judge for richer opinion text. Not load-bearing —
    // the entry is fully identified from line 1 — but improves the opinion
    // text we emit.
    let judgeText = '';
    if (i + 1 < lines.length) {
      const next = lines[i + 1].trim();
      const jm = next.match(JUDGE_LINE_RE);
      if (jm) judgeText = ` (${jm[1]})`;
    }

    const reporter = volumeMeta.reporter;
    const vol = volumeMeta.volume;
    const citation = `${vol} ${reporter} ${printedPage}`;

    const opinionText = `Motion for leave to appeal ${disposition} ${dispDate}${judgeText}. Reported below: ${courtCitePart}.`;

    const normalizedCaption = normalizeMatterOf(captionPart);
    cases.push({
      name: normalizedCaption,
      toc_name: null,
      caption_text: captionPart,
      running_head_name: null,
      captions: [{
        caption_index: 0,
        name: captionPart,
        name_abbreviation: normalizedCaption,
        docket_number: null,
      }],
      decision_date: decisionDate,
      argued_date: null,
      argued_or_submitted: 'decided_only',
      court_department: courtDepartment,
      docket_number: null,
      first_page: printedPage,
      last_page: null,
      citation,
      parallel_cites: [],
      court_name: 'Court of Appeals',
      source_url: pdfUrlBase
        ? `${pdfUrlBase}#page=${page.page.page_index + 1}`
        : null,
      disposition_line: opinionText,
      opinions: [{
        opinion_index: 0,
        opinion_type: 'memorandum',
        author: null,
        start_page_index: page.page.page_index,
        end_page_index: page.page.page_index,
        text: opinionText,
        footnotes: [],
        page_breaks: [],
      }],
      footnotes: [],
      provenance: {
        section: 'memoranda-motion-line',
        start_page_index: page.page.page_index,
        end_page_index: page.page.page_index,
      },
    });
  }
  return cases;
}

/**
 * Walk NY3d memoranda-section pages and recover line-based motion-calendar
 * entries that the parallel-cite walker missed (amici motions, reargument
 * denials, etc.). Strategy:
 *
 *   1. Concatenate the section's pages into one buffer with a per-page
 *      offset map so we can reverse offsets back to PDF page indices.
 *   2. Detect substantive memo body spans via banner-anchored open/close
 *      pairs (SUMMARY/HEADNOTES/APPEARANCES/OPINION OF THE COURT ...
 *      `Order ... in a memorandum.`). Anything inside a body span is masked
 *      out.
 *   3. In the unmasked complement, scan for date lines and grow each entry
 *      backwards (caption) and forwards (ruling) to its boundaries.
 *
 * NY3d only — AD3d/Misc 3d memos have different conventions and shouldn't
 * be touched here.
 *
 * `existingCaseCuries` is a Set of base-CURIEs already emitted by the main
 * walker; entries that collide with them are dropped to avoid double-counting
 * a substantive memo (e.g., when a memo's caption repeats inside its body).
 */
export function walkMemoMotionEntries(pages, classification, volumeMeta, existingCaseCuries) {
  if (volumeMeta?.reporter !== 'NY3d') return [];

  const inSection = pages.filter((_, i) => classification[i].section === 'memoranda');
  if (!inSection.length) return [];

  const reporterToken = String(volumeMeta.reporter).replace(/\s+/g, '');
  const pdfUrlBase = volumeMeta.volume != null
    ? `https://nycourts.gov/reporter/files/bv/${volumeMeta.volume}${reporterToken}.pdf`
    : null;

  // Concatenate page texts with a fixed separator so offsets stay aligned.
  const SEP = '\n\n';
  const pageBounds = [];
  let cursor = 0;
  const parts = [];
  for (const p of inSection) {
    const text = p.text_raw || '';
    pageBounds.push({ page_index: p.page_index, page: p, start: cursor, end: cursor + text.length });
    parts.push(text);
    cursor += text.length + SEP.length;
  }
  const concat = parts.join(SEP);

  // 1. Mask substantive memo bodies.
  const bodySpans = detectBodySpans(concat);

  // 2. Walk the complement.
  const motionRegions = complementSpans(concat.length, bodySpans);

  const cases = [];
  for (const region of motionRegions) {
    cases.push(...parseEntriesInRegion(concat, region.start, region.end, pageBounds, volumeMeta, pdfUrlBase));
    cases.push(...parseSingleLineEntries(concat, region.start, region.end, pageBounds, volumeMeta, pdfUrlBase));
  }

  // 3. De-dupe: drop entries whose base CURIE collides with a case the main
  //    walker already emitted at the same (vol, page, name-slug). Without
  //    this, captions that the substantive-memo walker rendered the same way
  //    would be double-counted.
  return cases.filter(c => {
    const base = caseCurieBase(volumeMeta, c.first_page, c.name);
    return !(base && existingCaseCuries.has(base));
  });
}

// Exposed for testing.
export const __test__ = {
  parseEntryBlock,
  walkPageEntries,
  detectPrintedPage,
  normalizeDateToISO,
};
