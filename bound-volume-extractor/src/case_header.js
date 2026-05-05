/**
 * First-page case-header extractor.
 *
 * Each case in the OPINIONS section starts with a fixed template:
 *
 *   <running head: NAME [vol REPORTER pg] vol-pg>     ← right-side only
 *   StatementofCase                                    ← small subtitle
 *   [<NE3d-cite>,<NYS3d-cite>]                         ← parallel cite header
 *   <CAPTION block — multiple lines, party names in small caps>
 *   <Argued|Submitted DATE; decided DATE>              ← date line
 *   SUMMARY                                            ← optional section
 *     <reporter-prepared summary, ending with a procedural disposition>
 *   HEADNOTES (or HEADNOTE)                            ← headnotes block
 *     <numbered, reporter-prepared headnotes>
 *   RESEARCH REFERENCES                                ← end of header
 *     ... opinion body begins thereafter on later pages ...
 *
 * MEMORANDA section first-pages share the same parallel-cite + caption +
 * date line layout but typically lack SUMMARY/HEADNOTES — the body is
 * short ("Order affirmed.") and starts immediately after the date line.
 *
 * We render lines by recombining words (small_caps.js) and grouping by
 * `top` coordinate. Then we walk lines top-to-bottom matching markers.
 *
 * SUMMARY and HEADNOTES blocks are reporter-prepared editorial content
 * (West/NY-Reporter copyright). We detect the section markers — the
 * `header_end_top` they pin lets the body extractor know where the
 * opinion starts — but never extract or emit the text itself.
 *
 * Returns:
 *   {
 *     parallel_cites: [...],     // already detected; passed in
 *     caption_text: string,      // raw caption block (multi-line, joined)
 *     decision_date: string|null,// ISO yyyy-mm-dd
 *     argued_date: string|null,  // ISO; may be null
 *     argued_or_submitted: 'argued' | 'submitted' | null,
 *     header_end_top: number|null,   // y where header ends (opinion body starts on later pages)
 *     warnings: [string],
 *   }
 */

import { recombineWords } from './small_caps.js';
import { spliceOrphanBodyRows } from './case_boundaries.js';

const MONTHS = {
  january:  '01', february: '02', march:    '03', april:   '04',
  may:      '05', june:     '06', july:     '07', august:  '08',
  september:'09', october:  '10', november: '11', december:'12',
};

// Date strings in this PDF often appear with no spaces ("September6,2017").
// This regex is tolerant of optional whitespace between tokens.
const DATE_RE = /([A-Z][a-z]+)\s*(\d{1,2})\s*,\s*(\d{4})/;

// Argued/Submitted-Decided line. Squashed: "ArguedMay30,2017;decidedSeptember7,2017".
// Used by NY3d opinion-section cases. Separator is usually `;` but newer
// volumes use `,` (e.g. 35 NY3d "Argued June 2, 2020, decided June 23, 2020").
// "decided" is usually lowercase; some recent volumes capitalize it
// (e.g. 43 NY3d "Argued ...; Decided November 26, 2024").
const ARGUED_LINE_RE = /^(Argued|Submitted)([\s\S]*?)[;,]\s*[Dd]ecided([\s\S]+)$/;

// Memorandum-style date line: "DecidedOctober12,2017" (no Argued prefix —
// memos are decided on submission without oral argument).
const DECIDED_LINE_RE = /^Decided\s*([A-Z][a-z]+\s*\d{1,2}\s*,\s*\d{4})\.?$/;

// AD3d / Misc 3d date line: court attribution followed by decision date.
// Examples (squashed):
//   "FirstDepartment,November21,2017"
//   "9thand10thJudicialDistricts,May23,2017"
//   "FirstDepartment,February,18,2021" — newer volumes occasionally insert
//     a comma between the month and the day, so the comma after the month
//     is optional.
// The court attribution can span multiple visual lines; we match each line
// individually and detect the one ending in `Month Day, Year`.
const COURT_DATE_LINE_RE = /^(.+),\s*([A-Z][a-z]+),?\s*(\d{1,2})\s*,\s*(\d{4})\.?$/;

// Section markers. Allow optional spaces and singular/plural.
const SUMMARY_MARKER  = /^SUMMARY\.?$/;
const HEADNOTES_MARKER = /^HEADNOTES?\.?$/;
const RESEARCH_MARKER = /^RESEARCH\s*REFERENCES?\.?$/i;
const APPEARANCES_MARKER = /^APPEARANCES?\s*OF\s*COUNSEL\.?$/i;

/**
 * Group recombined words into visual lines using the page's `top` coordinate.
 * Threshold of 2.5pt holds words on the same baseline together while keeping
 * separate lines apart. Returns lines sorted top-to-bottom, each with a
 * joined-text representation.
 */
function buildLines(words) {
  const recombined = recombineWords(words);
  const sorted = recombined.slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
  const lines = [];
  let current = [];
  let currentTop = null;
  for (const w of sorted) {
    if (currentTop === null || Math.abs(w.top - currentTop) <= 2.5) {
      current.push(w);
      if (currentTop === null) currentTop = w.top;
    } else {
      lines.push(makeLine(current, currentTop));
      current = [w];
      currentTop = w.top;
    }
  }
  if (current.length) lines.push(makeLine(current, currentTop));
  // Cross-row body splice: orphan body fragments that wrap from a hyphenated
  // lead on an earlier row (e.g. `Col-` + `LEGE OF THE` → `College of the`).
  // The splice mutates the hyphenated lead's text in-place and drops the
  // orphan row; subsequent code that reads `line.text` sees the fixed form.
  return spliceOrphanBodyRows(lines);
}

function makeLine(words, top) {
  return {
    top,
    x0: Math.min(...words.map(w => w.x0)),
    x1: Math.max(...words.map(w => w.x1)),
    size: words[0].size,
    text: words.map(w => w.text).join(' '),
    words,
  };
}

/**
 * Map a court-attribution prefix (e.g. "First Department",
 * "Second Judicial Department", "Appellate Term, First Department") to its
 * numbered department (1-4). Appellate Division and Appellate Term are both
 * organized into the same four departments by geography. Returns null when
 * no department keyword is present (e.g. trial-court attributions on Misc
 * 3d cases like "9th and 10th Judicial Districts").
 */
export function deriveDepartment(courtAttribution) {
  if (!courtAttribution || typeof courtAttribution !== 'string') return null;
  const m = courtAttribution.match(/\b(First|Second|Third|Fourth)\s+(?:Judicial\s+)?Department\b/i);
  if (!m) return null;
  return { first: 1, second: 2, third: 3, fourth: 4 }[m[1].toLowerCase()] || null;
}

function parseDateString(s) {
  if (!s) return null;
  const m = s.match(DATE_RE);
  if (!m) return null;
  const month = MONTHS[m[1].toLowerCase()];
  if (!month) return null;
  const day = m[2].padStart(2, '0');
  const year = m[3];
  return `${year}-${month}-${day}`;
}

/**
 * Find the index of the date line and parse it. Returns
 *   { idx, kind: 'argued'|'submitted'|'decided_only', argued_date, decision_date }
 * or null if no date line is found.
 *
 * Recognises both opinion-style "Argued X; decided Y" and memo-style
 * "Decided Y" (no oral argument). Optional `startIdx` to begin the search
 * from a specific line — used by `extractCaseHeader` to skip over date lines
 * belonging to motion-calendar entries that share the same page (those
 * dates appear *before* the case's parallel cite, so searching from
 * `parallelIdx + 1` finds the correct case-specific date).
 */
// Date-only line: `Month Day, Year` standing alone. Used for the wrapped
// court-attribution form where the court text fills one line and the date
// lands on the next.
const DATE_ONLY_LINE_RE = /^([A-Z][a-z]+)\s+(\d{1,2})\s*,\s*(\d{4})\.?$/;

function findDateLine(lines, startIdx = 0) {
  for (let i = startIdx; i < lines.length; i++) {
    const t = lines[i].text.trim();
    let m = t.match(ARGUED_LINE_RE);
    if (m) {
      return {
        idx: i,
        kind: m[1].toLowerCase(),
        argued_date:   parseDateString(m[2]),
        decision_date: parseDateString(m[3]),
      };
    }
    m = t.match(DECIDED_LINE_RE);
    if (m) {
      return {
        idx: i,
        kind: 'decided_only',
        argued_date:   null,
        decision_date: parseDateString(m[1]),
      };
    }
    m = t.match(COURT_DATE_LINE_RE);
    if (m) {
      return {
        idx: i,
        kind: 'court_attribution',
        argued_date:   null,
        decision_date: parseDateString(`${m[2]} ${m[3]}, ${m[4]}`),
        court_attribution: m[1],
      };
    }
    // Wrapped court-attribution form: long Misc 3d / Appellate Term court
    // attributions wrap onto two lines, e.g.
    //   "Supreme Court, Appellate Term, Second Department, 9th and 10th Judicial Districts,"
    //   "October 3, 2019"
    // Detected as a date-only line preceded by a non-empty line ending in `,`
    // (the comma is the visual continuation marker).
    m = t.match(DATE_ONLY_LINE_RE);
    if (m && i > startIdx) {
      const prev = lines[i - 1].text.trim();
      if (prev.endsWith(',')) {
        return {
          idx: i,
          kind: 'court_attribution',
          argued_date:   null,
          decision_date: parseDateString(`${m[1]} ${m[2]}, ${m[3]}`),
          court_attribution: prev.replace(/,\s*$/, ''),
        };
      }
    }
  }
  return null;
}

function findSectionLine(lines, startIdx, regex) {
  for (let i = startIdx; i < lines.length; i++) {
    if (regex.test(lines[i].text.trim())) return i;
  }
  return -1;
}

// Running-head signature on a continuation page. We recognize all three
// variants because cross-page header extraction joins lines across the page
// break and we want to skip over the header band on the next page when
// searching for the date line.
const NEXT_PAGE_RUNHEAD_RE = new RegExp(
  '^(?:' +
    '\\d+\\s+\\d+\\s*(?:APPELLATE\\s+DIVISION|NEW\\s*YORK|MISCELLANEOUS)\\s+REPORTS' +
    '|MEMORANDA(?:[,\\s][^\\n]*)?\\s+\\d+' +
    '|.+\\s+\\[\\s*\\d+\\s*(?:NY3d|AD3d|Misc\\s*3d)\\s+\\d+\\]\\s+\\d+' +
  ')$',
  'i'
);

/**
 * Extract the case header from the case's first page. Caller passes the
 * `pageRecord` (from the NDJSON page record) and pre-computed
 * `parallel_cites` from the boundary walker.
 *
 * Optional `nextPageRecord` is consulted only when this page yields no date
 * line — typical of NY3d / Misc 3d motion-decision and per-curiam memos
 * where the parallel cite sits at the bottom of page N and the date line is
 * the first non-runhead line of page N+1. The fallback only borrows the
 * date line; SUMMARY / HEADNOTES extraction stays scoped to the start page
 * because those banners stay with the rest of the header text.
 */
export function extractCaseHeader(pageRecord, parallel_cites, nextPageRecord) {
  const warnings = [];
  const lines = buildLines(pageRecord.words || []);

  // Find the parallel cite line — anchors the start of the caption block.
  // Either form: `[XX NE3d YY, ZZ NYS3d WW]` (NY3d) or `[ZZ NYS3d WW]`
  // (AD3d / Misc 3d).
  const parallelIdx = lines.findIndex(l =>
    /\[\s*\d+\s*NE3d/.test(l.text) || /\[\s*\d+\s*NYS3d/.test(l.text)
  );
  if (parallelIdx === -1) {
    warnings.push('parallel cite header not found in line stream');
  }

  // Date line: opinion-style "Argued X; decided Y" or memo-style "Decided Y".
  // Caption is the lines between parallel cite (exclusive) and date line (exclusive).
  // Search from after the parallel cite — pages that mix motion-calendar
  // entries (cite-at-end layout) above a regular memorandum (cite-at-top
  // layout) have date lines for the motions before the regular memorandum's
  // cite, and we want the regular memorandum's own date line.
  const dateSearchStart = parallelIdx >= 0 ? parallelIdx + 1 : 0;
  let dateInfo = findDateLine(lines, dateSearchStart);
  let dateIdx = dateInfo?.idx ?? -1;
  let crossPageDate = false;

  // Cross-page fallback — see jsdoc on the function.
  if (!dateInfo && nextPageRecord) {
    const nextLines = buildLines(nextPageRecord.words || []);
    // Skip the next page's running-head band. We can't rely on `top` alone
    // because the same band coordinate is reused across pages — instead, drop
    // initial lines that match the runhead patterns. Then scan up to the
    // first SUMMARY / HEADNOTE / OPINION marker, which always sits below the
    // date line. The window is intentionally generous (~40 lines) to handle
    // multi-caption consolidated cases (e.g. 71 Misc 3d 934 Schoharie limo
    // crash, 8+ named plaintiffs spilling onto the next page) where the
    // date line lands well below the caption tail.
    let nextStart = 0;
    while (nextStart < nextLines.length &&
           NEXT_PAGE_RUNHEAD_RE.test(nextLines[nextStart].text.trim())) {
      nextStart++;
    }
    let nextStop = nextLines.length;
    for (let i = nextStart; i < Math.min(nextLines.length, nextStart + 60); i++) {
      const t = nextLines[i].text.trim();
      if (SUMMARY_MARKER.test(t) || HEADNOTES_MARKER.test(t)
          || /^(?:OPINION|DECISION)\s+OF\s+THE\s+COURT$/.test(t)) {
        nextStop = i;
        break;
      }
    }
    const candidate = findDateLine(nextLines.slice(nextStart, nextStop), 0);
    if (candidate) {
      dateInfo = candidate;
      crossPageDate = true;
    }
  }

  if (!dateInfo) {
    warnings.push('date line not found (neither Argued/Submitted–decided nor Decided)');
  }

  let captionText = null;
  if (parallelIdx !== -1 && dateIdx !== -1 && dateIdx > parallelIdx) {
    captionText = lines.slice(parallelIdx + 1, dateIdx)
      .map(l => l.text.trim())
      .filter(Boolean)
      .join(' ');
  } else if (parallelIdx !== -1 && crossPageDate) {
    // Caption fills everything from the parallel cite to end of page when
    // the date line is on the next page.
    captionText = lines.slice(parallelIdx + 1)
      .map(l => l.text.trim())
      .filter(Boolean)
      .join(' ');
  }

  const decisionDate      = dateInfo?.decision_date ?? null;
  const arguedDate        = dateInfo?.argued_date   ?? null;
  const arguedOrSubmitted = dateInfo?.kind          ?? null;

  // SUMMARY / HEADNOTES section markers (everything after the date line).
  // We locate these markers only to anchor `header_end_top` below — the
  // SUMMARY and HEADNOTES blocks themselves are reporter-prepared
  // editorial content and are never extracted or emitted.
  const searchStart = dateIdx === -1 ? 0 : dateIdx + 1;
  const headnotesIdx = findSectionLine(lines, searchStart, HEADNOTES_MARKER);
  const researchIdx  = findSectionLine(lines, searchStart, RESEARCH_MARKER);
  const appearIdx    = findSectionLine(lines, searchStart, APPEARANCES_MARKER);

  // Where does the body begin? On the FIRST page, the header occupies the
  // upper portion; the opinion body itself usually starts on a later page
  // (or below APPEARANCES OF COUNSEL on the same page for short cases).
  // We record `header_end_top` as the y-coordinate of the section that ended
  // the header block, so the opinion-body extractor can know where to start.
  let headerEndTop = null;
  const endIdx = appearIdx !== -1 ? appearIdx + 1
              : researchIdx !== -1 ? researchIdx + 1
              : headnotesIdx !== -1 ? headnotesIdx + 1
              : null;
  if (endIdx !== null && endIdx < lines.length) {
    headerEndTop = lines[endIdx].top;
  }

  if (!captionText) warnings.push('caption block could not be extracted');

  return {
    parallel_cites: parallel_cites || [],
    caption_text: captionText,
    decision_date: decisionDate,
    argued_date: arguedDate,
    argued_or_submitted: arguedOrSubmitted,
    court_department: deriveDepartment(dateInfo?.court_attribution),
    header_end_top: headerEndTop,
    warnings,
  };
}
