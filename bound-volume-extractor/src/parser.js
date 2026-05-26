/**
 * Structural parser: turns extracted page records into a list of cases, each
 * with one or more opinions.
 *
 * Pipeline:
 *   pages
 *     → sections.classifyPages          (front_matter / tables / opinions / motions / memoranda / digest)
 *     → case_boundaries.detectCaseBoundaries  (per-case start/end + parallel cite + canonical citation)
 *     → for each case:
 *         case_header.extractCaseHeader        (caption, decision_date, header_end_top)
 *         case_boundaries.extractRunningHeadName (short cite from continuation-page running head)
 *         opinions.extractOpinions              (per-opinion type/author/text via small subtitle)
 *
 * Returns the contract documented at the top of this file:
 *   { cases: [...], warnings: [...] }
 */

import { classifyPages } from './sections.js';
import { detectCaseBoundaries, extractRunningHeadName } from './case_boundaries.js';
import { extractCaseHeader, deriveDepartment } from './case_header.js';
import { extractOpinions, extractCaseFootnotes } from './opinions.js';
import { buildTocMap, pickTocName } from './toc_parser.js';
import { assignCuries, caseCurieBase } from './curie.js';
import { recombineWords } from './small_caps.js';
import { walkMotionsSection, walkMemoMotionEntries } from './motion_calendar.js';
import { resolveVolumeDepartments } from './department.js';

/**
 * Find AD3d memo-section department banners. AD3d memos are organized into
 * blocks by department (First → Fourth) with a banner page between each
 * block carrying a "<Department>, <Month>, <Year>" line in small-caps
 * (recombined from the lead-cap + body-fragment line pair). Per-memo
 * department is inherited from the most recent banner before the memo's
 * start page.
 *
 * Returns a sorted array of { page_index, department } records. Empty for
 * NY3d / Misc 3d (no banners present).
 */
const BANNER_RE = /\b(First|Second|Third|Fourth)\s+(?:Judicial\s+)?Department\b/i;

function detectDepartmentBanners(pages) {
  const banners = [];
  for (const p of pages) {
    const recombined = recombineWords(p.words || []);
    // Build visual lines (top-grouped, 2.5pt threshold) — same heuristic
    // as case_header.buildLines.
    const sorted = recombined.slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
    let current = [];
    let curTop = null;
    const lines = [];
    for (const w of sorted) {
      if (curTop === null || Math.abs(w.top - curTop) <= 2.5) {
        current.push(w);
        if (curTop === null) curTop = w.top;
      } else {
        lines.push(current.map(x => x.text).join(' '));
        current = [w]; curTop = w.top;
      }
    }
    if (current.length) lines.push(current.map(x => x.text).join(' '));
    for (const text of lines) {
      const m = text.match(BANNER_RE);
      if (m) {
        const dept = deriveDepartment(m[0]);
        if (dept) {
          banners.push({ page_index: p.page_index, department: dept, line: text });
          break;  // one banner per page
        }
      }
    }
  }
  // Banners can re-trigger on within-body mentions of "First Department" in
  // opinion-section text. Filter to banner-style locations: keep only the
  // FIRST occurrence per (department, month-block) and only those where the
  // line contains a standalone year token (banner format is "X Department, M, YYYY").
  return banners.filter(b => /\b\d{4}\b/.test(b.line));
}

/**
 * Resolve a memo's department by looking up its start page against the
 * banner list. The most-recent banner at or before the memo's first page
 * wins. Returns null if no banner precedes the memo.
 */
function resolveDepartmentForPage(banners, pageIndex) {
  let current = null;
  for (const b of banners) {
    if (b.page_index <= pageIndex) current = b.department;
    else break;
  }
  return current;
}

/**
 * Find AD3d memo date-cluster checkpoints. AD3d memos are organized by
 * decision date within each department block; a per-cluster `(Month Day,
 * Year)` line appears at the top of each new date's memos. Each subsequent
 * memo in the same cluster inherits this date until the next checkpoint.
 *
 * Whole-line anchored regex avoids body-text false positives (parenthesized
 * citations like `(Jan. 18, 2018)` inside opinion bodies are NOT isolated
 * lines — they appear mid-sentence).
 *
 * Returns sorted `[{page_index, top, date}]` records.
 */
const MONTHS = {
  january:'01', february:'02', march:'03', april:'04', may:'05', june:'06',
  july:'07', august:'08', september:'09', october:'10', november:'11', december:'12',
};
const DATE_CHECKPOINT_RE = /^\s*\(\s*([A-Z][a-z]+)\s+(\d{1,2})\s*,\s*(\d{4})\s*\)\s*$/;

function detectDateCheckpoints(pages) {
  const checkpoints = [];
  for (const p of pages) {
    const recombined = recombineWords(p.words || []);
    const sorted = recombined.slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
    let current = [];
    let curTop = null;
    const lines = [];
    for (const w of sorted) {
      if (curTop === null || Math.abs(w.top - curTop) <= 2.5) {
        current.push(w);
        if (curTop === null) curTop = w.top;
      } else {
        lines.push({ top: curTop, text: current.map(x => x.text).join(' ') });
        current = [w]; curTop = w.top;
      }
    }
    if (current.length) lines.push({ top: curTop, text: current.map(x => x.text).join(' ') });
    for (const ln of lines) {
      const m = ln.text.match(DATE_CHECKPOINT_RE);
      if (!m) continue;
      const month = MONTHS[m[1].toLowerCase()];
      if (!month) continue;
      const date = `${m[3]}-${month}-${m[2].padStart(2, '0')}`;
      checkpoints.push({ page_index: p.page_index, top: ln.top, date });
    }
  }
  return checkpoints;
}

/**
 * Find a checkpoint's text_raw character offset on its page. Walk lines
 * top-sorted; the line whose top matches the checkpoint marks the position.
 * Returns the cumulative \n count up to that line — comparable to a
 * line-ordinal-based position in the page's text_raw stream.
 */
function lineOrdinalAtTop(page, targetTop) {
  const lines = (page.lines || []).slice().sort((a, b) => a.top - b.top);
  let idx = 0;
  for (const ln of lines) {
    if (ln.top >= targetTop) break;
    idx++;
  }
  return idx;
}

/**
 * Convert a text_raw character offset on a page to a line-ordinal (number
 * of \n characters preceding it). Comparable to lineOrdinalAtTop output.
 */
function lineOrdinalAtOffset(page, offset) {
  const text = page.text_raw || '';
  let count = 0;
  for (let i = 0; i < offset && i < text.length; i++) {
    if (text[i] === '\n') count++;
  }
  return count;
}

/**
 * Resolve a memo's decision date from the date checkpoints. Most-recent
 * checkpoint that comes BEFORE the memo's cite position wins.
 *
 * Cross-page comparison: any checkpoint with page_index < memo.cite_page_index
 * qualifies. Same-page comparison: the checkpoint's line ordinal must be
 * strictly less than the memo's cite line ordinal — this guards the edge
 * case where a single page contains the tail of a cluster + a new
 * checkpoint + the head of the next cluster.
 */
function resolveDateForMemo(checkpoints, pages, range) {
  const citePage = range.cite_page_index ?? range.start_page_index;
  const citeOffset = range.cite_offset ?? 0;
  const citePageRecord = pages.find(p => p.page_index === citePage);
  const memoLineOrd = citePageRecord ? lineOrdinalAtOffset(citePageRecord, citeOffset) : 0;

  let best = null;
  for (const cp of checkpoints) {
    if (cp.page_index > citePage) break;
    if (cp.page_index < citePage) {
      best = cp.date;
      continue;
    }
    // Same page: only count if checkpoint's line ordinal < memo's line ordinal
    const cpLineOrd = citePageRecord ? lineOrdinalAtTop(citePageRecord, cp.top) : 0;
    if (cpLineOrd < memoLineOrd) best = cp.date;
  }
  return best;
}

/**
 * Split a caption block into one or more individual captions. Consolidated
 * appellate decisions (e.g., 30 N.Y.3d 719 = Connolly + Baumann + Heeran
 * v. LIPA) bundle multiple actions in one opinion; the reporter prints
 * each action's full caption back-to-back, separated by a closing role
 * noun + period followed by the next plaintiff name.
 *
 * Boundary heuristic: a caption ends with `, <Role>.` where <Role> is one
 * of the appellate / trial role nouns (Defendant, Appellant, Respondent,
 * Petitioner, Plaintiff — singular or plural, optionally hyphenated like
 * `Defendants-Appellants`). Followed by whitespace + capital letter
 * starting the next caption. Single-caption cases yield one element.
 *
 * Edge cases not yet handled:
 *   - Cross-page caption wraps (rare for first-page captions).
 *   - Captions with no role suffix (attorney-disciplinary matters, etc.).
 *   - Trailing `(And a Third-Party Action.)` parentheticals — kept with
 *     the caption they follow (the lookahead requires `[A-Z]` after the
 *     period, and the parenthetical starts with `(`).
 */
function splitCaptions(captionText) {
  if (!captionText || !captionText.trim()) return [];
  const text = captionText.trim();
  // Role nouns that close a caption. Hyphenated combinations like
  // `Defendants-Appellants` and `Plaintiffs-Respondents` show up when
  // the same party occupied different roles below vs on appeal.
  const ROLE = '(?:Defendant|Appellant|Respondent|Petitioner|Plaintiff)s?(?:-(?:Defendant|Appellant|Respondent|Petitioner|Plaintiff)s?)?';
  const boundaryRe = new RegExp(`(, ${ROLE}\\.)\\s+(?=[A-Z])`, 'g');

  // Find candidate boundaries first; then validate each by checking
  // whether the chunk AFTER it looks like a real caption (`, v ` action
  // keyword + at least some lowercase). This filters out two false-
  // positive sources:
  //   - Small-caps body remnants left over from byline parsing (e.g.,
  //     `, Appellant. BERTO` where `BERTO` is the rest of the surname).
  //   - Multi-respondent single actions (`, Respondents. Workers' Comp
  //     Board, Respondent.`) where the trailing party list isn't a
  //     separate action.
  const candidates = [];
  let m;
  while ((m = boundaryRe.exec(text)) !== null) {
    candidates.push({ index: m.index, matchLen: m[1].length, full: m[0].length });
  }

  const captions = [];
  let lastEnd = 0;
  for (let i = 0; i < candidates.length; i++) {
    const cand = candidates[i];
    const nextStart = cand.index + cand.full;
    const nextEnd = i + 1 < candidates.length
      ? candidates[i + 1].index + candidates[i + 1].matchLen
      : text.length;
    const nextChunk = text.slice(nextStart, nextEnd);
    const looksLikeCaption =
      /,\s+v\s+/.test(nextChunk) &&
      /[a-z]/.test(nextChunk);
    if (!looksLikeCaption) continue;
    const captionEnd = cand.index + cand.matchLen;
    captions.push(text.slice(lastEnd, captionEnd).trim());
    lastEnd = nextStart;
  }
  const tail = text.slice(lastEnd).trim();
  if (tail) captions.push(tail);
  return captions.length ? captions : [text];
}

export function parseCases(pages, volumeMeta) {
  const warnings = [];

  if (!volumeMeta) {
    warnings.push('volume metadata not detected — output will lack reporter/volume context');
    return { cases: [], warnings };
  }

  const sortedPages = pages.slice().sort((a, b) => a.page_index - b.page_index);
  const classification = classifyPages(sortedPages);

  const ranges = detectCaseBoundaries(sortedPages, classification, volumeMeta);
  if (!ranges.length) {
    warnings.push('no case boundaries detected — check section classifier and parallel-cite header pattern');
    return { cases: [], warnings };
  }

  const courtName = volumeMeta.court || null;
  const reporter  = volumeMeta.reporter;
  // PDF URL on nycourts.gov for this volume; per-case fragment is appended
  // below using each case's start_page_index. Filename convention is
  // `<volume><reporter-no-space>.pdf`.
  const reporterToken = reporter ? String(reporter).replace(/\s+/g, '') : null;
  const pdfUrlBase = (reporterToken && volumeMeta.volume != null)
    ? `https://nycourts.gov/reporter/files/bv/${volumeMeta.volume}${reporterToken}.pdf`
    : null;

  // Build the (volume_page → name) lookup from the volume's Table of Cases.
  // ToC names are the reporter-edited canonical form ("Matter of Ayres" vs.
  // the running-head abbreviation "MTR OF AYRES"). Falls back to
  // running-head name and then caption text when ToC has no entry.
  const tocMap = buildTocMap(sortedPages, classification, volumeMeta);

  // AD3d memo-section department banners. Memos inherit department from the
  // most-recent banner before their start page. NY3d / Misc 3d return [].
  const departmentBanners = (reporter === 'AD3d')
    ? detectDepartmentBanners(sortedPages)
    : [];

  // AD3d memo date-cluster checkpoints. Each `(Month Day, Year)` line marks
  // the start of a date-cluster within a department block. Memos inherit
  // their decision_date from the most-recent checkpoint before their cite
  // position. NY3d / Misc 3d return [] (no checkpoints).
  const dateCheckpoints = (reporter === 'AD3d')
    ? detectDateCheckpoints(sortedPages)
    : [];

  const cases = [];
  for (const range of ranges) {
    const firstPage = sortedPages.find(p => p.page_index === range.start_page_index);
    // The page after the cite page — used as a fallback source for the
    // date line when the case's parallel cite + caption sit at the bottom
    // of the cite page and the `Decided <date>` (or court-attribution
    // line) is the first content on the following page. This is common
    // for short NY3d / Misc 3d per-curiam memos.
    const nextAfterCite = (range.cite_page_index != null)
      ? sortedPages.find(p => p.page_index === range.cite_page_index + 1)
      : null;
    let header = firstPage
      ? extractCaseHeader(firstPage, range.parallel_cites, nextAfterCite)
      : { warnings: ['first page record not found'] };
    // Fallback: if start_page didn't produce a date AND cite_page differs,
    // retry on cite_page. The boundary walker can over-extend start_page_index
    // backward across blank pages or section banners (e.g., "APPELLATE TERM
    // ABSTRACTS" index pages) — but the actual case template (parallel cite +
    // caption + date) always lives on cite_page_index.
    if (!header.decision_date
        && range.cite_page_index != null
        && range.cite_page_index !== range.start_page_index) {
      const citePage = sortedPages.find(p => p.page_index === range.cite_page_index);
      if (citePage) {
        const citeHeader = extractCaseHeader(citePage, range.parallel_cites, nextAfterCite);
        if (citeHeader.decision_date) {
          header = citeHeader;
        }
      }
    }
    const headName = extractRunningHeadName(sortedPages, range, reporter);
    // Footnotes are extracted at case level (each tagged with page_index +
    // volume_page), then passed into `extractOpinions` so the per-opinion
    // segmenter can attribute markers to the right footnote text via
    // `(marker, page_index)` matching after sentinel-based offset tracking.
    const caseFootnotes = extractCaseFootnotes(sortedPages, range);
    const opinions = extractOpinions(sortedPages, range, caseFootnotes);
    // Keep `case.footnotes` as the case-level catalog (useful for cases
    // where attribution is uncertain or for citation-graph queries that
    // span all opinions). Per-opinion footnotes (with body_offset) live
    // on each opinion record under `opinion.footnotes`.
    const footnotes = caseFootnotes;

    if (!opinions.length) {
      warnings.push(`[${range.citation || range.start_page_index}] no opinions extracted`);
    }

    // Fuzzy-match context for ToC name disambiguation. Priority:
    //   1. case_header.caption_text (opinion-section cases)
    //   2. range.caption_raw (memos: text between memo-number prefix and `[cite]`)
    //   3. opinion text head (last-resort fallback)
    const ctx = header.caption_text
      || range.caption_raw
      || (opinions[0]?.text ? opinions[0].text.slice(0, 500) : '');

    // Consolidated-appeal handling: an opinion can dispose of multiple
    // separately-filed actions in a single decision (Connolly v. LIPA +
    // Baumann v. LIPA + Heeran v. LIPA at 30 N.Y.3d 719). Each action
    // keeps its full caption; together they form one judicial opinion.
    // Split the caption block into individual captions, then resolve each
    // to its ToC short name. The FIRST caption is the lead (display) one.
    // Single-caption cases (the common case) come back as a one-element
    // array. `range.caption_raw` is used for memos where the case header
    // didn't yield a structured caption_text.
    const captionSource = header.caption_text || range.caption_raw || '';
    const captionTexts = splitCaptions(captionSource);
    const captions = captionTexts.map((capText, i) => {
      // For each split caption, run the ToC fuzzy-matcher with that
      // caption alone as context — this avoids the multi-caption
      // confusion where the picker swung to the surname mentioned most
      // distinctively in the merged context.
      const shortName = range.volume_page
        ? pickTocName(tocMap, range.volume_page, capText)
        : null;
      return {
        caption_index: i,
        name: capText,
        name_abbreviation: shortName,
        docket_number: null,
      };
    });
    // Backward-compat: top-level `tocName` falls back to first caption's
    // resolved short name. Older code paths that read `case.name`
    // continue to work.
    const tocName = captions[0]?.name_abbreviation
      || (range.volume_page ? pickTocName(tocMap, range.volume_page, ctx) : null);

    // Direct-provenance URL: PDF + `#page=<N>` (1-based) so the link opens
    // the PDF on nycourts.gov at the case's first page. Falls back to the
    // bare PDF URL if start_page_index isn't recorded.
    const sourceUrl = pdfUrlBase
      ? (range.start_page_index != null
          ? `${pdfUrlBase}#page=${range.start_page_index + 1}`
          : pdfUrlBase)
      : null;

    // Department resolution. For opinion-section cases, the per-case
    // attribution line (parsed by extractCaseHeader) carries it. For AD3d
    // memos, individual memos don't carry department info — they inherit
    // from the section banner that precedes their block in the volume.
    let courtDepartment = header.court_department ?? null;
    if (courtDepartment == null && range.section === 'memoranda' && departmentBanners.length) {
      courtDepartment = resolveDepartmentForPage(departmentBanners, range.start_page_index);
    }

    // Date resolution. extractCaseHeader catches the date for the FIRST
    // memo on each stacked page; subsequent memos miss it. AD3d memos
    // organize by date-cluster — inherit from the most-recent (Month Day,
    // Year) checkpoint that precedes the memo's cite position.
    let decisionDate = header.decision_date ?? null;
    if (decisionDate == null && range.section === 'memoranda' && dateCheckpoints.length) {
      decisionDate = resolveDateForMemo(dateCheckpoints, sortedPages, range);
    }

    // Per-case warning aggregation, prefixed with citation for traceability.
    // For AD3d memos, suppress the expected stacked-memo noise: extractCaseHeader
    // operates page-globally and only the first memo per page parses the date
    // / caption. The remaining memos' data is recovered via fallback paths
    // (date checkpoints, banner inheritance, caption_raw from boundary walker),
    // so these specific warnings would be misleading. Re-surface them only
    // when the fallback paths also failed.
    const suppressedNoise = new Set([
      'date line not found (neither Argued/Submitted–decided nor Decided)',
      'caption block could not be extracted',
      'parallel cite header not found in line stream',
    ]);
    const isStackedMemo = reporter === 'AD3d' && range.section === 'memoranda';
    for (const w of header.warnings || []) {
      if (isStackedMemo && suppressedNoise.has(w)) {
        if (w.startsWith('date line') && decisionDate != null) continue;
        if (w === 'caption block could not be extracted' && (range.caption_raw || '').length > 0) continue;
        if (w === 'parallel cite header not found in line stream' && (range.parallel_cites || []).length > 0) continue;
      }
      warnings.push(`[${range.citation || range.start_page_index}] ${w}`);
    }

    cases.push({
      name:           tocName || headName || header.caption_text || null,
      toc_name:       tocName,
      caption_text:   header.caption_text,
      running_head_name: headName,
      // All captions (length >= 1). For single-caption cases this mirrors
      // top-level `name` / `name_abbreviation`; for consolidated appeals
      // it carries every individual action's caption + short name.
      captions,
      decision_date:  decisionDate,
      argued_date:    header.argued_date,
      argued_or_submitted: header.argued_or_submitted,
      court_department: courtDepartment,
      docket_number:  null,                   // not present in NY3d front matter
      first_page:     range.volume_page,      // start page within bound volume
      last_page:      null,                   // can be derived later if needed
      citation:       range.citation,
      parallel_cites: range.parallel_cites,
      court_name:     courtName,
      source_url:     sourceUrl,
      opinions,
      footnotes,
      provenance: {
        section:        range.section,
        start_page_index: range.start_page_index,
        end_page_index:   range.end_page_index,
      },
    });
  }

  // Motion-calendar entries — line-based denials/grants/dismissals that the
  // parallel-cite walker doesn't recognize. Two source layouts:
  //   1. The dedicated NY3d "Motions for Leave to Appeal" section (skipped
  //      entirely by detectCaseBoundaries).
  //   2. NY3d motion entries interleaved within the memoranda section.
  // De-dupe (2) against the cases we already emitted by base-CURIE so a
  // substantive memo's (caption, page) pair isn't double-counted.
  const motionsCases = walkMotionsSection(sortedPages, classification, volumeMeta);
  const existingBases = new Set();
  for (const c of cases) {
    const base = caseCurieBase(volumeMeta, c.first_page, c.name);
    if (base) existingBases.add(base);
  }
  const memoMotionCases = walkMemoMotionEntries(sortedPages, classification, volumeMeta, existingBases);
  cases.push(...motionsCases, ...memoMotionCases);

  // Assign CURIEs in-place. Each case gets `case_curie`; each opinion gets
  // `curie`. Collision-disambiguation suffix `:NN` is appended when two
  // cases share (volume, page, name-slug). Done here (not in writeSql) so
  // the JSON output also carries CURIEs for downstream consumers.
  assignCuries(cases, volumeMeta);

  // Re-derive court_department from the opinion text itself (recited county +
  // panel justices). This supersedes banner inheritance, which silently
  // mislabels whole page-runs when banner detection misfires; banner/header
  // attribution is kept only as the fallback when the text is silent. AD3d
  // only — NY3d / Misc 3d have no departments. (Post-pass so the per-volume
  // justice roster can be bootstrapped from every case in the volume.)
  warnings.push(...resolveVolumeDepartments(cases, { reporter }));

  return { cases, warnings };
}
