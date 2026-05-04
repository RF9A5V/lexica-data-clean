/**
 * Opinion-type splitter.
 *
 * Within a case range, every page carries a small subtitle at top≈127.5pt
 * (size ≈8pt) that identifies what the page is *about*:
 *
 *   StatementofCase            ← case-header pages (caption/summary)
 *   Headnote / Headnotes       ← headnote block pages
 *   PointsofCounsel            ← counsel-arguments pages
 *   OpinionPerCuriam           ← per curiam opinion body
 *   Opinionby<NAME>,J.         ← signed majority/lead opinion
 *   Concurringopinionby<NAME>,J. ← concurrence
 *   Dissentingopinionby<NAME>,J. ← dissent
 *
 * The subtitle is a redundant signal placed on every page of an opinion;
 * grouping consecutive pages by (type, author) gives us discrete opinion
 * segments. This skips header/headnote/counsel pages entirely so the
 * resulting opinion bodies don't contain editorial frontmatter.
 *
 * Mid-page transitions exist (a concurrence can begin on the bottom of a
 * page that the typesetter still labelled with the prior opinion's
 * subtitle), but they're rare and refining them needs the in-body marker
 * `<NAME>, J. (concurring|dissenting).`. v1 uses page-level segmentation
 * only; the in-body marker is captured as a warning when found mid-segment.
 *
 * Returns:
 *   [{
 *     opinion_index: number,        // 0-based, in document order
 *     opinion_type:  'per_curiam' | 'majority' | 'concurring' | 'dissenting' | 'memorandum',
 *     author:        string | null, // judge surname; null for per_curiam/memorandum
 *     start_page_index: number,
 *     end_page_index:   number,
 *     text:          string,        // joined body text (running head + subtitle stripped)
 *   }, ...]
 */

// The small subtitle sits in a narrow band: the cap-sized baseline at
// top≈127.5 (sz≈8) and the small-caps body baseline at top≈130 (sz≈5).
// We collect words from both baselines and join by x-position without
// inserting spaces — the typesetter packs subtitle glyphs so tightly that
// inter-token spacing is unreliable. The downstream regex is whitespace-
// agnostic anyway.
const SUBTITLE_TOP_MIN  = 124;
const SUBTITLE_TOP_MAX  = 135;
const SUBTITLE_SIZE_MAX = 10;

// Classification patterns. The subtitle is joined as one squashed string
// like "OpinionbyFAHEY,J." or "ConcurringopinionbyRIVERA,J." — patterns
// match the prefix and capture the author surname.
//
// Honorific prefix (ChiefJudge / Judge) is optional. The trailing `,J.` is
// also optional: ChiefJudge subtitles omit it ("OpinionbyChiefJudgeDIFIORE").
// The author is captured as one or more all-caps tokens, optionally
// joined by hyphens for compound surnames.
const NAME_GROUP = /([A-Z][A-Z']+(?:[A-Z][A-Z']+)*(?:-[A-Z][A-Z']+)*)/.source;
const SUFFIX = /(?:,J\.?)?\.?/.source;
const HONORIFIC = /(?:ChiefJudge|Judge)?/.source;

const PATTERNS = [
  { re: /^OpinionPerCuriam\.?$/i,                              type: 'per_curiam' },
  { re: /^StatementofCase\.?$/i,                               type: 'header' },
  { re: /^Points?ofCounsel\.?$/i,                              type: 'header' },
  { re: /^Headnotes?\.?$/i,                                    type: 'header' },
  { re: new RegExp(`^Concurringopinionby${HONORIFIC}${NAME_GROUP}${SUFFIX}$`), type: 'concurring' },
  { re: new RegExp(`^Dissentingopinionby${HONORIFIC}${NAME_GROUP}${SUFFIX}$`), type: 'dissenting' },
  { re: new RegExp(`^Opinionby${HONORIFIC}${NAME_GROUP}${SUFFIX}$`),           type: 'majority' },
];

/**
 * Pull the small subtitle from a page, or null if no subtitle is present.
 * Returns a single string with all subtitle-band words concatenated in
 * x-order without intervening spaces — the typesetter renders subtitles
 * tightly and pdfplumber's word boundaries don't align with semantic
 * tokens here. We deliberately do NOT use the small-caps recombiner
 * because subtitle font sizes (5–8pt) fall below its MIN_CAP_SIZE.
 */
function readSubtitle(page) {
  const words = (page.words || []).filter(w =>
    w.top >= SUBTITLE_TOP_MIN &&
    w.top <= SUBTITLE_TOP_MAX &&
    w.size <  SUBTITLE_SIZE_MAX
  );
  if (!words.length) return null;
  const sorted = words.slice().sort((a, b) => a.x0 - b.x0);
  return sorted.map(w => w.text).join('').trim();
}

/**
 * Title-case an all-caps surname like "FAHEY" → "Fahey". Multi-word names
 * (rare in NY3d but possible for hyphenated surnames) get each part
 * title-cased independently.
 */
function cleanAuthor(raw) {
  if (!raw) return null;
  return raw.split(/[-\s]+/).map(t => /^[A-Z']+$/.test(t)
    ? t[0] + t.slice(1).toLowerCase()
    : t
  ).join(' ');
}

function classifySubtitle(subtitle) {
  if (!subtitle) return { kind: 'unknown', author: null };
  for (const { re, type } of PATTERNS) {
    const m = subtitle.match(re);
    if (m) {
      return {
        kind: type,
        author: type === 'majority' || type === 'concurring' || type === 'dissenting'
          ? cleanAuthor(m[1])
          : null,
      };
    }
  }
  return { kind: 'unknown', author: null };
}

/**
 * Page body text: lines below the running-head + subtitle band. We use
 * text_raw with the first 1-2 non-empty lines stripped, since text_raw is
 * cheaper than rebuilding lines from words. Running heads are always the
 * first non-empty line; subtitles when present are the second.
 */
function pageBody(page, hasSubtitle) {
  const text = page.text_raw || '';
  const lines = text.split('\n');
  let stripped = 0;
  let i = 0;
  const dropCount = hasSubtitle ? 2 : 1;
  while (i < lines.length && stripped < dropCount) {
    if (lines[i].trim()) stripped++;
    i++;
  }
  return lines.slice(i).join('\n').trim();
}

// Detect the first line of a running head. Used when slicing across pages
// so that page-break running heads don't contaminate body text. Matches
// the patterns from case_boundaries.js but standalone here (sub.line check).
const RUNNING_HEAD_PATTERNS = [
  /^\d+\s+\d+\s*(?:NEW\s*YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS/i,
  /^MEMORANDA(?:[,\s].*?)?\d+\s*$/,
  /^MEMORANDA$/,
  /^OTHERABSTRACTS\s+\d+\s*$/i,
  /^.+\[\s*\d+\s*(?:NY3d|AD3d|Misc\s*3d)\s*\d+\s*\]\s*\d+\s*$/i,
];

function isRunningHeadLine(line) {
  const t = line.trim();
  if (!t) return false;
  return RUNNING_HEAD_PATTERNS.some(re => re.test(t));
}

// NY3d per-page subtitle line. Stripped along with the running head when
// concatenating pages for marker-based body extraction. Tolerant of either
// the old squashed form ("ConcurringopinionbyRIVERA,J.") or the spaced form
// ("Concurring opinion by RIVERA, J.") since pdfplumber's x_tolerance
// setting affects whether intra-subtitle gaps become spaces.
const SUBTITLE_LINE_RE = /^(?:Opinion\s*(?:by|Per\s*Curiam)|(?:Concurring|Dissenting)\s*opinion\s*by|Statement\s*of\s*Case|Headnotes?|Points?\s*of\s*Counsel)\b/i;

function isSubtitleLine(line) {
  return SUBTITLE_LINE_RE.test(line.trim());
}

// Strip the leading running-head + subtitle band from a page. Up to four
// candidate lines are inspected so we cover empty-line padding too.
function stripPageHeader(text) {
  const lines = text.split('\n');
  let drop = 0;
  while (drop < lines.length && drop < 4) {
    const t = lines[drop].trim();
    if (!t) { drop++; continue; }
    if (isRunningHeadLine(t) || isSubtitleLine(t)) { drop++; continue; }
    break;
  }
  return lines.slice(drop).join('\n');
}

/**
 * Heuristic body-start detector for AD3d/Misc 3d memos.
 *
 * After `]—` on a memo's first line, content can take two forms:
 *  (a) Body starts immediately on the same line: `]—An appeal having been…`
 *  (b) A topic block intervenes:
 *        `]—`
 *        `MotionsandOrders—ReargumentorRenewal—LackofReasonable…`
 *        `tion for Failure to Present New Facts on Prior Motion…`     ← wraps from prev with `-`
 *        `PriorDetermination`
 *        `Order, Supreme Court, New York County (Anil C. Singh, J.),`  ← body starts here
 *
 * Topic-block lines have an em/en dash or a CamelCase boundary
 * `[a-z][A-Z]`. A body line lacks both. Wrap continuations (where the
 * previous topic line ended with a hyphen) are treated as topic too, so
 * `tion for Failure…` is correctly skipped despite looking like prose.
 *
 * Returns the offset (in `text`) of the first body line. Falls back to the
 * given start offset if no body line is found.
 */
function findMemoBodyStart(text, fromOffset) {
  let pos = fromOffset;
  while (pos < text.length && /\s/.test(text[pos])) pos++;
  let cur = pos;
  let prevWasTopic = false;
  let prevEndedWithHyphen = false;
  while (cur < text.length) {
    const lineEnd = text.indexOf('\n', cur);
    const lineStop = lineEnd === -1 ? text.length : lineEnd;
    const line = text.slice(cur, lineStop).trim();
    if (line) {
      const topic = isTopicLine(line);
      const wrap = isTopicWrapContinuation(line, prevWasTopic, prevEndedWithHyphen);
      if (!topic && !wrap) return cur;
      prevWasTopic = true;
      prevEndedWithHyphen = line.endsWith('-');
    }
    cur = lineStop + 1;
  }
  return pos;
}

function isTopicLine(line) {
  if (line.includes('—') || line.includes('–')) return true;
  if (/[a-z][A-Z]/.test(line)) return true;
  return false;
}

// Detect a topic continuation: a line that's not itself topic-shaped but
// follows one (because it wraps from the prior topic line). Two cases:
//   - prior line ended with hyphen (`Cross-` → `Examine…`)
//   - current line is a single word (`Adequate Notice of` → `Charges`)
// Both only fire after a topic line; otherwise normal body lines that
// happen to be short ("An appeal" right after `]—`) are not skipped.
function isTopicWrapContinuation(line, prevWasTopic, prevEndedWithHyphen) {
  if (!prevWasTopic) return false;
  if (prevEndedWithHyphen) return true;
  if (!/\s/.test(line.trim())) return true;  // single-word continuation
  return false;
}

/**
 * Multi-opinion split for AD3d/Misc 3d.
 *
 * AD3d memoranda and opinion-section cases occasionally have a concurrence
 * or dissent embedded in the body after the majority's `Concur—…JJ.` line.
 * The DB stores those as separate opinion records. We detect the embedded
 * opinion-start markers and split.
 *
 * Three marker forms are observed:
 *
 *   Parenthetical:
 *     `Devine, J. (concurring in part and dissenting in part). We agree…`
 *
 *   Short form:
 *     `Pritzker, J., concurs. Adjudged that…`
 *     `Smith, J., dissents.`
 *
 *   Multi-judge dissent (AD3d memo style):
 *     `Renwick, J.P., and Manzanet-Daniels, J., dissent in part in a
 *      memorandum by Manzanet-Daniels, J., as follows: Plaintiffs'…`
 *
 * To reduce false positives (citations to other cases that mention
 * "(concurring)" or "(dissenting)"), markers are only accepted if the
 * preceding text contains a closing signal of a prior opinion: either
 * `, JJ?\.` (end of "Concur—…JJ.") or `concurs?\.` (end of solo concur).
 */
const MARKER_PARENTHETICAL = /([A-Z][a-zA-Z'-]+),\s+J\.\s*\((concurring|dissenting)([^)]*)\)\.?\s+/g;
// Short form: `Pritzker, J., concurs.` or `Barros, J., dissents,`. Requires
// a `.` or `,` immediately after to avoid matching body sentences like
// `Smith, J., concurs in the result with ABC, J.` (where `concurs` is a verb).
const MARKER_SHORT         = /([A-Z][a-zA-Z'-]+),\s+J\.,\s+(concurs|dissents)(?:\s+in\s+(?:part|a))?(?=[,.])/g;
// Multi-judge form. Two observed sub-forms:
//   "Renwick, J.P., and Manzanet-Daniels, J., dissent in part…" (each judge
//     has individual title; comma before `and`)
//   "Andrias and Singh, JJ., dissent in part…" (no comma before `and`,
//     plural `JJ.` after second name)
// The comma before `and` is optional, and `JJ.` is accepted as well as `J.`.
const MARKER_MULTI         = /([A-Z][a-zA-Z'-]+)(?:,\s+J\.P\.)?,?\s+and\s+([A-Z][a-zA-Z'-]+),\s+JJ?\.,?\s+(dissent|concur)s?\s+in\s+(part|a)\b[^.]*?(?:as\s+follows:|\.)/g;

function classifyParenType(verb, modifier) {
  const m = (modifier || '').toLowerCase();
  // Either verb itself or modifier text mentions concurring/dissenting.
  const hasConcurring = verb === 'concurring' || m.includes('concurring');
  const hasDissenting = verb === 'dissenting' || m.includes('dissenting');
  if (hasConcurring && hasDissenting) {
    return 'concurring-in-part-and-dissenting-in-part';
  }
  return verb === 'concurring' ? 'concurrence' : 'dissent';
}

function findOpinionMarkers(body) {
  const markers = [];
  // Parenthetical form
  MARKER_PARENTHETICAL.lastIndex = 0;
  let m;
  while ((m = MARKER_PARENTHETICAL.exec(body)) !== null) {
    markers.push({
      offset: m.index,
      author: `${m[1]}, J.`,
      type:   classifyParenType(m[2], m[3]),
    });
  }
  // Short form ("Pritzker, J., concurs.")
  MARKER_SHORT.lastIndex = 0;
  while ((m = MARKER_SHORT.exec(body)) !== null) {
    markers.push({
      offset: m.index,
      author: `${m[1]}, J.,`,
      type:   m[2] === 'dissents' ? 'dissent' : 'concurrence',
    });
  }
  // Multi-judge form
  MARKER_MULTI.lastIndex = 0;
  while ((m = MARKER_MULTI.exec(body)) !== null) {
    markers.push({
      offset: m.index,
      author: `${m[1]}, J.P., and ${m[2]}, J.,`,
      type:   m[3] === 'dissent' ? 'dissent' : 'concurrence',
    });
  }
  // Sort and de-dupe overlapping markers (e.g. parenthetical + short matching same span).
  markers.sort((a, b) => a.offset - b.offset);
  const dedup = [];
  for (const mk of markers) {
    if (dedup.length && mk.offset - dedup[dedup.length - 1].offset < 40) continue;
    dedup.push(mk);
  }
  return dedup;
}

/**
 * Validate that an opinion marker is preceded somewhere in the body by an
 * "end of prior opinion" signal — either a `Concur—…JJ.` line or a solo
 * `<NAME>, J., concurs.` line. This filters out false positives from
 * citations or references that happen to use "(concurring)" or "(dissenting)".
 */
function hasPriorOpinionEnd(body, offset) {
  const before = body.slice(0, offset);
  // Look for the last "JJ." or "concurs." in the preceding text.
  return /\b(JJ?\.|concurs?\.)\s*$/i.test(before.replace(/\s+$/, '') + ' ')
    || /\bConcur—[^—]+\bJJ?\./i.test(before)
    || /\b[A-Z][a-zA-Z'-]+,\s+J\.,\s+concurs?\./i.test(before);
}

/**
 * Split a body text into multiple opinions if embedded concurrence/dissent
 * markers are found. Returns an array of `{type, author, text}` segments,
 * or null if no split is appropriate.
 */
function splitMultiOpinion(body, firstType, firstAuthor) {
  if (!body || body.length < 200) return null;
  const markers = findOpinionMarkers(body).filter(m =>
    m.offset > 100 && hasPriorOpinionEnd(body, m.offset)
  );
  if (!markers.length) return null;

  // Multi-opinion AD3d memos sometimes have an explicit majority byline at
  // the start of the body (e.g., `Lynch, J. Proceeding pursuant to…`). When
  // present and we don't already have a firstAuthor, extract it.
  let resolvedAuthor = firstAuthor;
  if (!resolvedAuthor) {
    const bylineMatch = body.match(/^([A-Z][a-zA-Z'-]+),\s+J\.(?:P\.)?\s+(?=[A-Z])/);
    if (bylineMatch) {
      // Capture as "Name, J." or "Name, J.P." matching the DB form.
      resolvedAuthor = bylineMatch[0].replace(/\s+$/, '');
    }
  }

  const segments = [];
  segments.push({
    type: firstType,
    author: resolvedAuthor,
    text: body.slice(0, markers[0].offset).trim(),
  });
  for (let i = 0; i < markers.length; i++) {
    const start = markers[i].offset;
    const end = i + 1 < markers.length ? markers[i + 1].offset : body.length;
    segments.push({
      type: markers[i].type,
      author: markers[i].author,
      text: body.slice(start, end).trim(),
    });
  }
  return segments;
}

/**
 * Trim the leading caption of the NEXT memo from a memo's text slice.
 *
 * The boundary walker slices each memo's text up to the offset of the
 * NEXT memo's `[` (parallel cite header). Between the current memo's
 * `Concur—…JJ.` and the next memo's `[`, the next memo's number prefix
 * and caption sit. We strip that tail by looking for the next-memo
 * number-prefix pattern at the start of a line.
 *
 * Pattern requirements (all must match):
 *   - 1–3 digit memo number (filters out body cites like "1515 Macombs"
 *     which has 4 digits)
 *   - Followed by EITHER:
 *       (a) small-caps lead pair `[A-Z]\s+[A-Z]` — matches "T P", "J K"
 *           in small-caps captions like `14 A P ,Appellant, v C…`
 *       (b) a known caption-opener word (In, The, Matter, etc.) — matches
 *           regular form like `13 In the Matter of…`
 *   - Word boundary after the opener
 *
 * Body false positives like `\n128 AD3d 441` are rejected: `AD3d` has no
 * lowercase between A and D, no space, isn't a known opener.
 */
const CAPTION_OPENERS = [
  'In', 'The', 'An', 'A', 'Matter', 'People', 'Application',
  'Petition', 'Appeal', 'Order', 'On', 'Estate', 'Claim',
];
const NEXT_MEMO_RE = new RegExp(
  '\\n\\s*\\d{1,3}\\s+(?:' +
  CAPTION_OPENERS.join('|') +
  '|[A-Z](?:\\s+[A-Z]))\\b'
);

function trimNextMemoCaption(text) {
  const m = text.match(NEXT_MEMO_RE);
  if (!m) return text;
  return text.slice(0, m.index).trimEnd();
}

/**
 * AD3d / Misc 3d opinion-section extractor.
 *
 * Opinion-section first pages use the same template as NY3d (parallel cite
 * → caption → court attribution + date → SUMMARY → HEADNOTES →
 * APPEARANCES OF COUNSEL → OPINION OF THE COURT → byline → body). We use
 * the `OPINIONOFTHECOURT` marker (squashed by typesetter) as the body
 * anchor. Bylines take three forms:
 *
 *   Signed (AD3d majority opinion):
 *     OPINIONOFTHECOURT
 *     R , J.            ← lead caps + ", J."
 *     ENWICK            ← small-caps body of "ENWICK" → "Renwick"
 *     <body>
 *
 *   Per Curiam (AD3d disciplinary):
 *     OPINIONOFTHECOURT
 *     Per Curiam.
 *     <body>
 *
 *   Memorandum (Misc 3d, also some AD3d):
 *     OPINIONOFTHECOURT
 *     M .               ← lead cap + period
 *     EMORANDUM         ← small-caps body
 *     <body>
 *
 * The body extends from after the byline through the end of the case range
 * (running heads stripped on continuation pages, line wraps normalized).
 * Per the DB, AD3d opinion-section cases have one opinion each (no inline
 * concurrences/dissents at this level), so we don't split mid-body.
 */
// All-caps banner — case-sensitive to avoid matching body-text references
// like "opinion of the Court of Appeals" that appear in the SUMMARY before
// the actual marker. The typesetter sometimes squashes inter-word spacing
// so we still allow `\s*` between tokens. AD3d uses "DECISION OF THE COURT"
// in place of "OPINION" for short procedural rulings (sanctions, withdrawn
// appeals) — same byline / body layout follows, so they share a marker.
const OPINION_MARKER_RE = /(?:OPINION|DECISION)\s*OF\s*THE\s*COURT/;

function parseAd3dByline(line1, line2) {
  const l1 = (line1 || '').trim();
  const l2 = (line2 || '').trim();
  // Per Curiam form
  if (/^Per\s+Curiam\.?$/i.test(l1)) {
    return { author: 'Per Curiam.', kind: 'per_curiam', linesUsed: 1 };
  }
  // Memorandum form: "M ." + "EMORANDUM"
  if (/^M\s*\.\s*$/.test(l1) && /^EMORANDUM\.?$/i.test(l2)) {
    return { author: null, kind: 'memorandum', linesUsed: 2 };
  }
  // Memorandum form (single line, recombined): "Memorandum." — observed
  // in some short Misc 3d procedural memos where the typesetter recombined
  // the lead+small-caps into one mixed-case line.
  if (/^Memorandum\.?$/i.test(l1)) {
    return { author: null, kind: 'memorandum', linesUsed: 1 };
  }
  // Signed opinion: "<caps with separators>, <Title>." + "<all-caps body>"
  // Lead-token forms observed:
  //   "R"           → single-cap (Renwick)
  //   "M-D"         → hyphenated double-cap (Manzanet-Daniels)
  //   "C E. R"      → multi-initial (Charles E. Ramos — middle initial keeps period)
  // Body line carries small-caps remnants for each main-cap token in order
  // (middle initials have no body fragment).
  // Title forms:
  //   "J."     — Justice (most common)
  //   "P.J."   — Presiding Justice (Appellate Division department head; also
  //              accepts "P. J." with optional internal space)
  //   "J.P."   — Justice Presiding (rare alt form, observed in Misc 3d)
  //   "S."     — Surrogate
  //   "Surr."  — Surrogate (alt)
  //   "JJ."    — multiple Justices (concur-block form, rarely appears as lead)
  // The comma between surname and title is usually present but some volumes
  // typeset it without (`H , J.`  vs.  `H J.` / `A I. B J.`), so allow it
  // optionally. The lazy `.+?` plus the strict end anchor keep this from
  // over-matching since the title alternatives are short and specific.
  // Trailing tolerance: optional period after the title, and either an
  // extra trailing comma (`H , J.,`) or a superscript footnote marker
  // (`T M , J. 1`) — both observed in newer volumes.
  const m = l1.match(/^(.+?)\s*,?\s*(P\.\s*J|J\.\s*P|J|S|Surr|JJ)\.?\s*(?:,|\d+)?\s*$/);
  if (!m || !/^[A-Z]/.test(l2)) return null;
  const leadStr = m[1];
  const title = m[2];

  // Tokenize lead, distinguishing main caps (need body), middle initials
  // (have a period), and hyphen separators.
  const tokens = [];
  let cur = '';
  for (let i = 0; i < leadStr.length; i++) {
    const ch = leadStr[i];
    if (/[A-Z]/.test(ch)) {
      cur += ch;
    } else if (ch === '.') {
      if (cur) tokens.push({ text: cur, type: 'init' });
      cur = '';
    } else if (ch === '-') {
      if (cur) { tokens.push({ text: cur, type: 'cap' }); cur = ''; }
      tokens.push({ type: 'hyphen' });
    } else if (/\s/.test(ch)) {
      if (cur) { tokens.push({ text: cur, type: 'cap' }); cur = ''; }
      // discard whitespace separators (will be normalized)
    }
  }
  if (cur) tokens.push({ text: cur, type: 'cap' });

  const bodyCaps = l2.split(/[\s-]+/).filter(Boolean);
  const mainCount = tokens.filter(t => t.type === 'cap').length;
  if (mainCount !== bodyCaps.length || mainCount === 0) {
    return { author: l1, kind: 'majority', linesUsed: 1 };
  }

  let bodyIdx = 0;
  const parts = [];
  for (const t of tokens) {
    if (t.type === 'cap')        parts.push(t.text + bodyCaps[bodyIdx++].toLowerCase());
    else if (t.type === 'init')  parts.push(t.text + '.');
    else if (t.type === 'hyphen')parts.push('-');
  }

  // Join: hyphens stick to neighbors, otherwise single space.
  let result = '';
  for (const p of parts) {
    if (p === '-')                      result += '-';
    else if (!result || result.endsWith('-')) result += p;
    else                                result += ' ' + p;
  }
  return { author: result + ', ' + title + '.', kind: 'majority', linesUsed: 2 };
}

// Vacatur placeholder pattern. The reporter brackets the notice in `[...]`
// and the text spans multiple visual lines; we accept any whitespace inside.
// Sentinel chars (page-break / footnote-marker) are stripped before matching
// so a sentinel landing inside the brackets doesn't break the regex.
const VACATUR_NOTICE_RE =
  /\[\s*The\s+opinion\s+appearing\s+at[\s\S]*?has\s+been\s+vacated[\s\S]*?\]/i;

function matchVacaturNotice(combined) {
  const stripped = combined.replace(/[\u{E001}\u{E002}\u{E003}\u{E004}]/gu, '');
  const m = stripped.match(VACATUR_NOTICE_RE);
  if (!m) return null;
  // Collapse internal whitespace to single spaces; strip surrounding brackets.
  const inner = m[0].replace(/^\[\s*|\s*\]$/g, '').replace(/\s+/g, ' ').trim();
  return { text: inner };
}

/**
 * Walk pages in the case range; concatenate text_raw with running heads
 * stripped on continuation pages; find the OPINION OF THE COURT marker;
 * parse byline from the next 1-2 lines; body is everything after.
 */
function extractOpinionSectionText(pages, range) {
  const startP = range.start_page_index;
  const endP   = range.end_page_index;

  // Build sentinel-bearing text from page.lines (not page.text_raw) so
  // in-body footnote markers and page-break records survive into the body.
  // buildNy3dPageText is reporter-agnostic in practice — both NY3d and
  // AD3d/Misc 3d use 10.98pt body via the same PDFlib output and share the
  // header band geometry.
  const combined = combineNy3dPages(pages, range);

  const markerMatch = combined.match(OPINION_MARKER_RE);
  if (!markerMatch) {
    // Fallback: vacated-opinion notice. The reporter publishes a placeholder
    // in place of the opinion when the issuing court has subsequently
    // vacated it, e.g.:
    //   `[The opinion appearing at 199 AD3d 16 (2021 NY Slip Op 04801)
    //    has been vacated by order of the Appellate Division, Fourth
    //    Department, see 2022 NY Slip Op 00560.]`
    // The bracketed notice IS the body (there is no opinion). Surface it as
    // a synthetic opinion with kind 'vacatur_notice' so downstream UI / search
    // can flag the case appropriately rather than treat it as missing.
    const vacatur = matchVacaturNotice(combined);
    if (vacatur) {
      const processed = processBodyText(vacatur.text);
      return {
        opinion_index: 0,
        opinion_type: 'vacatur_notice',
        author: null,
        start_page_index: range.start_page_index,
        end_page_index: range.end_page_index,
        text: processed.text,
        footnote_markers: processed.footnote_markers,
        page_breaks: processed.page_breaks,
      };
    }
    return null;
  }
  let pos = markerMatch.index + markerMatch[0].length;

  // Skip whitespace then read up to next 2 non-empty lines for the byline.
  while (pos < combined.length && /\s/.test(combined[pos])) pos++;
  const lines = combined.slice(pos).split('\n');
  const nonEmpty = [];
  for (const line of lines) {
    nonEmpty.push(line);
    if (nonEmpty.filter(l => l.trim()).length >= 2) break;
  }
  // Find non-empty line indices in `nonEmpty`. Strip sentinels for the
  // byline match — parseAd3dByline expects clean text.
  const ne = nonEmpty.map((l, i) => l.trim() ? i : -1).filter(i => i !== -1);
  if (ne.length < 1) return null;
  const stripSentinels = (s) => s.replace(/[\u{E001}\u{E002}\u{E003}\u{E004}]/gu, '').replace(/\s+/g, ' ').trim();
  const line1 = stripSentinels(nonEmpty[ne[0]]);
  const line2 = ne[1] !== undefined ? stripSentinels(nonEmpty[ne[1]]) : '';

  const byline = parseAd3dByline(line1, line2);
  if (!byline) return null;

  // Body starts after the consumed lines.
  const linesToConsume = ne[byline.linesUsed - 1] + 1;  // index of last consumed + 1
  let bodyStart = pos;
  let count = 0;
  while (bodyStart < combined.length && count < linesToConsume) {
    if (combined[bodyStart] === '\n') count++;
    bodyStart++;
  }
  const bodyRaw = combined.slice(bodyStart).trim();

  // Normalize line wraps + decode sentinels in one pass. processBodyText
  // gives the cleaned body text plus parallel { offset, marker } and
  // { offset, page_index, volume_page } records keyed to the cleaned text.
  const normalized = normalizeLineWraps(bodyRaw);
  const processed = processBodyText(normalized);

  return {
    opinion_index: 0,
    opinion_type: byline.kind,
    author: byline.author,
    start_page_index: startP,
    end_page_index: endP,
    text: processed.text,
    footnote_markers: processed.footnote_markers,
    page_breaks: processed.page_breaks,
  };
}

/**
 * NY3d body extraction.
 *
 * NY3d cases (both opinion-section and memoranda) use an `OPINIONOFTHECOURT`
 * banner as the body-start anchor. Everything before is editorial preamble
 * (caption / SUMMARY / HEADNOTE / counsel listings) and is discarded. The
 * body ends at the judge-tally / disposition block, which has three forms:
 *   "Concur: Chief Judge D F..."         (memo, Court of Appeals)
 *   "Judges R, S, F concur,"             (opinion-section, multi-judge tally)
 *   "Chief Judge D F concurs..."         (variant)
 * The block can span 2-4 lines because each judge's surname renders as a
 * lead-cap on one line and a small-caps body on the next.
 *
 * Within the body, the lead opinion may be followed by inline concurrences
 * or dissents, each prefaced by a byline like `Rivera, J. (concurring).` —
 * or in NY3d's small-caps form, `R , J. (concurring).` with the rest of the
 * surname (`IVERA`) appearing alone on a later line. We split on those
 * markers and reconstruct the author from the lead+body fragment, splicing
 * the orphan fragment out of the segment text.
 */
const NY3D_INBODY_MARKER_RE =
  /(^|[.?!\n]\s*)(?<lead>[A-Z][A-Z']*|[A-Z][a-zA-Z'-]+)\s*,\s+(?<title>J\.P?|JJ?|S|Surr)\.\s*\(\s*(?<verb>concurring|dissenting)(?<mod>[^)]*)\)\s*\.\s+/g;

const NY3D_CONCUR_END_PATTERNS = [
  /\n\s*Concur:\s/g,
  /\n\s*Judges?\s+[A-Z][\s\S]{0,500}?\bconcurs?\b/g,
  /\n\s*Chief\s+Judge\s+\S[\s\S]{0,500}?\b(?:concurs?|takes?\s+no\s+part)\b/g,
  // "Opinion by Judge X." line — announces the lead author of the case in
  // multi-opinion / consolidated cases. The Concur block follows directly,
  // sometimes on the same physical line (e.g. `Opinion by Judge S . Chief
  // Judge D F and Judges...`), so we anchor on this as the disposition-block
  // start instead.
  /\n\s*Opinion\s+by\s+(?:Chief\s+)?Judge\s+\S/g,
  // Consolidated-case disposition: `In <case>, <case>: Order affirmed/reversed/...`
  // — used in cases that decide multiple appeals together. The list of
  // case names can wrap across 2-3 lines so we allow `[\s\S]`.
  /\n\s*In\s+[A-Z][\s\S]{0,400}?:\s*Order\s+(?:affirmed|reversed|modified|dismissed)/g,
];

function findNy3dConcurEnd(text) {
  // The concur block sits at the end of the body; multiple `concur`/`concurs`
  // / `taking no part` mentions appear across its continuation lines, so we
  // take the EARLIEST overall match (the start of the block). Body text
  // doesn't reliably hit any of these line-anchored patterns mid-opinion.
  let earliest = -1;
  for (const re of NY3D_CONCUR_END_PATTERNS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text)) !== null) {
      if (earliest === -1 || m.index < earliest) earliest = m.index;
    }
  }
  return earliest;
}

// Concatenate all in-range pages with each page's running head + subtitle
// band stripped. Returns the combined raw text (NOT yet normalized).
//
// Builds the page text from `page.lines` (sorted by top) instead of
// `page.text_raw` so we can detect paragraph indents via x0 and insert a
// blank line before each paragraph start. After normalizeLineWraps that
// blank line collapses to a `\n`, which preserves paragraph breaks in the
// final body text — matching the CAP-imported convention.
//
// For memos, opts.endPage / opts.endOffset extend the slice past
// range.end_page_index when the body straddles a page (case_boundaries sets
// `end_page_index` to the cite-page of the case but the body can run into
// the page where the NEXT case's cite header sits — `text_end_page_index`
// is that page and `text_end_offset` is the cut point on it).
function combineNy3dPages(pages, range, opts = {}) {
  const startP = range.start_page_index;
  const endP   = opts.endPage ?? range.end_page_index;
  const endOff = opts.endOffset;  // optional truncation on endP
  const parts = [];
  for (const p of pages) {
    if (p.page_index < startP || p.page_index > endP) continue;
    parts.push(buildNy3dPageText(p, p.page_index === endP ? endOff : null));
  }
  return parts.join('\n');
}

// Body-text font size threshold for NY3d. Body is 10.98pt; footnotes are
// 8.98pt; headnote / subtitle bands are smaller still. Anything below 10
// is treated as non-body (footnote, header, or small-caps body fragment).
const NY3D_BODY_SIZE_MIN = 10;

// Top-band cutoff for the running-head + subtitle band. Body content
// starts at top≥140 on a typical NY3d page; the running head is at ~117
// and the subtitle (1-2 lines) at ~127-130. See stripPageHeader / the
// HEADER_BAND_BOTTOM constant in buildNy3dPageText.
const NY3D_HEADER_BAND_BOTTOM = 135;

// Compute the y-coordinate of the last body-size line on a page. Anything
// with size < NY3D_BODY_SIZE_MIN AND top > lastBodyTop is in the footnote
// band; small-caps body fragments interspersed with body text (sitting
// at top values within the body region) are kept.
function computeLastBodyTop(lines) {
  let lastBodyTop = -Infinity;
  for (const l of lines) {
    if (l.size >= NY3D_BODY_SIZE_MIN && l.top >= NY3D_HEADER_BAND_BOTTOM) {
      lastBodyTop = Math.max(lastBodyTop, l.top);
    }
  }
  return lastBodyTop;
}

// Extract footnotes from a page. Returns an array of { marker, text } where
// marker is the footnote number (`"1"`, `"2"`, …) and text is the joined
// footnote prose (continuation lines glued with a space). Footnotes are
// identified as size<10 lines below the body band, with start lines
// matching `^\d+\.\s` and continuation lines tacked on until the next
// numbered start. Asterisk / cross-page footnotes are not yet handled.
//
// The page record's running-head + subtitle band sit ABOVE the body and
// also contain small-font lines, so we constrain to lines whose top is
// strictly below the last body-size line.
function extractPageFootnotes(page) {
  const lines = (page.lines || []).slice().sort((a, b) => a.top - b.top);
  if (!lines.length) return [];
  const lastBodyTop = computeLastBodyTop(lines);
  const fnLines = lines.filter(l =>
    l.size < NY3D_BODY_SIZE_MIN &&
    l.size > 6 &&                       // exclude superscript-tier sizes (~5.5)
    l.top > lastBodyTop &&
    l.text && l.text.trim()
  );
  const footnotes = [];
  let current = null;
  // Marker forms observed in NY3d: `1.` (numbered, common) and `*`
  // (asterisk, used in some opinions for an unnumbered initial note).
  // Other symbols (†, ‡, §) appear rarely in older volumes — left as TODO.
  const markerRe = /^(?:(\d+)\.|(\*))\s+(.*)$/;
  for (const l of fnLines) {
    const t = l.text.trim();
    const m = t.match(markerRe);
    if (m) {
      if (current) footnotes.push(current);
      current = { marker: m[1] || m[2], text: m[3] };
    } else if (current) {
      // Glue continuation lines with `\n` so normalizeLineWraps can apply
      // its end-of-line dehyphenation rules. Joining with space here would
      // turn `Prep-\naration` (a wrap) into `Prep- aration` and the
      // hyphen-newline pattern would never fire.
      current.text += '\n' + t;
    }
    // else: orphan continuation with no preceding marker — likely a
    // cross-page continuation. Phase 2 will handle these properly.
  }
  if (current) footnotes.push(current);
  return footnotes.map(fn => ({
    marker: fn.marker,
    text: normalizeLineWraps(fn.text),
  }));
}

// Parse the volume-page number (the page number printed in the running
// head, e.g. "11" in `MYERS v SCHNEIDERMAN [30 NY3d 1] 11`). Returns null
// for pages without a parseable running head — banner pages, table-of-
// contents pages, etc.
const NY3D_RIGHT_RUNHEAD_RE = /\[\s*\d+\s*(?:NY3d|AD3d|Misc\s*3d)\s*\d+\s*\]\s*(\d+)\s*$/i;
const NY3D_LEFT_RUNHEAD_RE = /^\s*(\d+)\s+\d+\s*(?:NEW\s*YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS/i;
function getVolumePage(page) {
  const lines = (page.lines || []).slice().sort((a, b) => a.top - b.top);
  for (let i = 0; i < Math.min(3, lines.length); i++) {
    const t = lines[i].text.trim();
    if (!t) continue;
    let m = t.match(NY3D_RIGHT_RUNHEAD_RE);
    if (m) return parseInt(m[1], 10);
    m = t.match(NY3D_LEFT_RUNHEAD_RE);
    if (m) return parseInt(m[1], 10);
    break;  // first non-empty line is the running head if anywhere
  }
  return null;
}

// Walk the post-normalize body text to extract sentinel-encoded markers
// and page breaks. Returns the cleaned text (sentinels stripped) plus two
// parallel arrays of `{ offset, ... }` records giving each marker / page-
// break's character offset in the cleaned text. The typesetter renders
// `body, <super-N> body...` with a space on both sides of the marker; we
// preserve the leading space (already accumulated in `result`) and drop
// one trailing space to avoid leaving a double space behind.
function processBodyText(text) {
  const fnMarkers = [];
  const pageBreaks = [];
  let result = '';
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === SENTINEL_FN_START) {
      const end = text.indexOf(SENTINEL_FN_END, i + 1);
      if (end < 0) { i++; continue; }   // malformed sentinel — skip
      const marker = text.slice(i + 1, end);
      fnMarkers.push({ offset: result.length, marker });
      i = end + 1;
      if (i < text.length && /\s/.test(text[i])) i++;  // drop one trailing space
    } else if (ch === SENTINEL_PB_START) {
      const end = text.indexOf(SENTINEL_PB_END, i + 1);
      if (end < 0) { i++; continue; }
      const meta = text.slice(i + 1, end);
      const sep = meta.indexOf(':');
      const volPageStr = sep >= 0 ? meta.slice(0, sep) : '';
      const pageIdxStr = sep >= 0 ? meta.slice(sep + 1) : meta;
      pageBreaks.push({
        offset: result.length,
        volume_page: volPageStr ? parseInt(volPageStr, 10) : null,
        page_index: pageIdxStr ? parseInt(pageIdxStr, 10) : null,
      });
      i = end + 1;
    } else {
      result += ch;
      i++;
    }
  }
  // Tidy any accidental double spaces left after marker stripping. This
  // shifts character positions, so adjust recorded offsets in lockstep.
  // (Avoid a generic .replace because that would invalidate offsets.)
  return { text: result, footnote_markers: fnMarkers, page_breaks: pageBreaks };
}

// For a given offset in segment text, find the page_index the offset is on
// by walking the page_breaks list (offsets are monotonic). Falls back to
// the page recorded in `fallbackPageBreak` (the page break that preceded
// the segment, captured before slicing) if no page break sits within the
// segment yet.
function pageIndexAtOffset(offset, pageBreaks, fallbackPageBreak) {
  let current = fallbackPageBreak || null;
  for (const pb of pageBreaks) {
    if (pb.offset <= offset) current = pb;
    else break;
  }
  return current?.page_index ?? null;
}

// Walk raw text (with sentinels) up to `pos` and return the most recent
// page-break sentinel found before that position. Used to anchor the
// "what page is this segment starting on?" lookup when the segment
// contains no page-break sentinels of its own (typical for the first
// segment, since the page-break sentinel for the OPINION OF THE COURT
// page lives in the editorial preamble that we slice off).
function findPageBreakBefore(text, pos) {
  let result = null;
  let i = 0;
  while (i < pos) {
    if (text[i] === SENTINEL_PB_START) {
      const end = text.indexOf(SENTINEL_PB_END, i + 1);
      if (end < 0 || end >= pos) break;
      const meta = text.slice(i + 1, end);
      const sep = meta.indexOf(':');
      const volPageStr = sep >= 0 ? meta.slice(0, sep) : '';
      const pageIdxStr = sep >= 0 ? meta.slice(sep + 1) : meta;
      result = {
        page_index: pageIdxStr ? parseInt(pageIdxStr, 10) : null,
        volume_page: volPageStr ? parseInt(volPageStr, 10) : null,
      };
      i = end + 1;
    } else {
      i++;
    }
  }
  return result;
}

// Match each in-body marker found in a segment to a case-level footnote
// text by `(marker, page_index)`. The page is determined from the segment's
// page_breaks list. Returns the per-segment footnotes array with each
// entry carrying `body_offset`, `marker`, `text`, `page_index`,
// `volume_page`, and `footnote_index` (ordinal within the segment).
function attributeSegmentFootnotes(segment, caseFootnotes, fallbackPageBreak) {
  const out = [];
  let i = 0;
  for (const m of segment.footnote_markers) {
    const pageIdx = pageIndexAtOffset(m.offset, segment.page_breaks, fallbackPageBreak);
    const fn = caseFootnotes.find(f => f.marker === m.marker && f.page_index === pageIdx);
    out.push({
      footnote_index: i++,
      marker: m.marker,
      text: fn ? fn.text : null,
      body_offset: m.offset,
      page_index: pageIdx,
      volume_page: fn ? fn.volume_page : null,
    });
  }
  return out;
}

// Sentinels used to track footnote-marker positions and page-break
// positions through the line-text concat + normalizeLineWraps stages.
// Private-use code points; won't collide with real opinion text.
//   `<marker>`        — in-body footnote marker (e.g., `1`, `*`)
//   `<volPage>:<pageIdx>` — page-break with both numbers
//
// `pdf_parse.py` wraps superscript-sized words (size<6) with the FN
// sentinels at line-build time; this file inserts the page-break sentinel
// at the start of each page's content in `buildNy3dPageText`. After
// `normalizeLineWraps`, `processBodyText` walks the result to extract
// per-marker offsets (in the cleaned text), strip the sentinels, and
// return the final body text + parallel `footnote_markers` and
// `page_breaks` arrays.
const SENTINEL_FN_START   = '';
const SENTINEL_FN_END     = '';
const SENTINEL_PB_START   = '';
const SENTINEL_PB_END     = '';

// Build a page's text from its `lines` array (top-sorted) with running-head
// + subtitle stripped, footnote lines stripped, and a blank line inserted
// before each indented line (paragraph start). `endOff` (when provided for
// the last page) truncates to the byte offset in `text_raw` where the next
// case begins — we map this to a top boundary by walking lines until we
// cross the cumulative offset.
function buildNy3dPageText(page, endOff) {
  const lines = (page.lines || []).slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
  if (!lines.length) {
    let txt = page.text_raw || '';
    if (endOff != null) txt = txt.slice(0, endOff);
    return stripPageHeader(txt);
  }

  // Compute the bottom of the body band (last body-size line). Lines below
  // this with size<10 are footnotes — excluded from the opinion body output
  // and handled separately by extractPageFootnotes.
  const lastBodyTop = computeLastBodyTop(lines);

  // Determine the dominant body x0 (mode of x0 values, rounded to int) so we
  // can detect indented lines as paragraph starts.
  const bucket = {};
  for (const l of lines) {
    const k = Math.round(l.x0);
    bucket[k] = (bucket[k] || 0) + 1;
  }
  const bodyX0 = Number(Object.entries(bucket).sort((a, b) => b[1] - a[1])[0][0]);
  // ~5pt = roughly half a printer's en — observed indent is ~11pt for
  // paragraph starts in NY3d body text. Use 5 as the lower bound so we don't
  // miss subtle indents but skip noise.
  const indentThreshold = bodyX0 + 5;

  // Skip the running-head + subtitle band by top-coordinate. The running
  // head sits at top≈117 and the subtitle (1-2 lines, when the surname
  // renders as lead-cap + small-caps body on different baselines) at
  // top≈127-130. Body content starts at top≥140. Drop anything with top
  // less than NY3D_HEADER_BAND_BOTTOM. This also handles the case where
  // the subtitle's small-caps body fragment (e.g. `TEIN .` below `Opinion
  // by S , J`) doesn't match the textual subtitle regex but sits in the
  // same band as the rest of the subtitle.
  let drop = 0;
  while (drop < lines.length && lines[drop].top < NY3D_HEADER_BAND_BOTTOM) drop++;

  // If endOff is set, find which line index corresponds to byte position
  // endOff in text_raw — easier to truncate by line index than to map
  // offsets back. Walk page.text_raw splitting on \n: the line at offset
  // endOff corresponds to a line index in lines (assuming 1:1 ordering).
  let stopIdx = lines.length;
  if (endOff != null && page.text_raw) {
    const truncated = page.text_raw.slice(0, endOff);
    // Approximate line count in truncated text — number of \n-delimited
    // non-empty segments. Lines from `lines` should align with text_raw's
    // visual line ordering.
    const truncLines = truncated.split('\n').filter(s => s.trim()).length;
    stopIdx = Math.min(stopIdx, drop + truncLines);
  }

  const out = [];
  // Page-break sentinel at the start of every page's content. Surfaces in
  // processBodyText as a `{ offset, page_index, volume_page }` record so
  // pinpoint-cite anchors can link to the right page. Volume page may be
  // null on banner / non-running-head pages.
  const volPage = getVolumePage(page);
  out.push(`${SENTINEL_PB_START}${volPage ?? ''}:${page.page_index}${SENTINEL_PB_END}`);

  // Track the most recent body-size line we've emitted so we can recognize
  // small-caps body fragments that ride directly under a body line. On
  // opinion-start pages where no body content follows the byline (short
  // case + byline at the end of the page), `lastBodyTop` ends up being
  // the byline lead itself — so the byline's body fragment ("OHNNY AYNES"
  // under "J L. B , J.") would otherwise be wrongly treated as a footnote.
  // Real footnote bands always have a vertical gap > ~6pt between body and
  // notes; small-caps fragments sit ~3pt below their lead.
  let prevBodyLineTop = -Infinity;

  for (let i = drop; i < stopIdx; i++) {
    const l = lines[i];
    if (!l.text || !l.text.trim()) continue;
    // Strip footnote lines: smaller font AND below the body band. These
    // are surfaced separately via extractPageFootnotes; including them in
    // body text causes false-positive search hits and messes up holdings
    // / citation-graph analysis. Small-caps body fragments interspersed
    // with body text (above lastBodyTop) are kept; small-caps body
    // fragments riding directly under a body line on an opinion-start
    // page (within ~6pt) are also kept.
    if (l.size < NY3D_BODY_SIZE_MIN && l.top > lastBodyTop && (l.top - prevBodyLineTop >= 6)) continue;
    // Suppress paragraph break for orphan small-caps body fragments. The
    // pdfplumber output renders the small-caps remnant of a surname (e.g.
    // `ILSON` under lead `W`, `EMORANDUM` under lead `M`) as its own line
    // at a non-body x0. Treating it as a paragraph start would inject a
    // spurious break BEFORE the fragment, which then survives even after
    // the in-body marker code splices the fragment out — leaving the wrong
    // paragraph boundary in the body text. Heuristic: short line with no
    // lowercase letters (typical for these fragments). Section headers
    // like `I.`, `II.`, `A.`, `B.` use periods OR are very short Roman/
    // letter forms and are treated as legitimate paragraph starts.
    const t = l.text.trim();
    const isOrphanFrag =
      t.length <= 25 &&
      !/[a-z]/.test(t) &&
      !/^[IVX]+\.?$/i.test(t) &&     // Roman numeral section header
      !/^[A-Z]\.?$/.test(t);          // single-letter section header
    if (l.x0 >= indentThreshold && !isOrphanFrag) {
      // Mark a paragraph break before this line. We emit even when `out` is
      // empty (the leading blank line ensures a paragraph break is preserved
      // when this page is concatenated to the previous one).
      out.push('');
    }
    out.push(l.text);
    if (l.size >= NY3D_BODY_SIZE_MIN) prevBodyLineTop = l.top;
  }
  return out.join('\n');
}

// Detect the lead opinion's byline in the 1-2 non-empty lines after the
// `OPINIONOFTHECOURT` marker. Returns { kind, author, linesUsed } or null.
function parseNy3dLeadByline(line1, line2) {
  // Per Curiam form (most common for memos and many opinions).
  if (/^Per\s+Curiam\.?$/i.test(line1)) {
    return { kind: 'per_curiam', author: null, linesUsed: 1 };
  }
  // Chief Judge form (NY3d only): "Chief Judge D F[.]" on line 1, small-caps
  // surname body ("I IORE[.]") on line 2 → "Chief Judge DiFiore.". The
  // closing period can land on either line depending on typesetting; allow
  // both. Reject the multi-judge concur form ("Chief Judge D F and Judges
  // …") which begins with the same prefix but has more text after.
  const cj = line1.match(/^Chief\s+Judge\s+(.+?)\s*\.?\s*$/);
  if (cj && line2 && /^[A-Z]/.test(line2) && !/\band\b/i.test(line1)) {
    // Strip trailing period (if any) from the body-caps line for clean recombine.
    const body = line2.replace(/\.\s*$/, '').trim();
    const recombined = recombineLeadAndBodyCaps(cj[1], body);
    if (recombined) {
      return { kind: 'majority', author: `Chief Judge ${recombined}.`, linesUsed: 2 };
    }
  }
  // Signed: lead caps + ", J./JJ./S./Surr.", with small-caps body of the
  // surname on the next line. Reuse parseAd3dByline's recombiner.
  return parseAd3dByline(line1, line2);
}

// Stitch a lead-caps token sequence ("D F") with its small-caps body
// fragments ("I IORE") into a mixed-case name ("DiFiore"). Returns null if
// the token counts don't line up. Used for Chief Judge bylines and as a
// shared building block for parseAd3dByline.
function recombineLeadAndBodyCaps(leadStr, bodyStr) {
  const tokens = [];
  let cur = '';
  for (let i = 0; i < leadStr.length; i++) {
    const ch = leadStr[i];
    if (/[A-Z]/.test(ch))      cur += ch;
    else if (ch === '.')        { if (cur) tokens.push({ text: cur, type: 'init' }); cur = ''; }
    else if (ch === '-')        { if (cur) { tokens.push({ text: cur, type: 'cap' }); cur = ''; } tokens.push({ type: 'hyphen' }); }
    else if (/\s/.test(ch))     { if (cur) { tokens.push({ text: cur, type: 'cap' }); cur = ''; } }
  }
  if (cur) tokens.push({ text: cur, type: 'cap' });
  const bodyCaps = bodyStr.trim().split(/[\s-]+/).filter(Boolean);
  const mainCount = tokens.filter(t => t.type === 'cap').length;
  if (mainCount !== bodyCaps.length || mainCount === 0) return null;
  let bi = 0;
  const parts = [];
  for (const t of tokens) {
    if (t.type === 'cap')        parts.push(t.text + bodyCaps[bi++].toLowerCase());
    else if (t.type === 'init')  parts.push(t.text + '.');
    else if (t.type === 'hyphen')parts.push('-');
  }
  // Join: hyphens stick; consecutive caps glue without space (DiFiore form);
  // separators between an initial and the next cap insert a single space.
  let result = '';
  for (let i = 0; i < parts.length; i++) {
    const p = parts[i];
    if (p === '-')                                  result += '-';
    else if (!result || result.endsWith('-'))       result += p;
    else if (result.endsWith('.'))                  result += ' ' + p;  // initial → next token
    else                                            result += p;        // cap → cap glue (Di+Fiore)
  }
  return result;
}

// Find inline concurrence/dissent markers in a body slice. Each marker
// records its position, length, classified type, reconstructed author, and
// the range of any orphan small-caps body fragment that needs to be
// spliced out of the segment text.
function findNy3dInBodyMarkers(body) {
  const markers = [];
  NY3D_INBODY_MARKER_RE.lastIndex = 0;
  let m;
  while ((m = NY3D_INBODY_MARKER_RE.exec(body)) !== null) {
    const lead = m.groups.lead;
    const title = m.groups.title;
    const verb = m.groups.verb;
    const mod = m.groups.mod;
    const consumeStart = m.index + (m[1] ? m[1].length : 0);
    const consumeEnd = m.index + m[0].length;
    let author = lead;
    let fragRange = null;
    if (lead.length === 1) {
      // Small-caps form: lead is single letter; body fragment (rest of the
      // surname) sits on a separate line within ~300 chars after the byline.
      const tail = body.slice(consumeEnd, consumeEnd + 300);
      let frag = tail.match(/\n([A-Z]+(?:-[A-Z]+)?)[ \t]*\n/);
      if (frag) {
        author = lead + frag[1].toLowerCase();
        const fragStart = consumeEnd + frag.index + 1;             // skip leading \n
        const fragEnd   = consumeEnd + frag.index + 1 + frag[1].length + 1;  // include trailing \n
        fragRange = [fragStart, fragEnd];
      } else {
        // Fallback: any standalone all-caps run.
        frag = tail.match(/\b([A-Z]{2,}(?:-[A-Z]+)?)\b/);
        if (frag) {
          author = lead + frag[1].toLowerCase();
          fragRange = [consumeEnd + frag.index, consumeEnd + frag.index + frag[1].length];
        } else {
          author = null;
        }
      }
    } else if (/^[A-Z]+$/.test(lead) && lead.length > 1) {
      // ALLCAPS form (rare in body): title-case it.
      author = lead[0] + lead.slice(1).toLowerCase();
    }
    markers.push({
      offset: consumeStart,
      consumed: consumeEnd - consumeStart,
      fragRange,
      type: classifyParenType(verb, mod),
      author: author ? `${author}, ${title}.` : null,
    });
  }
  return markers;
}

function extractNy3dOpinions(pages, range, caseFootnotes = []) {
  const startP = range.start_page_index;
  const endP   = range.end_page_index;
  const combined = combineNy3dPages(pages, range);

  const markerMatch = combined.match(OPINION_MARKER_RE);
  if (!markerMatch) return [];

  // The OPINION OF THE COURT page — used as the fallback page anchor for
  // the FIRST segment, which starts on this page but contains no page-
  // break sentinel for it (the sentinel lives in the editorial preamble
  // we slice off below).
  const opinionStartPage = findPageBreakBefore(combined, markerMatch.index);

  // Skip past the OPINIONOFTHECOURT marker + any whitespace.
  let pos = markerMatch.index + markerMatch[0].length;
  while (pos < combined.length && /\s/.test(combined[pos])) pos++;

  // Read up to 2 non-empty lines for the lead byline.
  const after = combined.slice(pos);
  const lines = after.split('\n');
  const ne = [];
  for (let i = 0; i < lines.length && ne.length < 2; i++) {
    if (lines[i].trim()) ne.push(i);
  }
  const line1 = ne.length ? lines[ne[0]].trim() : '';
  const line2 = ne.length >= 2 ? lines[ne[1]].trim() : '';

  let firstType = 'majority', firstAuthor = null, bylineLines = 0;
  const byline = parseNy3dLeadByline(line1, line2);
  if (byline) {
    firstType = byline.kind;
    firstAuthor = byline.author;
    bylineLines = byline.linesUsed;
  }

  // Compute body offset (skip past the byline lines).
  let bodyStart = 0;
  if (bylineLines > 0) {
    const bodyLineIdx = ne[bylineLines - 1] + 1;
    for (let i = 0; i < bodyLineIdx; i++) bodyStart += lines[i].length + 1;  // +1 for \n
  }
  const body = after.slice(bodyStart);

  // End the body at the concur block (operating on raw text — sentinels
  // and \n preserved here, both regexes are sentinel-safe).
  const concurEnd = findNy3dConcurEnd(body);
  const bodyMain = body.slice(0, concurEnd >= 0 ? concurEnd : body.length);

  // Normalize line wraps + strip sentinels in one pass. processBodyText
  // gives us the cleaned body text plus parallel arrays of footnote-marker
  // and page-break offsets in the cleaned text. Subsequent segment slicing
  // and in-body byline detection operate on the cleaned text so the
  // sentinel chars don't trip up the regexes (and so segment offsets are
  // directly usable downstream).
  const normalized = normalizeLineWraps(bodyMain.trim());
  const processed = processBodyText(normalized);
  const cleaned = processed.text;
  const allFnMarkers = processed.footnote_markers;
  const allPageBreaks = processed.page_breaks;

  // Find inline concurrence/dissent markers on the CLEANED text.
  const segMarkers = findNy3dInBodyMarkers(cleaned);

  // Build segments with [start, end] offsets in the cleaned body text.
  const rawSegments = [];
  rawSegments.push({
    type: firstType,
    author: firstAuthor,
    rawStart: 0,
    rawEnd: segMarkers[0]?.offset ?? cleaned.length,
    fragRange: null,
  });
  for (let i = 0; i < segMarkers.length; i++) {
    const cur = segMarkers[i];
    const nxt = segMarkers[i + 1];
    rawSegments.push({
      type: cur.type,
      author: cur.author,
      rawStart: cur.offset + cur.consumed,
      rawEnd: nxt?.offset ?? cleaned.length,
      fragRange: cur.fragRange,
    });
  }

  // For each segment: slice cleaned text, partition the global marker /
  // page-break arrays by [rawStart, rawEnd], translate offsets to be
  // segment-local, splice out any small-caps fragment, then attribute
  // footnote markers to case-level footnotes via (marker, page_index).
  return rawSegments.map((seg, i) => {
    let segText = cleaned.slice(seg.rawStart, seg.rawEnd);
    let segFnMarkers = allFnMarkers
      .filter(m => m.offset >= seg.rawStart && m.offset < seg.rawEnd)
      .map(m => ({ offset: m.offset - seg.rawStart, marker: m.marker }));
    let segPageBreaks = allPageBreaks
      .filter(pb => pb.offset >= seg.rawStart && pb.offset < seg.rawEnd)
      .map(pb => ({
        offset: pb.offset - seg.rawStart,
        page_index: pb.page_index,
        volume_page: pb.volume_page,
      }));

    // Splice the byline's small-caps body fragment out of the segment text.
    // All marker / page-break offsets after the splice shift by -fragLen.
    if (seg.fragRange) {
      const ls = seg.fragRange[0] - seg.rawStart;
      const le = seg.fragRange[1] - seg.rawStart;
      if (ls >= 0 && le <= segText.length) {
        const fragLen = le - ls;
        segText = segText.slice(0, ls) + segText.slice(le);
        segFnMarkers = segFnMarkers.map(m => ({
          ...m,
          offset: m.offset >= le ? m.offset - fragLen : m.offset,
        }));
        segPageBreaks = segPageBreaks.map(pb => ({
          ...pb,
          offset: pb.offset >= le ? pb.offset - fragLen : pb.offset,
        }));
      }
    }

    // Page break that preceded this segment's start. The most recent page-
    // break in the *whole* body before seg.rawStart wins; if none, fall
    // back to the OPINION OF THE COURT page (relevant for the first
    // segment, where the marker page itself has no in-body sentinel).
    const fallbackPageBreak =
      [...allPageBreaks].reverse().find(pb => pb.offset <= seg.rawStart) ||
      opinionStartPage;

    const trimmed = segText.trim();
    const trimAdjustment = segText.indexOf(trimmed);
    const adjOffset = (off) => off - trimAdjustment;
    const finalFnMarkers = segFnMarkers
      .map(m => ({ offset: adjOffset(m.offset), marker: m.marker }))
      .filter(m => m.offset >= 0 && m.offset <= trimmed.length);
    const finalPageBreaks = segPageBreaks
      .map(pb => ({ ...pb, offset: adjOffset(pb.offset) }))
      .filter(pb => pb.offset >= 0 && pb.offset <= trimmed.length);

    const segObj = {
      footnote_markers: finalFnMarkers,
      page_breaks: finalPageBreaks,
    };
    const footnotes = attributeSegmentFootnotes(segObj, caseFootnotes, fallbackPageBreak);

    return {
      opinion_index: i,
      opinion_type: seg.type,
      author: seg.author,
      start_page_index: startP,
      end_page_index: endP,
      text: trimmed,
      footnotes,
      page_breaks: finalPageBreaks,
    };
  });
}

function extractNy3dMemo(pages, range, caseFootnotes = []) {
  const startP = range.start_page_index;
  // Use text_end_* fields to honor the actual body extent. case_boundaries
  // sets `end_page_index` to the cite page of the LAST page-cite-header in
  // the case range, which can be earlier than the page where the body and
  // the next memo's `[cite]` appear together.
  const endP = range.text_end_page_index ?? range.end_page_index;
  const combined = combineNy3dPages(pages, range, {
    endPage: endP,
    endOffset: range.text_end_offset,
  });

  const markerMatch = combined.match(OPINION_MARKER_RE);
  if (!markerMatch) {
    return [];
  }

  let pos = markerMatch.index + markerMatch[0].length;
  while (pos < combined.length && /\s/.test(combined[pos])) pos++;
  const after = combined.slice(pos);

  // Memos may have a byline (Per Curiam, Memorandum, signed) or none. When
  // present, skip it before extracting body. Reuse the lead-byline parser
  // that already handles `Per Curiam.` / `Chief Judge X.` / signed forms /
  // memorandum form (`M .` + `EMORANDUM`).
  const lines = after.split('\n');
  const ne = [];
  for (let i = 0; i < lines.length && ne.length < 2; i++) {
    if (lines[i].trim()) ne.push(i);
  }
  const line1 = ne.length ? lines[ne[0]].trim() : '';
  const line2 = ne.length >= 2 ? lines[ne[1]].trim() : '';
  const byline = parseNy3dLeadByline(line1, line2);
  let bodyStart = 0;
  if (byline) {
    const lastBylineIdx = ne[byline.linesUsed - 1];
    for (let i = 0; i <= lastBylineIdx; i++) bodyStart += lines[i].length + 1;
  }
  const body = after.slice(bodyStart);
  const concurEnd = findNy3dConcurEnd(body);
  const bodyTextRaw = body.slice(0, concurEnd >= 0 ? concurEnd : body.length);

  // Normalize + strip sentinels; surface footnote markers + page breaks
  // for the single memo segment. Memos don't split into multi-opinion
  // segments, so the whole body is one segment.
  const normalized = normalizeLineWraps(bodyTextRaw.trim());
  const processed = processBodyText(normalized);
  const segObj = {
    footnote_markers: processed.footnote_markers,
    page_breaks: processed.page_breaks,
  };
  const footnotes = attributeSegmentFootnotes(segObj, caseFootnotes, null);

  return [{
    opinion_index: 0,
    opinion_type: 'memorandum',
    author: null,
    start_page_index: startP,
    end_page_index: endP,
    text: processed.text,
    footnotes,
    page_breaks: processed.page_breaks,
  }];
}

/**
 * Extract the body text of a single AD3d/Misc 3d memo. The walker has
 * already given us start_page+start_offset (right after `]—`) and
 * end_page+end_offset (just before the next memo's `[`). We walk pages in
 * that range, stripping running-head lines from continuation pages, then
 * apply the body-start heuristic to skip the topic block.
 */
function extractMemoText(pages, range) {
  // Walk from the cite page (where `[NYS3d]—` is) — for off-by-one cases
  // the start_page_index can be earlier than the cite page (when the memo
  // caption wraps from the prior page), but text_start_offset is on the
  // cite page.
  const citeP   = range.cite_page_index ?? range.start_page_index;
  const endP    = range.text_end_page_index;
  const startOff = range.text_start_offset;
  const endOff   = range.text_end_offset;     // null → end of endP's text

  const parts = [];
  for (const p of pages) {
    if (p.page_index < citeP || p.page_index > endP) continue;
    const pageText = p.text_raw || '';
    let from = 0, to = pageText.length;
    if (p.page_index === citeP) from = startOff;
    if (p.page_index === endP && endOff !== null) to = endOff;
    let slice = pageText.slice(from, to);
    if (p.page_index !== citeP) {
      // Strip leading running-head line(s) on continuation pages.
      while (true) {
        const nl = slice.indexOf('\n');
        if (nl === -1) break;
        const firstLine = slice.slice(0, nl);
        if (!firstLine.trim()) { slice = slice.slice(nl + 1); continue; }
        if (isRunningHeadLine(firstLine)) { slice = slice.slice(nl + 1); break; }
        break;
      }
    }
    parts.push(slice);
  }
  let combined = parts.join('\n').trim();
  if (!combined) return '';
  const bodyStart = findMemoBodyStart(combined, 0);
  const body = combined.slice(bodyStart).trim();
  const trimmed = trimNextMemoCaption(body);
  return normalizeLineWraps(trimmed);
}

/**
 * Normalize PDF line-wraps to flowing text:
 *   - Join hyphenated word-wraps: "Mi-\nchael" → "Michael"
 *   - Collapse single newlines to spaces: "An appeal\nhaving been" →
 *     "An appeal having been"
 *   - Preserve paragraph breaks (`\n\n` or more) as a single newline
 *
 * pdfplumber's text_raw doesn't reliably preserve indentation-based
 * paragraph cues, so most paragraph breaks are lost upstream regardless.
 * What this leaves us with is one long paragraph per memo, plus the
 * `Concur—…JJ.` line. That matches the body content the DB stores even if
 * paragraph segmentation differs.
 */
function normalizeLineWraps(text) {
  const PARA_MARK = '\x01';
  let out = text;
  // Compound-term wrap, lowercase neighbor: a hyphen at line end inside an
  // already-hyphenated term ("aid-in-\ndying") is real punctuation, not a
  // soft typesetter break — preserve the hyphen. Lookbehind requires a
  // preceding `-<letters>` so we only match the second/third hyphen of a
  // compound, not a single soft hyphen.
  out = out.replace(/(?<=-[a-z]+)-\n([a-z])/g, '-$1');
  // Soft-hyphen wrap: lowercase + hyphen + newline + lowercase → rejoin
  // ("Mi-\nchael" → "Michael", "Justifica-\ntion" → "Justification")
  out = out.replace(/([a-z])-\n([a-z])/g, '$1$2');
  // Compound word wrap: letter + hyphen + newline + uppercase → keep hyphen, no space
  // ("Manzanet-\nDaniels" → "Manzanet-Daniels")
  out = out.replace(/([A-Za-z])-\n([A-Z])/g, '$1-$2');
  // Paragraph breaks: 2+ newlines → placeholder
  out = out.replace(/\n{2,}/g, PARA_MARK);
  // Single newlines → space
  out = out.replace(/\n/g, ' ');
  // Restore paragraph markers
  out = out.replace(new RegExp(PARA_MARK, 'g'), '\n');
  // Tidy double spaces
  out = out.replace(/ {2,}/g, ' ');
  return out.trim();
}

/**
 * Group consecutive pages within a case range into opinion segments.
 * Each segment is a run of pages with the same (kind, author) classification.
 * Returns the list of opinion segments — header/unknown pages are dropped.
 */
function segmentPages(pages, caseRange, opinionTypeOverride) {
  const inRange = pages.filter(p =>
    p.page_index >= caseRange.start_page_index &&
    p.page_index <= caseRange.end_page_index
  );

  const classified = inRange.map(p => {
    const subtitle = readSubtitle(p);
    const { kind, author } = classifySubtitle(subtitle);
    return { page: p, subtitle, kind, author };
  });

  const segments = [];
  for (const item of classified) {
    if (item.kind === 'header' || item.kind === 'unknown') continue;
    const last = segments[segments.length - 1];
    if (last && last.kind === item.kind && last.author === item.author) {
      last.pages.push(item);
    } else {
      segments.push({ kind: item.kind, author: item.author, pages: [item] });
    }
  }

  // Memorandum override: every case in the memoranda section gets one
  // synthetic opinion of type 'memorandum' regardless of subtitle content.
  if (opinionTypeOverride === 'memorandum') {
    return [{
      kind: 'memorandum',
      author: null,
      pages: classified.filter(c => c.kind === 'unknown' || c.kind === 'header' || c.kind === 'memorandum'),
    }];
  }

  return segments;
}

/**
 * Top-level: extract opinions for a case range. `caseRange` includes
 * { start_page_index, end_page_index, section, text_start_offset,
 * text_end_page_index, text_end_offset }. Returns the opinion list.
 *
 * Memoranda: extracted as one synthetic memorandum opinion using the
 * offset-based slicer (handles multi-memo-per-page in AD3d).
 * Opinions: classified via small-subtitle (NY3d only); AD3d/Misc 3d
 * opinion-section cases currently produce zero opinions because they
 * don't carry the subtitle convention — to be addressed separately.
 */
/**
 * Extract footnotes for a case range. Walks every page in the range, picks
 * up footnote-sized text below the body band on each page, and tags each
 * footnote with its page_index + volume_page so downstream code can map
 * back to a pinpoint location. NY3d cases restart numbering per opinion
 * (per_curiam→1,2,…; concurrence→1,2,…), so the same `marker` value can
 * appear multiple times in the result — disambiguate by page_index.
 *
 * v1 limitation: footnotes that span page boundaries (continuation lines
 * at the top of the next page's footnote band, no number prefix) are
 * dropped. In-body marker-to-footnote attribution + per-opinion split is
 * Phase 2 work alongside character-offset tracking.
 */
export function extractCaseFootnotes(pages, caseRange) {
  const startP = caseRange.start_page_index;
  const endP   = caseRange.text_end_page_index ?? caseRange.end_page_index;

  // Skip pages before OPINION OF THE COURT — those are editorial preamble
  // (SUMMARY / HEADNOTE / APPEARANCES OF COUNSEL) where numbered HEADNOTE
  // entries look identical to footnotes (same `^\d+\.` shape, same 8.98pt
  // font) and would be picked up as false positives. The marker page
  // itself is included since on that page the marker sits mid-page and
  // any footnotes would be below the body band, naturally separated.
  let bodyStartP = startP;
  for (const p of pages) {
    if (p.page_index < startP || p.page_index > endP) continue;
    if (OPINION_MARKER_RE.test(p.text_raw || '')) {
      bodyStartP = p.page_index;
      break;
    }
  }

  const out = [];
  for (const p of pages) {
    if (p.page_index < bodyStartP || p.page_index > endP) continue;
    const fns = extractPageFootnotes(p);
    if (!fns.length) continue;
    const volPage = getVolumePage(p);
    for (const fn of fns) {
      out.push({
        marker: fn.marker,
        text: fn.text,
        page_index: p.page_index,
        volume_page: volPage,
      });
    }
  }
  return out;
}

export function extractOpinions(pages, caseRange, caseFootnotes = []) {
  // Reporter discriminator: AD3d/Misc 3d carry one parallel cite (NYS3d
  // only); NY3d carries two (NE3d + NYS3d). Their body conventions differ
  // enough that we route them through separate extractors. AD3d/Misc 3d
  // paths don't yet have sentinel-aware footnote/page-break processing —
  // those reporters are not active backfill targets, so the AD3d output
  // continues to use text_raw-based concat without per-marker offsets.
  const isAd3dStyle = (caseRange.parallel_cites || []).length === 1;

  if (caseRange.section === 'opinions') {
    if (isAd3dStyle) {
      return extractAd3dOpinions(pages, caseRange, caseFootnotes);
    }
    return extractNy3dOpinions(pages, caseRange, caseFootnotes);
  }

  if (caseRange.section === 'memoranda') {
    if (isAd3dStyle) {
      return extractAd3dMemo(pages, caseRange, caseFootnotes);
    }
    return extractNy3dMemo(pages, caseRange, caseFootnotes);
  }

  return [];
}

/**
 * AD3d / Misc 3d opinion-section path with full sentinel-aware footnote +
 * page-break attribution.
 *
 * Splits the body via `findOpinionMarkers` (same regex set as splitMultiOpinion
 * uses) into segments with explicit offset ranges in the cleaned text, then
 * partitions the global footnote_markers / page_breaks arrays per segment and
 * attributes markers to case-level footnotes via (marker, page_index).
 */
function extractAd3dOpinions(pages, caseRange, caseFootnotes) {
  const opinion = extractOpinionSectionText(pages, caseRange);
  if (!opinion) return [];

  const cleaned = opinion.text;
  const allFnMarkers  = opinion.footnote_markers || [];
  const allPageBreaks = opinion.page_breaks || [];

  // Find inline concurrence/dissent markers in the cleaned text. Only count
  // markers past the 200-char minimum and after a prior-opinion-end signal —
  // matching splitMultiOpinion's filtering so we don't false-split on
  // citations to other cases that mention "(concurring)".
  const markers = (cleaned.length >= 200)
    ? findOpinionMarkers(cleaned).filter(m =>
        m.offset > 100 && hasPriorOpinionEnd(cleaned, m.offset)
      )
    : [];

  // Build segment offset ranges. Segment 0 is the lead opinion (from byline);
  // each marker starts a new segment.
  const segments = [];
  segments.push({
    type: opinion.opinion_type,
    author: opinion.author,
    rawStart: 0,
    rawEnd: markers[0]?.offset ?? cleaned.length,
  });
  for (let i = 0; i < markers.length; i++) {
    segments.push({
      type: markers[i].type,
      author: markers[i].author,
      rawStart: markers[i].offset,
      rawEnd: markers[i + 1]?.offset ?? cleaned.length,
    });
  }

  return segments.map((seg, i) => {
    let segText = cleaned.slice(seg.rawStart, seg.rawEnd);
    let segFnMarkers = allFnMarkers
      .filter(m => m.offset >= seg.rawStart && m.offset < seg.rawEnd)
      .map(m => ({ offset: m.offset - seg.rawStart, marker: m.marker }));
    let segPageBreaks = allPageBreaks
      .filter(pb => pb.offset >= seg.rawStart && pb.offset < seg.rawEnd)
      .map(pb => ({
        offset: pb.offset - seg.rawStart,
        page_index: pb.page_index,
        volume_page: pb.volume_page,
      }));

    // Trim segment text and adjust offsets in lockstep.
    const trimmed = segText.trim();
    const trimAdjustment = segText.indexOf(trimmed);
    const adj = (off) => off - trimAdjustment;
    const finalFnMarkers = segFnMarkers
      .map(m => ({ offset: adj(m.offset), marker: m.marker }))
      .filter(m => m.offset >= 0 && m.offset <= trimmed.length);
    const finalPageBreaks = segPageBreaks
      .map(pb => ({ ...pb, offset: adj(pb.offset) }))
      .filter(pb => pb.offset >= 0 && pb.offset <= trimmed.length);

    // Page break that preceded this segment's start — used as the fallback
    // page anchor for footnotes whose marker offset sits before any in-segment
    // page break (i.e., on the page where the segment begins).
    const fallbackPageBreak =
      [...allPageBreaks].reverse().find(pb => pb.offset <= seg.rawStart) || null;

    const segObj = {
      footnote_markers: finalFnMarkers,
      page_breaks: finalPageBreaks,
    };
    const footnotes = attributeSegmentFootnotes(segObj, caseFootnotes, fallbackPageBreak);

    return {
      opinion_index: i,
      opinion_type: seg.type,
      author: seg.author,
      start_page_index: opinion.start_page_index,
      end_page_index: opinion.end_page_index,
      text: trimmed,
      footnotes,
      page_breaks: finalPageBreaks,
    };
  });
}

/**
 * AD3d / Misc 3d memoranda path. The boundary walker tracks body offsets in
 * text_raw, which doesn't carry the in-body footnote-marker sentinels. Rather
 * than rebuild that offset machinery against lines-based output (the memo
 * walker is delicate), we keep extractMemoText for the body and attach
 * case-level footnotes directly.
 *
 * Without per-marker body offsets, the footnotes land on opinion 0 with
 * body_offset=null. The schema permits null body_offset; the admin UI shows
 * footnote text under the case but doesn't anchor it to a specific position.
 * This is acceptable for memos because:
 *   - they're typically short single-paragraph entries
 *   - footnote density on AD3d memos is low (~0.1 per case)
 * Multi-opinion memos lump all case footnotes onto opinion 0.
 */
function extractAd3dMemo(pages, caseRange, caseFootnotes) {
  const text = extractMemoText(pages, caseRange);
  if (!text) return [];
  const startP = caseRange.start_page_index;
  const endP   = caseRange.text_end_page_index ?? caseRange.end_page_index;

  const memoFootnotes = (caseFootnotes || []).map((fn, i) => ({
    footnote_index: i,
    marker: fn.marker,
    text: fn.text,
    body_offset: null,
    page_index: fn.page_index,
    volume_page: fn.volume_page,
  }));

  const split = splitMultiOpinion(text, 'memorandum', null);
  if (split) {
    return split.map((seg, i) => ({
      opinion_index: i,
      opinion_type: i === 0 ? 'memorandum' : seg.type,
      author: seg.author,
      start_page_index: startP,
      end_page_index: endP,
      text: seg.text,
      footnotes: i === 0 ? memoFootnotes : [],
      page_breaks: [],
    }));
  }
  return [{
    opinion_index: 0,
    opinion_type: 'memorandum',
    author: null,
    start_page_index: startP,
    end_page_index: endP,
    text,
    footnotes: memoFootnotes,
    page_breaks: [],
  }];
}
