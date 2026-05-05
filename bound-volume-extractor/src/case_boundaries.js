/**
 * Case-boundary walker for the OPINIONS and MEMORANDA sections.
 *
 * Two independent signals exist on each opinion page; we use both:
 *
 *  1. Parallel cite header `[XX NE3d YY, ZZ NYS3d WW]` (squashed by the
 *     typesetter — appears as `[85NE3d57,62NYS3d838]` in text_raw). Present
 *     on the FIRST page of each case, just before the caption. This is our
 *     primary first-page detector.
 *
 *  2. Right-side running head `<NAME> [<vol> <reporter> <pg>] <vol-page>`
 *     (e.g. `MYERS v SCHNEIDERMAN [30 NY3d 1] 3`). Present on every
 *     right-side opinion page including the case's first page. We use this
 *     for the canonical NY3d-form citation.
 *
 * For 30 NY3d both signals independently identify 42 cases. They agree.
 *
 * Returns:
 *   [{
 *     start_page_index, end_page_index,
 *     citation: '30 NY3d 1',          // canonical reporter cite
 *     volume_page: 1,                  // start page within the bound volume
 *     parallel_cites: ['85 NE3d 57', '62 NYS3d 838'],
 *     section: 'opinions' | 'memoranda',
 *   }, ...]
 */

// Parallel cite header on a case's first page. Two flavors:
//   - NY3d (Court of Appeals):   [<n> NE3d <m>, <p> NYS3d <q>]
//   - AD3d / Misc 3d:            [<p> NYS3d <q>]
// AD3d (intermediate appellate) and Misc 3d (trial) cases aren't published
// in the regional NE3d reporter, so they only get the state-specific NYS3d
// parallel cite. We try the two-cite form first; if it doesn't match, fall
// back to the NYS3d-only form. The /g flag is used because AD3d/Misc 3d
// memoranda are short (often <1pp/memo) and multiple memos can appear on
// one page, each with its own inline parallel cite.
const PARALLEL_HEADER_BOTH      = /\[\s*(\d+)\s*NE3d\s*(\d+)\s*,\s*(\d+)\s*NYS3d\s*(\d+)\s*\]/g;
const PARALLEL_HEADER_NYS3D     = /\[\s*(\d+)\s*NYS3d\s*(\d+)\s*\]/g;

// Left-side (verso) running head, reporter-agnostic. Examples:
//   "932 30 NEWYORK REPORTS, 3d SERIES"            (NY3d)
//   "402 157APPELLATE DIVISION REPORTS, 3d SERIES" (AD3d — squashed)
//   "2 57 MISCELLANEOUS REPORTS, 3d SERIES"        (Misc 3d)
// Whitespace between volume number and reporter name is `\s*` because
// pdfplumber sometimes squashes them when the typesetter packs glyphs tight.
const LEFT_RUNHEAD = /^(\d+)\s+\d+\s*(?:NEW\s*YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS/i;

// Right-side memoranda running head. Two forms observed:
//   "MEMORANDA 927"                            (NY3d)
//   "MEMORANDA, First Dept., January, 2018 409"  (AD3d — department + month + page)
const MEMO_RUNHEAD = /^MEMORANDA(?:[,\s].*?)?(\d+)\s*$/;

// Memo-number prefix at the start of an AD3d memo. Each AD3d memo opens
// with a sequence number on its own line at the start of the case caption.
// Caption forms observed across volumes 157-165+:
//
//   `13 In the Matter of Christopher D.B.…`              (known opener word)
//   `12 J K , Appellant, v V G OLKSWAGEN ROUP OF`        (small-caps lead pair)
//   `32 Carol Artibee et al., Appellants, v State of …`  (regular cap-word)
//   `13 Q Aviation Management LLC, Appellant…`           (single-cap + word)
//   `82 E.V., Appellant, v R.V., Respondent.`            (initials form)
//   `21 180 Ludlow Development LLC, Appellant…`          (digit-led entity)
//   `1 2004 McDonald Avenue Corp., Respondent…`          (digit-led entity)
//   `15 In the Matter of <name>, a Suspended Attorney.`  (disciplinary)
//
// We use TWO validators for the lookahead, either of which proves we're
// looking at a real memo prefix:
//
//   (a) `[<vol> NYS3d <page>]—` cite-header terminator within 500 chars.
//       This is the strongest signal — every memo ends its caption with the
//       parallel cite header followed by an em/en-dash. Body-text NYS3d
//       cites end with `,` or `)` or `[year]`, never em-dash.
//
//   (b) `, Appellant/Respondent/Plaintiff/Defendant/Petitioner` within 500
//       chars. Catches the cross-page caption case where the cite header
//       is on the next page (so the strict (a) check doesn't see it). The
//       comma+role-word distinguishes real captions from "the appellant"
//       body-text usage.
const CAPTION_OPENERS = [
  'In', 'The', 'An', 'A', 'Matter', 'People', 'Application',
  'Petition', 'Appeal', 'Order', 'On', 'Estate', 'Claim',
];
const ROLE_MARKER = '(?:Appellants?|Respondents?|Plaintiffs?|Defendants?|Petitioners?)';
const NYS3D_CITE_TERM = '\\[\\s*\\d+\\s+NYS3d\\s+\\d+\\s*\\]\\s*[\\u2014\\u2013\\-]';
const MEMO_PREFIX_RE = new RegExp(
  '(?:^|\\n)\\s*(\\d{1,3})\\s+' +
  '(?=' +
    // (a) Strict: NYS3d cite header with em-dash within 1500 chars. Wider
    //     limit than the role-marker variant because long entity-name
    //     captions (banks listing all their predecessors-by-merger) can
    //     run 700+ chars; body-text false positives are very rare since
    //     `[<vol> NYS3d <page>]—` (em-dash terminator) is exclusively a
    //     memo cite-header layout.
    '[\\s\\S]{0,1500}?' + NYS3D_CITE_TERM +
    '|' +
    // (b) Looser fallback: caption-shape + role marker within 500 chars.
    //     Tighter window because role markers (Appellant/Respondent/etc.)
    //     do appear in body text — just less frequently after a comma.
    '(?:\\d+\\s+)?' +
    '(?:' +
      CAPTION_OPENERS.join('|') +
      '|[A-Z]\\.[A-Z]\\.' +
      '|[A-Z]\\s+[A-Z]' +
      '|[A-Z]' +
    ')' +
    '[\\s\\S]{0,500}?,\\s*' + ROLE_MARKER +
  ')',
  'g'
);

// Running-head bracketed cite. Reporter token allowed: NY3d / AD3d / Misc 3d.
// We pass the reporter from volumeMeta so we can build a tight regex.
function runHeadCiteRegex(reporter) {
  // reporter examples: 'NY3d' | 'AD3d' | 'Misc 3d'
  const escaped = reporter.replace(/\s+/g, '\\s*');
  return new RegExp(`\\[\\s*(\\d+)\\s*${escaped}\\s*(\\d+)\\s*\\]`);
}

function firstNonEmptyLine(page) {
  const text = page.text_raw || '';
  for (const raw of text.split('\n')) {
    const t = raw.trim();
    if (t) return t;
  }
  return '';
}

/**
 * Find every parallel cite header on a page. Returns an array of
 *   { offset, end_offset, parallel_cites: [...] }
 * sorted by character offset within text_raw.
 *   - `offset`: position of the opening `[`
 *   - `end_offset`: position right after `]` (consumes a trailing `—` /
 *     em dash if present, since AD3d/Misc 3d inline memo cites are
 *     followed by `]—Topic` and the dash isn't part of the body)
 * NY3d cases use the two-cite form; AD3d/Misc 3d use NYS3d-only. AD3d/Misc
 * 3d memoranda often have multiple short memos per page, each with an
 * inline parallel cite, so we scan the whole page text rather than just the
 * first 600 chars.
 */
function detectParallelCites(page) {
  const text = page.text_raw || '';
  const hits = [];
  // Scan with the two-cite (NY3d) regex; record matched ranges to avoid
  // double-counting them under the NYS3d-only regex.
  const masked = new Set();
  PARALLEL_HEADER_BOTH.lastIndex = 0;
  let m;
  while ((m = PARALLEL_HEADER_BOTH.exec(text)) !== null) {
    hits.push({
      offset: m.index,
      end_offset: consumeEmDash(text, m.index + m[0].length),
      parallel_cites: [`${m[1]} NE3d ${m[2]}`, `${m[3]} NYS3d ${m[4]}`],
    });
    for (let i = m.index; i < m.index + m[0].length; i++) masked.add(i);
  }
  PARALLEL_HEADER_NYS3D.lastIndex = 0;
  while ((m = PARALLEL_HEADER_NYS3D.exec(text)) !== null) {
    if (masked.has(m.index)) continue;       // already captured by BOTH
    hits.push({
      offset: m.index,
      end_offset: consumeEmDash(text, m.index + m[0].length),
      parallel_cites: [`${m[1]} NYS3d ${m[2]}`],
    });
  }
  hits.sort((a, b) => a.offset - b.offset);
  return hits;
}

/**
 * Advance past an em dash / en dash / hyphen immediately after `]`. AD3d
 * and Misc 3d memos use `[NYS3d cite]—Topic…` as the case-start marker;
 * the dash is a separator, not body text.
 */
function consumeEmDash(text, pos) {
  if (pos < text.length && /[—–-]/.test(text[pos])) return pos + 1;
  return pos;
}

/**
 * Detect the running-head bracketed cite (if present) on the very first line.
 * Returns { volume, page } on hit. Used for canonical citation building.
 */
function detectRunHeadCite(page, runRe) {
  const head = firstNonEmptyLine(page);
  const m = head.match(runRe);
  if (!m) return null;
  return { volume: parseInt(m[1], 10), volume_page: parseInt(m[2], 10) };
}

/**
 * Walk a single section (opinions or memoranda) and return its case ranges.
 */
function walkSection(pages, classification, section, volumeMeta) {
  // Filter pages belonging to this section.
  const inSection = pages.filter((p, i) => classification[i].section === section);
  if (!inSection.length) return [];

  // Build the list of contiguous in-section page ranges. Some Misc 3d
  // volumes interleave 'opinions' and 'abstracts' blocks; without
  // tracking these blocks separately the case-end-page cap below would
  // run a case in the FIRST opinions block all the way through the
  // abstracts section to just before the next case in the SECOND
  // opinions block (e.g. Hayes v Mia's Bathhouse for Pets, 57 Misc 3d 78,
  // ended up with end_page_index=169 instead of 134 because the next
  // case's parallel cite was on page 170).
  const sectionBlocks = [];
  for (let i = 0; i < pages.length; i++) {
    if (classification[i].section !== section) continue;
    const last = sectionBlocks[sectionBlocks.length - 1];
    if (last && pages[i].page_index === last.end + 1) last.end = pages[i].page_index;
    else sectionBlocks.push({ start: pages[i].page_index, end: pages[i].page_index });
  }
  const blockEndFor = (pageIdx) => {
    const blk = sectionBlocks.find(b => pageIdx >= b.start && pageIdx <= b.end);
    return blk ? blk.end : null;
  };

  const reporter = volumeMeta?.reporter || 'NY3d';
  const runRe = runHeadCiteRegex(reporter);

  // First-pass: mark every (page, offset) where a parallel cite header
  // appears. Multiple cites per page are possible in AD3d/Misc 3d memoranda
  // (short memos, several per page).
  const firstPages = [];
  for (const p of inSection) {
    const cites = detectParallelCites(p);
    for (const hit of cites) {
      firstPages.push({
        page_index: p.page_index,
        offset: hit.offset,
        end_offset: hit.end_offset,
        parallel_cites: hit.parallel_cites,
      });
    }
  }

  // Second-pass: detect memo-number prefixes (AD3d memoranda only). A memo's
  // prefix is the canonical start of the case — when a caption wraps across
  // a page break, the parallel cite header lands on a later page than the
  // prefix, and using the cite's page would assign the wrong vol-page to
  // the cite. For each cite, find the nearest preceding prefix that comes
  // *after* the previous cite (so it belongs to this memo, not the prior
  // one); that prefix's page becomes the canonical start_page_index.
  //
  // We run prefix detection on the SECTION-WIDE concatenated text rather
  // than per-page so the regex lookahead (which validates caption-shape
  // within ~500 chars after the prefix) can see across page boundaries.
  // Cross-page memos — prefix at end of page N, parallel cite header at
  // top of page N+1 — would otherwise fail validation since the cite is
  // only visible in the concatenated form.
  //
  // ONLY for AD3d memoranda: NY3d / Misc 3d cases never use memo-number
  // prefixes. Running prefix detection on those reporters gets bitten by
  // body-text citation strings like `... 12 NY3d 468, 476 [2009], ...
  // Plaintiff` which match `\d{1,3}\s+...,\s*<role-marker>` purely by
  // accident. AD3d opinion-section cases also lack prefixes (they follow
  // the NY3d-style parallel-cite-then-caption layout). Restrict here.
  const usePrefixes = reporter === 'AD3d' && section === 'memoranda';
  const SEP = '\n\n';
  const pageBounds = [];
  let cursor = 0;
  const concatParts = [];
  for (const p of inSection) {
    const text = p.text_raw || '';
    pageBounds.push({ page_index: p.page_index, start: cursor, end: cursor + text.length });
    concatParts.push(text);
    cursor += text.length + SEP.length;
  }
  const concatText = concatParts.join(SEP);

  const prefixes = [];
  MEMO_PREFIX_RE.lastIndex = 0;
  let mp;
  while (usePrefixes && (mp = MEMO_PREFIX_RE.exec(concatText)) !== null) {
    let off = mp.index;
    while (off < concatText.length && /[\s]/.test(concatText[off])) off++;
    // Reject false-positive prefixes that are actually the page-top running
    // head. AD3d's left-page running head is "<page> <volume> APPELLATE
    // DIVISION REPORTS, 3d SERIES" — the leading <page> would otherwise look
    // like a memo number to MEMO_PREFIX_RE, and since the caption (with role
    // markers) follows on the same/next page within the lookahead window,
    // the regex's caption-shape validator accepts it. Skip when the prefix's
    // line is a known running-head form.
    const lineStart = concatText.lastIndexOf('\n', off - 1) + 1;
    let lineEnd = concatText.indexOf('\n', off);
    if (lineEnd === -1) lineEnd = concatText.length;
    const line = concatText.slice(lineStart, lineEnd).trim();
    if (PAGE_RUNHEAD_RE.test(line)) continue;

    // Reject prefixes whose preceding line is a continuation fragment, not a
    // memo-end signal. A real memo prefix follows either:
    //   - a concur / disposition line ending in `.`
    //   - a date-cluster header `(Month Day, Year)` ending in `)`
    //   - a running head / section banner (handled separately)
    // Continuation fragments end in `,` (comma — caption wrapping like
    // "Glen Gerisch, Plaintiff, v CF" / "620 Owner One et al., ...") or `-`
    // (soft hyphen wrap), `and`, `or`, etc. Skip when the previous non-empty
    // line ends that way. Also skip when the previous line is itself the
    // page-top running head, which can happen on the very first line of a
    // page where a body line wraps and starts with a digit-led address.
    let prevLineEnd = lineStart - 1;
    while (prevLineEnd >= 0 && concatText[prevLineEnd] === '\n') prevLineEnd--;
    if (prevLineEnd >= 0) {
      const prevLineStart = concatText.lastIndexOf('\n', prevLineEnd) + 1;
      const prevLine = concatText.slice(prevLineStart, prevLineEnd + 1).trim();
      if (prevLine && !PAGE_RUNHEAD_RE.test(prevLine)) {
        // Continuation indicators: end with comma, hyphen, or a continuation
        // word like "and"/"or"/"of"/"the". A real memo-end line ends with
        // `.` or `)`; date-cluster headers end with `)`.
        if (/[,\-]$/.test(prevLine)) continue;
        if (/\b(?:and|or|of|the|with|by|from|et|al)$/i.test(prevLine)) continue;
        // If the previous line doesn't end in `.`, `)`, or `]`, it's almost
        // certainly mid-paragraph body text. Legit memo-end forms:
        //   - `concur.` / `affirmed.` (ends `.`)
        //   - `(Month Day, Year)` date cluster header (ends `)`)
        //   - `[Prior Case History: ...]` trailer (ends `]` or `.]`)
        if (!/[.)\]]$/.test(prevLine)) continue;
      }
    }

    // Map the global offset back to (page_index, page-local offset)
    const bounds = pageBounds.find(b => off >= b.start && off < b.end);
    if (!bounds) continue;
    prefixes.push({ page_index: bounds.page_index, offset: off - bounds.start });
  }
  // Pair each cite with its preceding prefix.
  for (let i = 0; i < firstPages.length; i++) {
    const cite = firstPages[i];
    const prevCite = i > 0 ? firstPages[i - 1] : null;
    let nearest = null;
    for (const pref of prefixes) {
      const beforeCite = pref.page_index < cite.page_index
        || (pref.page_index === cite.page_index && pref.offset < cite.offset);
      if (!beforeCite) break;
      if (prevCite) {
        const afterPrev = pref.page_index > prevCite.page_index
          || (pref.page_index === prevCite.page_index && pref.offset > prevCite.offset);
        if (!afterPrev) continue;
      }
      // Restrict to within 2 pages — a longer back-reach is risky for false matches.
      if (cite.page_index - pref.page_index > 2) continue;
      nearest = pref;
    }
    cite.start_page_index = nearest ? nearest.page_index : cite.page_index;
    cite.start_offset     = nearest ? nearest.offset     : cite.offset;
  }

  // Build ranges: start_i to (next start - 1), last range to end-of-section.
  // AD3d/Misc 3d memo sections often have multiple short memos starting on
  // the same physical page; in that case `next_start - 1 < start`, so we
  // floor `end` at `start` to keep the range non-empty (the body of memo K
  // ends mid-page where memo K+1 begins, but the physical page is shared).
  const sectionEnd = inSection[inSection.length - 1].page_index;
  const cases = [];
  for (let i = 0; i < firstPages.length; i++) {
    // Use the prefix-derived start_page when available (off-by-one fix);
    // fall back to the cite's page if no preceding prefix was found.
    const start = firstPages[i].start_page_index;
    const citePage = firstPages[i].page_index;
    // End cap: next case's start - 1, OR end of section. ALSO capped at
    // the end of THIS case's contiguous in-section block, so cases never
    // bleed across a non-section interlude (e.g., the Misc 3d abstracts
    // block sandwiched between two opinions blocks).
    const blockEnd = blockEndFor(start) ?? sectionEnd;
    const candidateEnd = i + 1 < firstPages.length ? firstPages[i + 1].start_page_index - 1 : sectionEnd;
    const rawEnd = Math.min(candidateEnd, blockEnd);
    const end = Math.max(rawEnd, start);

    // Find the canonical citation. The bracketed running-head cite on
    // opinion pages (e.g. `[30 NY3d 59] 59`) is anchored to the case's
    // *start* volume page directly — so for opinion cases we use it
    // verbatim. For memoranda, only `<vol-page> 30 NEWYORK REPORTS` and
    // `MEMORANDA <vol-page>` running heads are present, and they're
    // anchored to the page they appear on, not the case start. We back
    // them off by `(observed_p - start_p)` to derive the start vol-page.
    // This assumes 1:1 PDF↔volume pagination within the case range, which
    // is true since section banners only appear at section boundaries.
    let volumePage = null;
    let citationVolume = volumeMeta?.volume ?? null;

    for (const p of pages) {
      if (p.page_index < start || p.page_index > end) continue;
      const hit = detectRunHeadCite(p, runRe);
      if (hit) {
        volumePage = hit.volume_page;       // bracketed cite gives start directly
        citationVolume = hit.volume;
        break;
      }
    }
    if (volumePage === null) {
      // Anchor on start_page, not cite_page. The official reporter citation
      // points at the page where the memo prefix is — i.e., where the
      // case's first line appears — not where the parallel cite header
      // lands. So the consolidated case at 172 AD3d 520 is cited as 520
      // (where its memo prefix "1 Francesco Bellucia..." sits) and 158 AD3d
      // 919 / Seck is cited as 919 (where prefix "17" sits), even though
      // the parallel cite headers for both are on the next page.
      const lookaheadEnd = Math.min(end + 2, sectionEnd);
      for (const p of pages) {
        if (p.page_index < start || p.page_index > lookaheadEnd) continue;
        const head = firstNonEmptyLine(p);
        const memo = head.match(MEMO_RUNHEAD);
        if (memo) {
          volumePage = parseInt(memo[1], 10) - (p.page_index - start);
          break;
        }
        const left = head.match(LEFT_RUNHEAD);
        if (left) {
          volumePage = parseInt(left[1], 10) - (p.page_index - start);
          break;
        }
      }
    }

    const citation = (citationVolume && volumePage)
      ? `${citationVolume} ${reporter} ${volumePage}`
      : null;

    // Body slice bounds (used by opinions.js for memo text extraction):
    //   - text starts right after `]—` on the *cite page* (which may be
    //     start_page_index or one page later for off-by-one cases)
    //   - text ends just before the next memo's `[cite]` on its cite page
    //     (or end-of-text on the last page if this is the last memo)
    const textStartOffset = firstPages[i].end_offset;
    const next = i + 1 < firstPages.length ? firstPages[i + 1] : null;
    const textEndPage   = next ? next.page_index : end;
    const textEndOffset = next ? next.offset : null;  // null = run to end of page

    // Capture the raw caption text — everything between the memo-number
    // prefix (start_offset on start_page_index) and the `[<NYS3d>]` cite
    // (cite_offset on cite_page_index). For memos this gives us the party
    // names that the body text alone doesn't carry, which is what the
    // ToC fuzzy-match needs to disambiguate cases sharing a vol_page.
    let captionRaw = extractCaptionRaw(
      pages,
      start, firstPages[i].start_offset,
      citePage, firstPages[i].offset
    );

    // Defense in depth: strip any leading memo-number digit on memoranda
    // captions. extractCaptionRaw already strips when its prefix-detection
    // anchored on the start_offset; this catches secondary detections of
    // the same case where start_offset lands past the digit but the
    // recombined text still carries it at the head.
    if (section === 'memoranda' && captionRaw) {
      captionRaw = captionRaw.replace(/^\s*\d{1,3}\s+(?=[A-Z]|\d)/, '');
    }

    // NY3d motion-decision fallback. Regular extraction returns empty when
    // start_offset == cite_offset (no AD3d-style memo prefix matched, which
    // is normal for NY3d). For motion-calendar entries — short cases laid
    // out as <caption>/<date>/<ruling>/[<cite>] — backward-scan from the
    // cite to recover the caption block. The two-cite form (NE3d + NYS3d)
    // is the NY3d signal; AD3d/Misc 3d use one cite and have their own
    // memo-prefix path above.
    if (!captionRaw && firstPages[i].parallel_cites.length === 2) {
      const citePageRecord = pages.find(p => p.page_index === citePage);
      if (citePageRecord) {
        const motionCap = extractNyMotionCaption(
          citePageRecord.text_raw || '',
          firstPages[i].offset
        );
        if (motionCap) captionRaw = motionCap;
      }
    }

    cases.push({
      start_page_index: start,
      end_page_index: end,
      cite_page_index: citePage,         // page where `[cite]` is (≥ start_page_index)
      citation,
      volume_page: volumePage,
      parallel_cites: firstPages[i].parallel_cites,
      cite_offset: firstPages[i].offset,
      caption_raw: captionRaw,
      text_start_offset: textStartOffset,
      text_end_page_index: textEndPage,
      text_end_offset: textEndOffset,
      section,
    });
  }
  return cases;
}

/**
 * NY3d motion-decision caption extraction.
 *
 * Regular NY3d opinions and memoranda lay out as:
 *
 *   [<NE3d>, <NYS3d>]    ← cite header at TOP
 *   <caption>
 *   Argued/Submitted X; decided Y
 *   SUMMARY / HEADNOTES / APPEARANCES OF COUNSEL
 *   <body>
 *
 * NY3d motion-calendar entries (denials of leave to appeal, motions for
 * reconsideration, stay dismissals) use the OPPOSITE order:
 *
 *   <previous case ends with "denied/granted/dismissed.">
 *   <caption>           ← party names with role markers
 *   Submitted X; decided Y    OR   Decided Y
 *   Reported below, ... AD3d ...
 *   <short motion ruling>
 *   [<NE3d>, <NYS3d>]   ← cite header at END
 *
 * `extractCaseHeader` is built for the regular layout (cite-first) and
 * returns null `caption_text` for motion-calendar entries because the date
 * line falls before the parallel cite, breaking its
 * `dateIdx > parallelIdx` precondition.
 *
 * This helper handles the motion-calendar layout: walk backward from the
 * cite header to find the date line, then walk further backward to find
 * the previous case's ruling-end (a non-caption line ending with a
 * period). The lines in between are this case's caption.
 */
const NY_MOTION_DATE_RE =
  /(?:^|\n)\s*(?:Submitted[\s\S]{0,100}?;\s*decided|Decided)\s+[A-Z][a-z]+\.?\s+\d{1,2}\s*,?\s+\d{4}/g;
const ROLE_MARKER_END_RE =
  /\b(?:Appellants?|Respondents?|Plaintiffs?|Defendants?|Petitioners?)\b\.?\s*$/;
const RUNNING_HEAD_RE = /^(?:MEMORANDA|MOTION\s+DECISIONS?|MOTIONS|MEMORANDUM)\s+\d+\s*$/i;

// Page-top running heads that appear on every continuation page of a case.
// Detected so we can strip them out of caption_raw — they're not part of any
// caption, just typesetter chrome that pdfplumber returns inline with body
// text. Three variants observed across the three reporters:
//
//   1. Left/verso page (always):
//      `<page> <vol> APPELLATE DIVISION REPORTS, 3d SERIES`
//      `<page> <vol> NEW YORK REPORTS, 3d SERIES`
//      `<page> <vol> MISCELLANEOUS REPORTS, 3d SERIES`
//
//   2. Right/recto page in memoranda sections (AD3d):
//      `MEMORANDA, <Dept>, <Month>, <Year> <page>`
//
//   3. Right/recto page in opinion sections (NY3d, Misc 3d):
//      `<NAME-IN-CAPS> [<vol> <reporter> <vol-page>] <page>`
//
// The third form starts with a case name and is risky to strip with a single
// regex (could clip real caption text); we keep that variant page-specific
// and only strip variants 1 and 2 from caption_raw. The bracketed cite in
// variant 3 is its own first-line marker so it never lands inside a caption.
const PAGE_RUNHEAD_RE = new RegExp(
  '^(?:' +
    // Variant 1: left-page running head
    '\\d+\\s+\\d+\\s*(?:APPELLATE\\s+DIVISION|NEW\\s*YORK|MISCELLANEOUS)\\s+REPORTS,?\\s+3d\\s+SERIES' +
    '|' +
    // Variant 2: right-page memoranda running head
    'MEMORANDA(?:[,\\s][^\\n]*?)?\\s+\\d+' +
  ')\\s*$',
  'i'
);

function stripPageRunheads(text) {
  if (!text) return text;
  return text.split('\n').filter(l => !PAGE_RUNHEAD_RE.test(l.trim())).join('\n');
}

// Patterns that signal a line is NOT part of a caption (previous case's
// ruling text, concur block, court order). When we hit one walking backward,
// the caption ends and we stop. Each pattern uses word-boundaries so the
// match has to be a discrete keyword rather than embedded.
const NON_CAPTION_RE = /\b(?:concur|denied|granted|dismissed|affirmed|reversed|withdrawn|adjudged|taking\s+no\s+part|sua\s+sponte|Ordered\s+that|Judgment\s+entered|in\s+a\s+memorandum)\b/i;

// "In the Matter of" is the canonical opener for guardianship / disciplinary
// / probate captions where the case isn't a v-style party dispute. These
// captions end with a descriptive noun phrase (e.g., "..., a Justice of the
// Monroe Town Court, Orange County.") rather than a role marker, so they
// need their own caption-start signal.
const MATTER_OF_RE = /^In\s+the\s+Matter\s+of\b/i;

function isLikelyCaptionLine(line) {
  const l = line.trim();
  if (!l) return false;
  if (MATTER_OF_RE.test(l)) return true;
  if (ROLE_MARKER_END_RE.test(l)) return true;
  // All-caps line (small-caps body fragment of a name): "OHN ENRY" etc.
  if (/^[A-Z][A-Z\s.\-,'’()&]*$/.test(l) && !/[a-z]/.test(l)) return true;
  // Mixed-case line with commas — looks party-name-like — but reject lines
  // that contain ruling/concur keywords.
  if (/^[A-Z]/.test(l) && /,/.test(l) && !NON_CAPTION_RE.test(l)) return true;
  return false;
}

function extractNyMotionCaption(text, citeOffset) {
  const before = text.slice(0, citeOffset);
  // Find the LAST date line that comes before the cite — that's the date
  // line for the case the cite belongs to. Earlier date lines belong to
  // motions that share the same cite-block bundle (rare, but observed).
  let lastDate = null;
  let m;
  NY_MOTION_DATE_RE.lastIndex = 0;
  while ((m = NY_MOTION_DATE_RE.exec(before)) !== null) lastDate = m;
  if (!lastDate) return null;

  // The caption ends at the start of the date line. Walk backward,
  // including lines that look caption-shaped (Matter-of openers, role
  // markers, small-caps fragments, party-name-with-comma lines), and
  // stopping when we hit a definite non-caption line (ruling text,
  // concur block, running head).
  const dateStart = lastDate.index;
  const captionRegion = before.slice(0, dateStart);
  const lines = captionRegion.split('\n');

  let captionStart = lines.length;
  let sawDefiniteCaptionLine = false;
  for (let i = lines.length - 1; i >= 0; i--) {
    const l = lines[i].trim();
    if (!l) {
      // Blank lines: accept if we've already started building the caption
      if (captionStart < lines.length) captionStart = i;
      continue;
    }
    if (NON_CAPTION_RE.test(l) || RUNNING_HEAD_RE.test(l)) break;
    if (isLikelyCaptionLine(l)) {
      captionStart = i;
      if (MATTER_OF_RE.test(l) || ROLE_MARKER_END_RE.test(l)) {
        sawDefiniteCaptionLine = true;
      }
      continue;
    }
    // Line not caption-shaped and not a hard non-caption marker — stop here.
    break;
  }

  if (!sawDefiniteCaptionLine || captionStart >= lines.length) return null;

  let captionLines = lines.slice(captionStart);
  while (captionLines.length && (
    !captionLines[0].trim() ||
    RUNNING_HEAD_RE.test(captionLines[0].trim())
  )) {
    captionLines.shift();
  }
  const captionText = captionLines.join('\n').trim();
  if (!captionText) return null;
  return captionText;
}

/**
 * Cluster a (already-recombined) word array into visual lines. Each line
 * record carries `top`, `text`, and the underlying `words` array (for the
 * cross-row body splice pass that follows). Lines are sorted top-to-bottom;
 * words within a line are sorted left-to-right. Two words belong to the
 * same line if their `top` values differ by less than 2.5pt.
 */
function recombinedLines(words) {
  if (!words.length) return [];
  const sorted = words.slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
  const lines = [];
  let cur = [];
  let curTop = null;
  const flush = () => {
    if (!cur.length) return;
    const ws = cur.slice().sort((a, b) => a.x0 - b.x0);
    lines.push({ top: curTop, text: ws.map(w => w.text).join(' '), words: ws });
  };
  for (const w of sorted) {
    if (curTop === null || Math.abs(w.top - curTop) <= 2.5) {
      cur.push(w);
      if (curTop === null) curTop = w.top;
    } else {
      flush();
      cur = [w];
      curTop = w.top;
    }
  }
  flush();
  return lines;
}

// Body word size cap used by the orphan-row splice. Small-caps body
// fragments render at 6.98pt; section banners (SUMMARY, HEADNOTE,
// APPEARANCES OF COUNSEL, …) render at 8.98pt. Both are all-caps but only
// the 6.98pt fragments are wrap continuation candidates. Threshold of 8
// cleanly separates them. Bumping back to 9 (the lead-cap floor) would
// false-positive on banners, splicing "summary" into the previous case's
// hyphenated word.
const ORPHAN_BODY_MAX_SIZE = 8;

/**
 * Second pass over the recombined line records: detect rows whose words are
 * ALL small-caps body fragments (i.e., the per-cap recombiner couldn't pair
 * them with anything on their own row), and splice them into the most
 * recent prior line whose last word ends with a hyphen — the wrapped lead
 * they continue.
 *
 * Empirical patterns this resolves (from validator survey):
 *
 *   "...v Purchase Col-"           ← row K with hyphenated lead
 *   "LEGE OF THE"                  ← row K+2 (or K+1) all body fragments
 *   "State University ... Respon-" ← row K+1 leads
 *
 * The first body fragment ("LEGE") continues the wrapped word with no
 * space ("Col-" + "lege" → "College"); subsequent fragments get a space
 * before each ("College" + " of" + " the"). The orphan row is removed.
 *
 * Lookback is bounded to 3 prior lines: typical caption layouts have at
 * most one intervening leads-row between the wrapped lead and its body
 * continuation. A wider window would risk attaching footnote / headnote
 * fragments to unrelated headers.
 */
export function spliceOrphanBodyRows(lines) {
  const out = [];
  for (const line of lines) {
    const allBody = line.words.length > 0
      && line.words.every(w => w.size < ORPHAN_BODY_MAX_SIZE && /^[A-Z]/.test(w.text));
    if (!allBody) {
      out.push(line);
      continue;
    }
    // Find the orphan's host caps row — the line directly above with a
    // small baseline gap (< 6pt). The orphan and its host form one visual
    // row pair; the splice TARGET (the wrap source) lives one row pair
    // earlier.
    let hostIdx = -1;
    if (out.length > 0 && (line.top - out[out.length - 1].top) < 6) {
      hostIdx = out.length - 1;
    }

    // Determine whether the orphan fragments sit AT THE LEFT of the host
    // caps row (i.e., before the host's first lead cap). When true, the
    // orphan is wrap continuation from the previous row pair — even if
    // the previous row's last word is a non-hyphenated recombined small-
    // caps word (e.g., `Committee` wrapping to `of the` on the next row).
    let orphanAtLeft = false;
    if (hostIdx >= 0) {
      const host = out[hostIdx];
      const firstHostCap = host.words.find(w => w.size >= 9);
      if (firstHostCap && line.words[0].x0 < firstHostCap.x0) {
        orphanAtLeft = true;
      }
    }

    // Find the splice target. Hyphenated leads (`Depart-`) are the
    // strongest signal and always splice. Non-hyphenated recombined words
    // splice only when the orphan is at the left of its host (the more
    // conservative case to avoid attaching unrelated orphans). Plain text
    // hyphenated wraps like `Peti-` continuing as `tioner` are NOT
    // targets — `_recombined` distinguishes small-caps merges from
    // ordinary soft-hyphen line wraps.
    let target = null;
    const startK = (hostIdx >= 0 ? hostIdx - 1 : out.length - 1);
    for (let k = startK; k >= 0 && k >= startK - 3; k--) {
      const prev = out[k];
      const lastW = prev.words[prev.words.length - 1];
      if (!lastW || !lastW._recombined) continue;
      const hyphenated = /-$/.test(lastW.text);
      if (hyphenated || orphanAtLeft) {
        target = { line: prev, lastWord: lastW, hyphenated };
        break;
      }
    }
    // Partition the orphan row into contiguous groups by an 8pt gap. Each
    // group is a separate piece of small-caps body text that the per-cap
    // recombiner couldn't pair. A row like Utica's `ING IN OF THE` splits
    // into `[ING, IN]` and `[OF, THE]`: the first group is wrap continuation
    // from row N-1's `Proceed-`, the second sits between two leads on the
    // host row and prepends to the lead on its right (`Real` → `of the Real`).
    const TAKE_GAP = 8;
    const groups = [[line.words[0]]];
    for (let k = 1; k < line.words.length; k++) {
      const gap = line.words[k].x0 - line.words[k - 1].x1;
      if (gap > TAKE_GAP) groups.push([]);
      groups[groups.length - 1].push(line.words[k]);
    }

    const leftover = [];
    if (target) {
      // Group 0 splices into the wrap target.
      //   - Hyphenated target: strip trailing hyphen; first fragment glues
      //     directly to continue the wrapped word (`Depart` + `ment`).
      //   - Non-hyphenated target: first fragment starts a separate word
      //     with a leading space (`Committee` + ` of`).
      const taken = groups[0];
      let merged = target.hyphenated
        ? target.lastWord.text.slice(0, -1)
        : target.lastWord.text;
      for (let k = 0; k < taken.length; k++) {
        const frag = taken[k].text.toLowerCase();
        if (k === 0 && target.hyphenated) {
          merged += frag;
        } else {
          merged += ' ' + frag;
        }
      }
      target.lastWord.text = merged;
      target.line.text = target.line.words.map(w => w.text).join(' ');
    } else {
      // No splice target — keep group 0 as leftover.
      leftover.push(...groups[0]);
    }

    // Subsequent groups: prepend to the next recombined lead to the right
    // on the orphan's host row pair. This handles intra-row orphans like
    // `OF THE` between Article 11 and Real (Real becomes "of the Real").
    for (let g = 1; g < groups.length; g++) {
      const grp = groups[g];
      const grpLeft = grp[0].x0;
      // The host row pair includes the orphan and its host caps row.
      // We look for a recombined lead whose x0 is just to the right of
      // the orphan group, on either the host caps row OR the orphan row's
      // top range (within ~6pt of either).
      let nextLead = null;
      if (hostIdx >= 0) {
        const host = out[hostIdx];
        const candidates = host.words
          .filter(w => w._recombined && w.x0 > grpLeft)
          .sort((a, b) => a.x0 - b.x0);
        if (candidates.length) nextLead = candidates[0];
      }
      if (nextLead) {
        let merged = '';
        for (let k = 0; k < grp.length; k++) {
          merged += (k > 0 ? ' ' : '') + grp[k].text.toLowerCase();
        }
        nextLead.text = merged + ' ' + nextLead.text;
        out[hostIdx].text = out[hostIdx].words.map(w => w.text).join(' ');
      } else {
        leftover.push(...grp);
      }
    }

    if (leftover.length) {
      out.push({
        top: line.top,
        x0: line.x0,
        x1: line.x1,
        size: line.size,
        text: leftover.map(w => w.text).join(' '),
        words: leftover,
      });
    }
  }
  return out;
}

/**
 * Pull the raw caption text between a memo-number prefix and its `[cite]`.
 * Walks pages between `startPage` and `citePage` (inclusive), slicing each
 * appropriately. Used as fuzzy-match context for ToC name disambiguation
 * on multi-case-per-page citations.
 *
 * Caption text is built from the page's word stream after running the
 * small-caps recombiner — NOT from `text_raw`. The recombiner merges lead
 * caps with their small-caps body fragments and inserts spaces at compound
 * word boundaries, so "T P S N Y" + "HE EOPLE OF THE TATE OF EW ORK"
 * collapses to "The People of the State of New York" instead of leaving
 * the body row stranded as `OF THE OF` orphans.
 *
 * For boundary pages (start / cite), we re-locate the memo prefix and the
 * parallel cite within the recombined line stream rather than translating
 * text_raw byte offsets — those offsets become meaningless once small-caps
 * lines collapse into their lead-cap lines.
 */
function extractCaptionRaw(pages, startPage, startOffset, citePage, citeOffset) {
  // Identify the prefix digit by reading text_raw at startOffset. AD3d
  // memo-section cases always have one; opinion-section cases may not.
  let prefixDigit = null;
  const startPageRec = pages.find(p => p.page_index === startPage);
  if (startPageRec) {
    const raw = startPageRec.text_raw || '';
    const m = raw.slice(startOffset).match(/^\s*(\d+)\b/);
    if (m) prefixDigit = m[1];
  }

  // Build recombined text for each page in the range.
  const parts = [];
  for (const p of pages) {
    if (p.page_index < startPage || p.page_index > citePage) continue;
    const recombined = recombineWords(p.words || []);
    const lines = spliceOrphanBodyRows(recombinedLines(recombined));
    parts.push(lines.map(l => l.text).join('\n'));
  }
  let allText = parts.join('\n');

  // Slice from the memo-prefix line. Match `<digit><space><opener>` at line
  // start. The opener `(?:[A-Z][a-z]|...)` requires a real caption word,
  // not a body cite like `1 NY3d 100` (which has uppercase-only `NY` after
  // the digit). After locating the prefix line, drop the prefix digit
  // itself — it's the memo's stack position within the day, not part of
  // the caption.
  if (prefixDigit) {
    const re = new RegExp(
      '(?:^|\\n)\\s*' + prefixDigit +
      '\\s+(?:In\\b|The\\b|A\\b|An\\b|Matter\\b|People\\b|Application\\b|Petition\\b|Appeal\\b|Order\\b|Estate\\b|Claim\\b|[A-Z][a-z])'
    );
    const m = re.exec(allText);
    if (m) {
      const lineStart = allText.lastIndexOf('\n', m.index) + 1;
      allText = allText.slice(lineStart).replace(/^\s*\d+\s+/, '');
    }
  }

  // Truncate at the parallel cite. `\s` matches `\n` so cites that wrap
  // across visual lines (e.g. `[66 NYS3d\n124]`) still match.
  const citeRe = /\[\s*\d+\s*NYS3d\s*\d+\s*\]|\[\s*\d+\s*NE3d\s*\d+\s*,\s*\d+\s*NYS3d\s*\d+\s*\]/;
  const citeMatch = citeRe.exec(allText);
  if (citeMatch) {
    allText = allText.slice(0, citeMatch.index);
  }

  return stripPageRunheads(allText).trim();
}

/**
 * Top-level: detect case ranges across both OPINIONS and MEMORANDA sections.
 * `pages` must be sorted by page_index and `classification` parallel to it.
 */
export function detectCaseBoundaries(pages, classification, volumeMeta) {
  const opinions  = walkSection(pages, classification, 'opinions', volumeMeta);
  const memoranda = walkSection(pages, classification, 'memoranda', volumeMeta);
  return [...opinions, ...memoranda];
}

/**
 * Extract the case-name portion of the right-side running head from any
 * continuation page in the case's range. Format:
 *   `<NAME-IN-SMALL-CAPS> [<vol> <reporter> <pg>] <vol-page>`
 *
 * Pulled from the recombined words (small_caps.js), so party names come back
 * in mixed case (e.g. "Myers v Schneiderman" not "MYERS v SCHNEIDERMAN").
 * Caveats:
 *  - Running heads use abbreviated forms ("MTR" for "Matter of", abbreviated
 *    party names). Treat as a *short* cite, not a full case name.
 *  - The first page of a case has no bracketed running head, so we walk
 *    pages start+1 .. end looking for the first match.
 *  - Words right at the running-head baseline (top=116.7 in 30 NY3d) form
 *    the heading line; `tolerance` keeps cap+small-cap fragments together.
 */
import { recombineWords } from './small_caps.js';

const RUNHEAD_TOP_TOLERANCE = 4.0;

export function extractRunningHeadName(pages, caseRange, reporter) {
  const runRe = runHeadCiteRegex(reporter);
  for (const p of pages) {
    if (p.page_index <= caseRange.start_page_index) continue;
    if (p.page_index > caseRange.end_page_index) break;

    // Identify the running-head line by its top-most words.
    const words = (p.words || []).slice();
    if (!words.length) continue;
    const headTop = Math.min(...words.map(w => w.top));
    const headWords = words.filter(w => w.top - headTop <= RUNHEAD_TOP_TOLERANCE);

    // Verify this is a right-side running head (must contain the bracketed cite).
    const joinedRaw = headWords.sort((a, b) => a.x0 - b.x0).map(w => w.text).join(' ');
    if (!runRe.test(joinedRaw)) continue;

    // Recombine small caps then slice off everything from `[` onward.
    const recombined = recombineWords(headWords).sort((a, b) => a.x0 - b.x0);
    const joined = recombined.map(w => w.text).join(' ');
    const m = joined.match(/^(.+?)\s*\[/);
    if (m) {
      const name = m[1].trim();
      if (name) return name;
    }
  }
  return null;
}
