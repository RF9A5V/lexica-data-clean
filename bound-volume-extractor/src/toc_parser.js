/**
 * Table-of-Cases parser.
 *
 * Each bound volume's `tables` section (pp 6-67 in 30 NY3d, pp 6-29 in 157
 * AD3d, pp 30-55 in 57 Misc 3d) carries a clean alphabetical index of
 * every reported case in the volume:
 *
 *   "Alexander (Yvette), People v—30 NY3d 1113"
 *   "Aponte, Matter of, v Olatoye—30 NY3d 693, 1027"
 *   "Abax Lotus Ltd. v China Mobile Me-diaTech.Inc.—30 NY3d 1090"
 *
 * These names are the canonical reporter-edited form, far cleaner than the
 * abbreviated running-head form ("MTR OF AYRES", "MAKINEN v CITY OF NEW
 * YORK") that we otherwise fall back on for the case `name` field.
 *
 * Layout: every ToC page has a two-column layout. We split words by
 * x-coordinate, reconstruct each column independently, then run a regex
 * scan that captures `<name>—<vol> <reporter> <page-list>`. Multi-page
 * references (a case that appears at multiple vol-pages) get one map
 * entry per page so any case-page lookup returns the same canonical name.
 *
 * Returns:
 *   Map<number, string>  // volume_page → case name
 */

// Entry pattern: `<name>—<vol> <reporter> <page-list>`
//   <name>      = [A-Z][^—]+? (lazy, stops at first em dash)
//   <vol>       = digits
//   <reporter>  = NY3d / AD3d / Misc 3d
//   <page-list> = comma-separated digits (optionally with `(A)` abstract marker)
//
// Whitespace between reporter and page-list is `\s*` (zero or more) because
// pdfplumber sometimes squashes the cite into one token: `NY3d1090` and
// `NY3d 1113` both occur in the same volume's ToC.
const ENTRY_RE = /([A-Z][^—]+?)\s*—\s*(\d+)\s*(NY3d|AD3d|Misc\s*3d)\s*(\d+(?:\(A\))?(?:\s*,\s*\d+(?:\(A\))?)*)/g;

const RUNNING_HEAD_PATTERNS = [
  /^TABLE\s+OF\s+CASES.*$/im,
  /^\[To cite.*$/im,
  /^\[Abstracts of unreported.*$/im,
  /^following the page number\].*$/im,
  /^\d+\s+(?:NEW\s*YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS.*$/im,
  /^[a-z]+\s+\d+\s*(?:NEWYORK|NEW\s+YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS.*$/im,
  /^[ivxlcdm]+\s+\d+\s+(?:NEW\s*YORK|APPELLATE\s+DIVISION|MISCELLANEOUS)\s+REPORTS.*$/im,
];

/**
 * Group words by visual line (using `top` proximity), then join words on
 * each line with single spaces and join lines with `\n`.
 */
function wordsToText(words) {
  if (!words.length) return '';
  const sorted = words.slice().sort((a, b) => (a.top - b.top) || (a.x0 - b.x0));
  const lines = [];
  let cur = [];
  let curTop = null;
  // Within each visual line, re-sort by x0 before joining. The outer sort
  // primary-keys on `top`, so when two segments at slightly different
  // baselines (e.g. cite tail at top=137.54 and name at top=137.61) cluster
  // into one line under the 2.5pt tolerance, the cluster's iteration order
  // reflects their (top, x0) tuple — which puts the lower-top segment first
  // even when it sits to the right visually. ToC lines like
  // `Rosenfeld v Rosenfeld—236 AD3d 478` would then come out reversed as
  // `—236 AD3d 478 Rosenfeld v Rosenfeld`, breaking ENTRY_RE.
  const flushCluster = (c) => c.slice().sort((a, b) => a.x0 - b.x0).map(x => x.text).join(' ');
  for (const w of sorted) {
    if (curTop === null || Math.abs(w.top - curTop) <= 2.5) {
      cur.push(w);
      if (curTop === null) curTop = w.top;
    } else {
      lines.push(flushCluster(cur));
      cur = [w];
      curTop = w.top;
    }
  }
  if (cur.length) lines.push(flushCluster(cur));
  return lines.join('\n');
}

/**
 * Strip running heads, ToC banner, and bracketed citation notes; collapse
 * line wraps (with hyphenation handling) into a single-line stream so the
 * entry regex can match across line boundaries.
 */
function flattenColumn(text) {
  let out = text;
  for (const re of RUNNING_HEAD_PATTERNS) out = out.replace(re, '');
  // Drop bare letter section dividers like "A" / "B" alone on a line.
  out = out.replace(/^[A-Z]\s*$/gm, '');
  // Soft-hyphen wrap: word ending in `-\n` rejoins (lowercase next).
  out = out.replace(/([a-z])-\n([a-z])/g, '$1$2');
  // Compound-word wrap: `-\n` followed by uppercase keeps the hyphen.
  out = out.replace(/([A-Za-z])-\n([A-Z])/g, '$1-$2');
  // Other newlines → space.
  out = out.replace(/\n+/g, ' ');
  out = out.replace(/\s+/g, ' ');
  return out.trim();
}

/**
 * Strip a leading section-divider letter (e.g. "A Alexander…" → "Alexander…")
 * Only strips a SINGLE capital followed by space + capital, since real
 * names like "A.M.P." have a period, and "U.S. Bank" has a period too.
 */
function stripDivider(name) {
  return name.replace(/^([A-Z])\s+(?=[A-Z])/, '');
}

/**
 * pdfplumber sometimes packs ToC name glyphs without inter-word spaces,
 * yielding squashed forms like:
 *   "Andujar(John),Peoplev"          → "Andujar (John), People v"
 *   "Phillips,Weissv"                → "Phillips, Weiss v"
 *   "ClaudiaDowling,Inc.,Peoplev"    → "Claudia Dowling, Inc., People v"
 *   "MatterofSalami"                 → "Matter of Salami"
 *
 * Rules applied (order matters):
 *   1. Stop-word injection: lowercase + (of|v|the|in|et|al|and|on) + Cap
 *      gets a surrounding space pair. This rescues "Matterof" → "Matter of".
 *   2. Punctuation neighbors: `,X`, `)X`, `&X`, `X(`, `X&` get a space.
 *   3. Standalone `v` between letters gets surrounding spaces (case
 *      separator: `LLCvNewYorkers` → `LLC v NewYorkers`).
 *   4. Trailing `v` after a letter gets a leading space (`Peoplev` → `People v`).
 *   5. CamelCase boundary with ≥3 lowercase letters before the Cap gets a
 *      space (`ParkSightseeing` → `Park Sightseeing`). Threshold 3 protects
 *      name prefixes like Mc/Di/De/Mac which only have 1-2 lowercase
 *      letters before the second cap.
 *
 * Known limitation: 2-letter lowercase + Cap (e.g. `NewYorkers`,
 * `NewJersey`) stay squashed; specific tokens could be added to a denylist
 * if needed.
 */
function unsquashName(name) {
  let out = name;
  // Punctuation neighbors first.
  out = out.replace(/,([A-Z])/g, ', $1');
  out = out.replace(/\)([A-Z])/g, ') $1');
  out = out.replace(/&([A-Z])/g, '& $1');
  out = out.replace(/([a-zA-Z])\(/g, '$1 (');
  out = out.replace(/([A-Za-z])&/g, '$1 &');
  // Period-then-Capital with no intervening space: `Educ.Dept.` → `Educ. Dept.`
  // (matches lowercase before period to skip already-spaced "U.S. Bank").
  out = out.replace(/([a-z])\.([A-Z])/g, '$1. $2');
  // Right-side stop-word: `ofNewYork` → `of NewYork`. Includes `on` for
  // patterns like `onJud.` (Commn. on Jud.) but not `et`/`al`/`in` which
  // would over-split real words.
  out = out.replace(/\b(of|the|and|on)([A-Z])/g, '$1 $2');
  // CamelCase boundary, 3+ lowercase before the Cap (skips `Mc`/`Di`/`De`).
  // Run BEFORE left stop-word so squashed runs like "Matterof[Salami]" first
  // become "Matterof Salami", letting the next rule split "Matterof".
  out = out.replace(/([a-z]{3,})([A-Z])/g, '$1 $2');
  // Left-side stop-word: any letter or period before stop word at word
  // boundary → insert space. Catches `Matterof ` / `Co.of Am.` / `Inc.the`.
  // Note: `on` is NOT in this list — it would falsely split `Clarendon`,
  // `Wilson`, etc. ("on" as a word-ending suffix is too common).
  out = out.replace(/([a-zA-Z\.])(of|the|and)(?=[\s\(\),\.&]|$)/g, '$1 $2');
  // `v` separator: `LLCvNew` → `LLC v New`, `P.C.vAmerican` → `P.C. v American`.
  out = out.replace(/([A-Za-z\.])v([A-Z])/g, '$1 v $2');
  out = out.replace(/\s+/g, ' ').trim();
  // Trailing `v` after a letter or period: "Peoplev" → "People v",
  // "P.L.L.C.v" → "P.L.L.C. v".
  out = out.replace(/([A-Za-z\.])v$/, '$1 v');
  return out;
}

function parseColumn(text) {
  const flat = flattenColumn(text);
  const entries = [];
  ENTRY_RE.lastIndex = 0;
  let m;
  while ((m = ENTRY_RE.exec(flat)) !== null) {
    const rawName = m[1].trim();
    const name = unsquashName(stripDivider(rawName));
    if (!name) continue;
    const vol = parseInt(m[2], 10);
    const reporter = m[3].replace(/\s+/g, ' ');
    // Page list: split by comma, strip "(A)" abstract markers, parse first as int.
    const pageList = m[4].split(/\s*,\s*/).map(p => parseInt(p, 10)).filter(p => !isNaN(p));
    for (const page of pageList) {
      entries.push({ name, vol, reporter, page });
    }
  }
  return entries;
}

/**
 * Build the (volume_page → [names]) map for a volume.
 *
 * `pages` is the full pages array; `classification` parallel to it; both
 * from the parser pipeline. Only `tables` section pages are scanned. For
 * each ToC page we split words into left/right columns by x-coordinate,
 * parse each column for entries, and merge into a single map.
 *
 * Multi-page references (`Aponte, Matter of, v Olatoye—30 NY3d 693, 1027`)
 * map all listed pages to the same name. Multiple cases sharing a single
 * page (common in AD3d/Misc 3d, e.g. 157 A.D.3d 417 has 3 cases) yield an
 * array with all candidate names; callers use `pickTocName` with the case's
 * caption to fuzzy-match the right one.
 *
 * Misc 3d ToCs cross-reference each case under both party names (e.g.
 * "Magnani v Cuggino" AND "Cuggino, Magnani v"); deduped here so each
 * canonical case gets one entry per form.
 */
export function buildTocMap(pages, classification, volumeMeta) {
  const map = new Map();
  if (!volumeMeta) return map;
  const expectedReporter = volumeMeta.reporter;
  const expectedVol = volumeMeta.volume;
  const expReporterNoWs = expectedReporter.replace(/\s+/g, '');
  const matches = (entry) =>
    entry.vol === expectedVol &&
    entry.reporter.replace(/\s+/g, '') === expReporterNoWs;
  const push = (page, name) => {
    if (!map.has(page)) map.set(page, []);
    const arr = map.get(page);
    if (!arr.includes(name)) arr.push(name);
  };

  for (let i = 0; i < pages.length; i++) {
    if (classification[i].section !== 'tables') continue;
    const page = pages[i];
    const words = page.words || [];
    if (!words.length) continue;

    const pageWidth = page.width || 612;
    const midX = pageWidth / 2;
    const left  = words.filter(w => w.x0 <  midX);
    const right = words.filter(w => w.x0 >= midX);

    for (const entry of parseColumn(wordsToText(left)))  if (matches(entry)) push(entry.page, entry.name);
    for (const entry of parseColumn(wordsToText(right))) if (matches(entry)) push(entry.page, entry.name);
  }

  return map;
}

/**
 * Pick the best ToC name for a case at a given volume page. When multiple
 * names share the page, score each candidate via two complementary signals:
 *
 *   1. **Token overlap**: shared 3+-char alphanumeric tokens between the
 *      caption text and the candidate name. Works well for opinion-section
 *      cases where `caption_text` has been recombined into mixed-case
 *      proper nouns ("Schneiderman", "Manzanet-Daniels").
 *
 *   2. **Caps-fragment substring**: 3+-letter ALL-CAPS sequences from the
 *      caption's raw text (small-caps body remnants like `CIAME`,
 *      `ONSTRUCTION`, `RADLEY`) checked as substrings of the candidate's
 *      squashed form ("sciameconstructionllc", "bradleyvhwaiiillc").
 *      Works for memos where caption_raw has small-caps splits.
 *
 * Scores from both methods are summed; highest-scoring candidate wins.
 * Ties go to the first candidate (alphabetically-first ToC entry).
 */
export function pickTocName(tocMap, volumePage, caption) {
  const candidates = tocMap.get(volumePage);
  if (!candidates || !candidates.length) return null;
  if (candidates.length === 1) return candidates[0];
  if (!caption) return candidates[0];

  const capTokens = new Set(tokensFor(caption));
  const capsFragments = [...new Set(
    (caption.match(/\b[A-Z]{3,}\b/g) || []).map(f => f.toLowerCase())
  )];

  let best = candidates[0];
  let bestScore = -1;
  for (const cand of candidates) {
    let score = 0;
    // Token overlap.
    for (const t of tokensFor(cand)) if (capTokens.has(t)) score++;
    // Caps-fragment substring against squashed candidate.
    if (capsFragments.length) {
      const candSquashed = cand.toLowerCase().replace(/[^a-z]/g, '');
      for (const frag of capsFragments) {
        if (candSquashed.includes(frag)) score++;
      }
    }
    if (score > bestScore) { bestScore = score; best = cand; }
  }
  return best;
}

const TOKEN_STOPWORDS = new Set([
  'a','an','the','of','in','on','at','to','for','with','by','from','v','vs','and','or',
  'matter','people','state','inc','corp','llc','llp','ltd','co','et','al','jr','sr',
  'plaintiffs','defendant','defendants','plaintiff','respondent','respondents','appellant','appellants',
]);

function tokensFor(s) {
  return (s || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .split(/\s+/)
    .filter(t => t.length >= 3 && !TOKEN_STOPWORDS.has(t));
}
