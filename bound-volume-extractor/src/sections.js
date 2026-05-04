/**
 * Page-level section classifier.
 *
 * NY official-reports bound volumes have a fixed linear layout:
 *
 *   front_matter → tables → opinions → motions → memoranda → digest → errata
 *
 * Each transition is signalled by a dedicated banner page whose first
 * non-empty line is a known heading WITHOUT an adjacent page number. This
 * distinguishes a banner page (e.g. "MEMORANDA" alone) from an opinion
 * continuation page (running head "MEMORANDA 927"). Once a banner is seen,
 * subsequent pages stay in that section until the next banner.
 *
 * Signals observed in 30 NY3d (verified against output/30NY3d.raw.ndjson):
 *
 *   tables    p6     "TABLE OF CASES REPORTED"
 *   opinions  p68    "CASES DECIDED" (followed by "IN THE COURT OF APPEALS …")
 *   motions   p830   "MOTIONS FOR LEAVE TO APPEAL"
 *   memoranda p844   "MEMORANDA"  ← bare; running heads "MEMORANDA <n>" are NOT
 *   digest    p1046  "DIGEST-INDEX"
 *   errata    ~p1090 "Errata" or "CUMULATIVE ERRATA TABLE"
 *
 * For AD3d / Misc 3d the banners may differ; the BANNER table is the place
 * to extend. The classifier ignores empty pages (blank versos) by inheriting
 * the prior section.
 */

const BANNERS = [
  { section: 'tables',    re: /^TABLE\s+OF\s+CASES\s+REPORTED$/i },
  { section: 'tables',    re: /^TABLE\s+OF\s+CASES\s+AFFECTED$/i },
  { section: 'opinions',  re: /^CASES\s+DECIDED$/i },
  { section: 'opinions',  re: /^SELECTED\s+CASES\s+DECIDED$/i },  // Misc 3d
  { section: 'motions',   re: /^MOTIONS\s+FOR\s+LEAVE\s+TO\s+APPEAL$/i },
  { section: 'memoranda', re: /^MEMORANDA$/i },
  { section: 'abstracts', re: /^Abstracts\s+of\s+Other\s+Court\s+Cases(\s+Selected\s+for)?$/i }, // Misc 3d unreported (A)-cites; banner is 2-line, first line ends "...Selected for"
  { section: 'digest',    re: /^DIGEST[-\s]INDEX$/i },
  { section: 'digest',    re: /^TABLE\s+OF\s+STATUTES(\s+CONSTRUED)?$/i },
  { section: 'errata',    re: /^(CUMULATIVE\s+)?ERRATA(\s+TABLE)?$/i },
];

/**
 * Pull the first N non-empty lines from a page record. We use `text_raw`
 * (not `text_layout`) since pdfplumber's layout text inserts extra spaces
 * that break exact heading matches. Word arrays are even cleaner but
 * slower; for banner detection text_raw is good enough.
 */
function firstNonEmptyLines(page, n = 5) {
  const text = page.text_raw || '';
  const lines = [];
  for (const raw of text.split('\n')) {
    const t = raw.trim();
    if (!t) continue;
    lines.push(t);
    if (lines.length >= n) break;
  }
  return lines;
}

/**
 * Detect a banner heading on this page. Returns the section name or null.
 * A banner is the FIRST non-empty line and matches a known regex exactly.
 * Running heads like "MEMORANDA 927" are rejected by the `^…$` anchors.
 */
function detectBanner(page) {
  const lines = firstNonEmptyLines(page, 3);
  if (!lines.length) return null;
  const head = lines[0];
  for (const { section, re } of BANNERS) {
    if (re.test(head)) return section;
  }
  return null;
}

/**
 * Classify every page in the volume. Returns a parallel array
 * [{ page_index, section, banner }] where `banner` is true on the page that
 * triggered the section change.
 */
export function classifyPages(pages) {
  const result = [];
  let current = 'front_matter';
  for (const page of pages) {
    const banner = detectBanner(page);
    if (banner && banner !== current) {
      current = banner;
      result.push({ page_index: page.page_index, section: current, banner: true });
    } else {
      result.push({ page_index: page.page_index, section: current, banner: false });
    }
  }
  return result;
}

/**
 * Convenience: collapse a per-page classification into [{section, start, end}]
 * ranges, mirroring the section state machine.
 */
export function sectionRanges(classification) {
  const ranges = [];
  for (const { page_index, section } of classification) {
    const last = ranges[ranges.length - 1];
    if (last && last.section === section) {
      last.end = page_index;
    } else {
      ranges.push({ section, start: page_index, end: page_index });
    }
  }
  return ranges;
}
