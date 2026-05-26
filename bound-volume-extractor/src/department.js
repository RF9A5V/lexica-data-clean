/**
 * Appellate Division department resolution.
 *
 * The reporter publishes AD3d cases from all four departments interleaved in
 * one volume. The previous approach inferred each memo's department from
 * "department banner" pages (detectDepartmentBanners in parser.js) — fragile:
 * a missed banner sends a whole page-run to the wrong department.
 *
 * This module re-derives department from the opinion text itself, which is
 * far more reliable (validated at 99.9% on a 61k-case audit):
 *   - the recited lower-court county  -> a deterministic county->department map
 *   - the panel justices              -> a justice->department roster
 *
 * `resolveVolumeDepartments` runs as a post-pass over a parsed volume: it
 * bootstraps a per-volume roster from the cases that recite a county, backs
 * it with the shipped static roster (configs/ad_justice_roster.json), and
 * overrides court_department wherever the text gives a confident answer.
 * Banner / header attribution remains the fallback when the text is silent.
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// County -> AD department. Verified against the NY Board of Law Examiners
// county list (which misspells NY's "Allegany" as "Allegheny" — corrected
// here). Stable for the whole 1896-present range: no county has ever crossed
// a department boundary, so no date-conditional logic is needed.
// ---------------------------------------------------------------------------
export const COUNTY_DEPT = {
  'new york': 1, 'bronx': 1,
  'kings': 2, 'queens': 2, 'richmond': 2, 'nassau': 2, 'suffolk': 2,
  'westchester': 2, 'dutchess': 2, 'orange': 2, 'putnam': 2, 'rockland': 2,
  'albany': 3, 'broome': 3, 'chemung': 3, 'chenango': 3, 'clinton': 3,
  'columbia': 3, 'cortland': 3, 'delaware': 3, 'essex': 3, 'franklin': 3,
  'fulton': 3, 'greene': 3, 'hamilton': 3, 'madison': 3, 'montgomery': 3,
  'otsego': 3, 'rensselaer': 3, 'st. lawrence': 3, 'saratoga': 3,
  'schenectady': 3, 'schoharie': 3, 'schuyler': 3, 'sullivan': 3, 'tioga': 3,
  'tompkins': 3, 'ulster': 3, 'warren': 3, 'washington': 3,
  'allegany': 4, 'cattaraugus': 4, 'cayuga': 4, 'chautauqua': 4, 'erie': 4,
  'genesee': 4, 'herkimer': 4, 'jefferson': 4, 'lewis': 4, 'livingston': 4,
  'monroe': 4, 'niagara': 4, 'oneida': 4, 'onondaga': 4, 'ontario': 4,
  'orleans': 4, 'oswego': 4, 'seneca': 4, 'steuben': 4, 'wayne': 4,
  'wyoming': 4, 'yates': 4,
};

// ---------------------------------------------------------------------------
// County extraction
// ---------------------------------------------------------------------------
const COURT_TYPES =
  '(?:Supreme Court|County Court|Family Court|Surrogate’?s? Court|Surrogate\'?s? Court' +
  '|Court of Claims|City Court|Civil Court|Criminal Court|District Court' +
  '|Justice Court|Town Court|Village Court|Children\'?s? Court)';
const ANCHORED_RE = new RegExp(
  COURT_TYPES + ',?\\s+(?:of\\s+the\\s+(?:State\\s+of\\s+)?New York,?\\s+)?' +
  '([A-Z][A-Za-z.]+(?:\\s[A-Z][A-Za-z.]+){0,2})\\s+County\\b', 'g');
const ANCHORED_OF_RE = new RegExp(
  COURT_TYPES + '[^.]{0,40}?County of\\s+([A-Z][A-Za-z.]+(?:\\s[A-Z][A-Za-z.]+){0,2})\\b', 'g');

function lookupCounty(raw) {
  if (!raw) return null;
  const words = raw.trim().replace(/\s+/g, ' ').split(' ');
  for (const cand of [words.join(' '), words.slice(-2).join(' '), words.slice(-1).join(' ')]) {
    const key = cand.toLowerCase();
    if (key in COUNTY_DEPT) return key;
  }
  return null;
}

/**
 * Resolve the lower-court department from a case's opinion text. Only counts
 * counties attached to a court type ("Supreme Court, Bronx County") — those
 * name the originating court. Returns { dept, counties, conflict }; `dept` is
 * null when no county is found or when the recited counties span >1
 * department (a genuine multi-department consolidation — left to the panel).
 */
export function extractCountyDepartment(text) {
  const counties = new Set();
  let m;
  for (const re of [ANCHORED_RE, ANCHORED_OF_RE]) {
    re.lastIndex = 0;
    while ((m = re.exec(text || ''))) {
      const c = lookupCounty(m[1]);
      if (c) counties.add(c);
    }
  }
  const list = [...counties];
  const depts = new Set(list.map((c) => COUNTY_DEPT[c]));
  if (depts.size === 1) return { dept: [...depts][0], counties: list, conflict: false };
  if (depts.size > 1) return { dept: null, counties: list, conflict: true };
  return { dept: null, counties: list, conflict: false };
}

// ---------------------------------------------------------------------------
// Panel-justice extraction
// ---------------------------------------------------------------------------
const ROLE_RE = /^(?:P\.?\s?J\.?|J\.?\s?P\.?|J\.?\s?E\.?|JJ?\.?|C\.?\s?J\.?)$/i;
const STOP_WORDS = new Set([
  'Ordered', 'Order', 'Judgment', 'Decree', 'Appeal', 'Appeals', 'Decision',
  'Motion', 'Cross', 'Present', 'Concur', 'Memorandum', 'Per', 'Curiam',
  'We', 'The', 'It', 'Defendant', 'Plaintiff', 'Settle', 'In',
]);
function isRole(t) { return ROLE_RE.test(t.replace(/\s+/g, ' ').trim()); }
function looksLikeName(tok) {
  const t = tok.trim();
  if (t.length < 2 || t.length > 24) return false;
  if (!/^[A-Z][A-Za-z'’.\- ]*$/.test(t)) return false;
  if (/\d/.test(t)) return false;
  if (STOP_WORDS.has(t.split(' ')[0])) return false;
  if (/\b(v|vs|see|of|the|and|Court|County)\b/.test(t)) return false;
  if (isRole(t)) return false;
  return true;
}
function tokenize(seg) {
  return seg.split(/\s*,\s*/).flatMap((p) => p.split(/\s+and\s+/))
    .map((t) => t.replace(/^and\s+/i, '').replace(/^[\s;:()]+|[\s;:()]+$/g, '').trim())
    .filter(Boolean);
}
// Bare honorific suffixes and OCR-mangled role markers (e.g. "EJ." for
// "P.J.") that sit between/after surnames — skipped, never counted as names.
const SKIP_TOKENS = new Set(['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'ej.', 'e.j.', 'rj.']);
function isSkippable(tok) { return SKIP_TOKENS.has(tok.trim().toLowerCase()); }

// formA ("Concur — <list>"): walk the list forward from the dash, stopping
// at the closing "JJ." or the first token that isn't a name/role.
function collectNamesForward(tokens) {
  const out = [];
  for (const tok of tokens) {
    if (isRole(tok)) { if (/^JJ\.?$/i.test(tok.trim()) && out.length) break; continue; }
    if (isSkippable(tok)) continue;
    if (looksLikeName(tok)) { out.push(tok); continue; }
    break;
  }
  return out;
}

// formB ("<list>, JJ., concur"): the list has already been isolated down to
// just the justice names — collect every name token.
function namesFromList(seg) {
  const out = [];
  for (const tok of tokenize(seg)) {
    if (isRole(tok) || isSkippable(tok)) continue;
    if (looksLikeName(tok)) out.push(tok);
  }
  return out;
}

/** Extract panel-justice surnames from a case's opinion text. */
export function extractPanel(text) {
  const found = new Set();
  const src = text || '';
  let m;
  const formA = /(?:Concur|Present)\s*[—–:\-]\s*/g;
  while ((m = formA.exec(src))) {
    const win = src.slice(m.index + m[0].length, m.index + m[0].length + 220);
    for (const n of collectNamesForward(tokenize(win))) found.add(n);
  }
  const formB = /,?\s*JJ?\.?,?\s+concurs?\b/gi;
  while ((m = formB.exec(src))) {
    // Window ends just before "…, JJ., concur". The justice list begins
    // after the last sentence/citation boundary — drop everything up to and
    // including it so the first justice (the Presiding Justice, glued to the
    // prior sentence: "…in merit.\nGarry, P.J., …") survives.
    const win = src.slice(Math.max(0, m.index - 240), m.index)
      .replace(/^[\s\S]*[.\]\)]\s+/, '');
    for (const n of namesFromList(win)) found.add(n);
  }
  const withRe = /\bconcurs?\s+with\s+([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+JJ?\.?/g;
  while ((m = withRe.exec(src))) { if (looksLikeName(m[1])) found.add(m[1].trim()); }
  const dissRe = /\b([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+JJ?\.?,?\s*(?:dissent|concurr)/g;
  while ((m = dissRe.exec(src))) { if (looksLikeName(m[1])) found.add(m[1].trim()); }
  return [...found];
}

// ---------------------------------------------------------------------------
// Justice -> department roster
// ---------------------------------------------------------------------------
/**
 * Build a justice->department roster from records that each carry a known
 * department and a panel. records: [{ dept, panel }] (dept 1-4, falsy to
 * skip). Each panel justice gets one vote for the record's department; a
 * clean surname lands ~1.0 on a single department. Returns
 * { surname: { dept, conf, n } }.
 */
export function buildRoster(records) {
  const votes = {};
  for (const r of records) {
    if (!r.dept) continue;
    for (const j of r.panel) (votes[j] ||= [0, 0, 0, 0, 0])[r.dept]++;
  }
  const roster = {};
  for (const [j, v] of Object.entries(votes)) {
    const total = v[1] + v[2] + v[3] + v[4];
    let best = 1;
    for (let d = 2; d <= 4; d++) if (v[d] > v[best]) best = d;
    roster[j] = { dept: best, conf: v[best] / total, n: total };
  }
  return roster;
}

let _staticRoster = null;
/** Lazily load the shipped static roster (configs/ad_justice_roster.json). */
export function loadStaticRoster() {
  if (_staticRoster) return _staticRoster;
  try {
    const doc = JSON.parse(fs.readFileSync(
      path.join(__dirname, '..', 'configs', 'ad_justice_roster.json'), 'utf8'));
    _staticRoster = doc.justices || {};
  } catch {
    _staticRoster = {};   // absent roster is non-fatal — county signal still works
  }
  return _staticRoster;
}

/** Per-volume observations supersede the static roster for the same surname. */
function mergeRosters(volumeRoster, staticRoster) {
  const merged = { ...staticRoster };
  for (const [j, e] of Object.entries(volumeRoster)) if (e.n >= 3) merged[j] = e;
  return merged;
}

/**
 * Load historical justice rosters from one or more JSON files. Each file has
 * the shape:
 *   { dept: 3, justices: [{ surname, full_name?, depts_served: [{dept,
 *     start_year, end_year}] }, ...] }
 * The returned map keys by surname and merges date-bounded entries from every
 * file, so the same surname can appear with depts_served spanning multiple
 * departments (e.g. a justice who served on both 2nd and 4th). Missing files
 * are silently skipped (treated as empty).
 *
 * Shape:
 *   { surname: [{ dept, start_year, end_year }, ...] }
 */
export function loadHistoricalRoster(paths) {
  const out = {};
  for (const p of paths || []) {
    let doc;
    try {
      if (!fs.existsSync(p)) continue;
      doc = JSON.parse(fs.readFileSync(p, 'utf8'));
    } catch {
      continue;   // malformed / unreadable — treat as empty
    }
    const justices = Array.isArray(doc?.justices) ? doc.justices : [];
    for (const j of justices) {
      if (!j || typeof j.surname !== 'string') continue;
      const ranges = Array.isArray(j.depts_served) ? j.depts_served : [];
      for (const r of ranges) {
        if (!r || typeof r.dept !== 'number') continue;
        const entry = {
          dept: r.dept,
          start_year: typeof r.start_year === 'number' ? r.start_year : null,
          end_year: typeof r.end_year === 'number' ? r.end_year : null,
        };
        (out[j.surname] ||= []).push(entry);
      }
    }
  }
  return out;
}

// The panel extractor preserves generational suffixes as part of the surname
// token ("Crew III", "Yesawich Jr."), but historical-roster surnames are
// suffix-stripped by convention ("Crew", "Yesawich"). Try both forms.
const SUFFIX_RE = /\s+(?:Jr|Sr|II|III|IV)\.?$/i;

/**
 * Look up a justice in the historical roster for a given decision year.
 * Returns { dept } if a date-bounded entry matches, else null. A null `year`
 * always returns null — date-bounded lookup requires a year.
 *
 * Range semantics: inclusive on both ends. A missing start_year is treated
 * as -Infinity; a missing end_year as +Infinity (justice still sitting).
 *
 * Falls back to a suffix-stripped variant of the surname to bridge the gap
 * between the extractor (keeps " Jr." / " III") and the roster (drops them).
 */
export function lookupHistorical(surname, year, historicalRoster) {
  if (!historicalRoster || !surname || year == null) return null;
  const tryKey = (key) => {
    const entries = historicalRoster[key];
    if (!entries || !entries.length) return null;
    for (const e of entries) {
      const lo = e.start_year == null ? -Infinity : e.start_year;
      const hi = e.end_year == null ? Infinity : e.end_year;
      if (year >= lo && year <= hi) return { dept: e.dept };
    }
    return null;
  };
  return tryKey(surname) || (SUFFIX_RE.test(surname) ? tryKey(surname.replace(SUFFIX_RE, '')) : null);
}

/**
 * Majority department among panel justices that have a confident roster entry.
 *
 * `opts.historicalRoster` + `opts.year` (both optional): when present, each
 * surname is first looked up in the historical roster. A historical match
 * counts as a confident vote (acts as n=999, conf=1.0); otherwise we fall
 * back to the bootstrap/static roster check (n>=3, conf>=0.8).
 *
 * Backward compat: if `opts` is missing / empty, behavior is byte-identical
 * to the previous two-arg signature.
 */
export function panelVoteDepartment(panel, roster, opts = {}) {
  const { historicalRoster = null, year = null } = opts || {};
  const tally = [0, 0, 0, 0, 0];
  let used = 0, historicalHits = 0;
  for (const j of panel) {
    const hist = historicalRoster ? lookupHistorical(j, year, historicalRoster) : null;
    if (hist) { tally[hist.dept]++; used++; historicalHits++; continue; }
    const e = roster[j];
    if (e && e.n >= 3 && e.conf >= 0.8) { tally[e.dept]++; used++; }
  }
  if (!used) return { dept: null, agree: false, used: 0, historicalHits };
  let best = 1;
  for (let d = 2; d <= 4; d++) if (tally[d] > tally[best]) best = d;
  return { dept: best, agree: tally[best] === used, used, historicalHits };
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------
export function resolveOne(cty, panel, roster, priorDept, opts = {}) {
  const countyDept = cty.conflict ? null : cty.dept;
  const pv = panelVoteDepartment(panel, roster, opts);
  const strongPanel = pv.used >= 3 && pv.agree ? pv.dept : null;
  let department = null, source = null, conflict = false;
  if (countyDept && strongPanel) {
    department = countyDept;
    if (countyDept === strongPanel) source = 'county+panel';
    else { source = 'county'; conflict = true; }
  } else if (countyDept) { department = countyDept; source = 'county'; }
  else if (strongPanel) { department = strongPanel; source = 'panel'; }
  else if (pv.dept) { department = pv.dept; source = 'panel-weak'; }
  else { department = priorDept ?? null; source = priorDept != null ? 'prior' : null; }
  return { department, source, conflict, historicalHits: pv.historicalHits || 0 };
}

/**
 * Post-pass over a parsed AD3d volume: re-derive court_department from the
 * county + panel signals and override the banner/header value where the
 * text gives a confident answer. Mutates each case's `court_department`.
 * No-op for non-AD3d reporters (NY3d / Misc 3d have no departments).
 * Returns an array of warning strings for the volume's warning log.
 */
export function resolveVolumeDepartments(cases, { reporter } = {}) {
  const warnings = [];
  if (reporter !== 'AD3d' || !Array.isArray(cases) || !cases.length) return warnings;

  // pass A — extract county + panel for every case
  const ext = cases.map((c) => {
    const text = (c.opinions || []).map((o) => o.text || '').join('\n');
    return { cty: extractCountyDepartment(text), panel: extractPanel(text) };
  });

  // pass B — bootstrap a per-volume roster from this volume's own
  // county-resolved cases, then back it with the shipped static roster
  const volumeRoster = buildRoster(ext.map((e) => ({
    dept: e.cty.conflict ? null : e.cty.dept, panel: e.panel,
  })));
  const roster = mergeRosters(volumeRoster, loadStaticRoster());

  // pass C — resolve, override, and account
  let resolved = 0, overridden = 0, conflicts = 0;
  cases.forEach((c, i) => {
    const prior = c.court_department ?? null;
    const r = resolveOne(ext[i].cty, ext[i].panel, roster, prior);
    if (r.department == null) return;
    resolved++;
    if (r.conflict) {
      conflicts++;
      warnings.push(`[${c.citation || c.first_page || i}] department signal conflict: ` +
        `county and panel disagree — using county (${r.department})`);
    }
    if (prior != null && r.department !== prior && r.source !== 'prior') {
      overridden++;
      c.court_department = r.department;
    } else if (prior == null) {
      c.court_department = r.department;
    }
  });
  warnings.unshift(
    `department resolution (AD3d): ${resolved}/${cases.length} resolved from text; ` +
    `${overridden} overrode the banner/header value; ${conflicts} county/panel conflict(s); ` +
    `roster ${Object.keys(volumeRoster).length} volume + ${Object.keys(loadStaticRoster()).length} static`);
  return warnings;
}
