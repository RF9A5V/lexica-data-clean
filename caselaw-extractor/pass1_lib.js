/**
 * Pass-1 extraction library for Appellate Division opinion text.
 *
 * Pure (no DB) functions shared by the prototype and the PDF-department audit:
 *   - extractCounties / deptOfCounties  — lower-court county -> AD department
 *   - extractPanel                      — panel-justice surnames
 *   - extractAuthor                     — signed-opinion author
 *   - buildRoster / panelVoteDept       — justice -> department roster + vote
 *
 * County->department map verified against the NY Board of Law Examiners county
 * list (which misspells NY's "Allegany" as "Allegheny" — corrected here).
 * Stable for the whole 1896-present range.
 */

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

// --- county ----------------------------------------------------------------
const COURT_TYPES =
  '(?:Supreme Court|County Court|Family Court|Surrogate’?s? Court|Surrogate\'?s? Court' +
  '|Court of Claims|City Court|Civil Court|Criminal Court|District Court' +
  '|Justice Court|Town Court|Village Court|Children\'?s? Court)';
const ANCHORED_RE = new RegExp(
  COURT_TYPES + ',?\\s+(?:of\\s+the\\s+(?:State\\s+of\\s+)?New York,?\\s+)?' +
  '([A-Z][A-Za-z.]+(?:\\s[A-Z][A-Za-z.]+){0,2})\\s+County\\b', 'g');
const ANCHORED_OF_RE = new RegExp(
  COURT_TYPES + '[^.]{0,40}?County of\\s+([A-Z][A-Za-z.]+(?:\\s[A-Z][A-Za-z.]+){0,2})\\b', 'g');
const BARE_RE = /\b([A-Z][A-Za-z.]+(?:\s[A-Z][A-Za-z.]+){0,2})\s+County\b/g;

function lookupCounty(raw) {
  if (!raw) return null;
  const words = raw.trim().replace(/\s+/g, ' ').split(' ');
  for (const cand of [words.join(' '), words.slice(-2).join(' '), words.slice(-1).join(' ')]) {
    const key = cand.toLowerCase();
    if (key in COUNTY_DEPT) return key;
  }
  return null;
}

export function extractCounties(text) {
  const anchored = new Set(), bare = new Set();
  let m;
  for (const re of [ANCHORED_RE, ANCHORED_OF_RE]) {
    re.lastIndex = 0;
    while ((m = re.exec(text))) { const c = lookupCounty(m[1]); if (c) anchored.add(c); }
  }
  BARE_RE.lastIndex = 0;
  while ((m = BARE_RE.exec(text))) { const c = lookupCounty(m[1]); if (c) bare.add(c); }
  return { anchored: [...anchored], bare: [...bare] };
}

export function deptOfCounties(counties) {
  const depts = new Set(counties.map((c) => COUNTY_DEPT[c]));
  if (depts.size === 1) return { dept: [...depts][0], conflict: false };
  if (depts.size > 1) return { dept: null, conflict: true };
  return { dept: null, conflict: false };
}

// --- panel justices ---------------------------------------------------------
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
function collectNames(tokens, dir) {
  const out = [];
  const seq = dir === 1 ? tokens : [...tokens].reverse();
  for (const tok of seq) {
    if (isRole(tok)) { if (/^JJ\.?$/i.test(tok.trim()) && out.length) break; continue; }
    if (looksLikeName(tok)) { out.push(tok); continue; }
    break;
  }
  return dir === 1 ? out : out.reverse();
}

export function extractPanel(text) {
  const found = new Set();
  let m;
  const formA = /(?:Concur|Present)\s*[—–:\-]\s*/g;
  while ((m = formA.exec(text))) {
    const win = text.slice(m.index + m[0].length, m.index + m[0].length + 220);
    for (const n of collectNames(tokenize(win), 1)) found.add(n);
  }
  const formB = /,?\s*JJ?\.?,?\s+concurs?\b/gi;
  while ((m = formB.exec(text))) {
    const win = text.slice(Math.max(0, m.index - 220), m.index);
    for (const n of collectNames(tokenize(win), -1)) found.add(n);
  }
  const withRe = /\bconcurs?\s+with\s+([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+JJ?\.?/g;
  while ((m = withRe.exec(text))) { if (looksLikeName(m[1])) found.add(m[1].trim()); }
  const dissRe = /\b([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+JJ?\.?,?\s*(?:dissent|concurr)/g;
  while ((m = dissRe.exec(text))) { if (looksLikeName(m[1])) found.add(m[1].trim()); }
  return [...found];
}

// --- opinion author ---------------------------------------------------------
export function extractAuthor(text, type) {
  if (!text) return null;
  const head = text.slice(0, 160).replace(/^[\s—–\-]+/, '');
  if (/^per\s+curiam/i.test(head) || type === 'per_curiam') return 'Per Curiam';
  const m = head.match(/^([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+(?:P\.?\s?J\.?|JJ?\.?)[\s,:.(]/);
  if (m && looksLikeName(m[1])) return m[1].trim();
  return null;
}

// --- roster -----------------------------------------------------------------
// records: [{ countyDept, countyConflict, panel }]
export function buildRoster(records) {
  const votes = {};
  for (const r of records) {
    if (!r.countyDept || r.countyConflict) continue;
    for (const j of r.panel) (votes[j] ||= [0, 0, 0, 0, 0])[r.countyDept]++;
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

// majority department among panel justices that have a confident roster entry
export function panelVoteDept(panel, roster) {
  const tally = [0, 0, 0, 0, 0];
  let used = 0;
  for (const j of panel) {
    const e = roster[j];
    if (e && e.n >= 3 && e.conf >= 0.8) { tally[e.dept]++; used++; }
  }
  if (!used) return { dept: null, agree: false, used: 0 };
  let best = 1;
  for (let d = 2; d <= 4; d++) if (tally[d] > tally[best]) best = d;
  return { dept: best, agree: tally[best] === used, used };
}
