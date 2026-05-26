/**
 * PROTOTYPE — Appellate Division "Pass 1" extractor + downstream validation.
 *
 * Pass 1 (extraction): one read-only pass over opinion text pulling three raw
 * fields per case — panel justices, opinion authors, lower-court county.
 *
 * It then exercises the downstream design to validate the extractor:
 *   - county -> department via a static, verified map
 *   - a justice -> department roster bootstrapped from county-resolved cases
 *   - panel-vote department, cross-checked against county and against the
 *     stored court_department (PDF cohort only)
 *
 * Nothing is written back. Usage:  node prototype_ad_pass1.js [capN] [pdfN]
 */
import pg from 'pg';

const { Pool } = pg;
const pool = new Pool({
  host: 'localhost', port: 5432, database: 'ny_appellate_division',
  user: 'claude', password: 'claude', max: 4,
});

const CAP_SAMPLE = parseInt(process.argv[2] || '10000', 10);
const PDF_SAMPLE = parseInt(process.argv[3] || '15000', 10);

// ---------------------------------------------------------------------------
// County -> AD department. Verified against the NY Board of Law Examiners
// county list (which itself misspells NY's "Allegany" as "Allegheny" —
// corrected here). Stable for the whole 1896-present range.
// ---------------------------------------------------------------------------
const COUNTY_DEPT = {
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

function extractCounties(text) {
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

function deptOfCounties(counties) {
  const depts = new Set(counties.map((c) => COUNTY_DEPT[c]));
  if (depts.size === 1) return { dept: [...depts][0], conflict: false };
  if (depts.size > 1) return { dept: null, conflict: true };
  return { dept: null, conflict: false };
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
function extractPanel(text) {
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

// ---------------------------------------------------------------------------
// Opinion-author extraction
// ---------------------------------------------------------------------------
function extractAuthor(text, type) {
  if (!text) return null;
  const head = text.slice(0, 160).replace(/^[\s—–\-]+/, '');
  if (/^per\s+curiam/i.test(head) || type === 'per_curiam') return 'Per Curiam';
  const m = head.match(/^([A-Z][A-Za-z'’.\- ]{1,22}?),?\s+(?:P\.?\s?J\.?|JJ?\.?)[\s,:.(]/);
  if (m && looksLikeName(m[1])) return m[1].trim();
  return null;
}

// ---------------------------------------------------------------------------
// Pass-1 extraction for one case row
// ---------------------------------------------------------------------------
function extractCase(row, cohort) {
  const ops = row.opinions || [];
  const joined = ops.map((o) => o.text || '').join('\n~~~\n');
  const counties = extractCounties(joined);
  const { dept: countyDept, conflict } = deptOfCounties(counties.anchored);
  const panel = extractPanel(joined);
  const authors = ops.map((o) => extractAuthor(o.text, o.type)).filter(Boolean);
  const volume = (row.source_url || '').match(/bv\/([0-9A-Za-z]+)\.pdf/)?.[1] || null;
  return {
    id: row.id, cohort, date: row.decision_date || '',
    storedDept: row.court_department, volume,
    counties: counties.anchored, anyCounty: !!(counties.anchored.length || counties.bare.length),
    countyDept, countyConflict: conflict, panel, authors, nOpinions: ops.length,
  };
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------
async function sampleIds(cohort, n) {
  const { rows } = await pool.query(
    `SELECT id FROM cases WHERE court_name_abbreviation = $1 ORDER BY random() LIMIT $2`, [cohort, n]);
  return rows.map((r) => r.id);
}
async function fetchCases(ids) {
  const out = [];
  for (let i = 0; i < ids.length; i += 2000) {
    const { rows } = await pool.query(
      `SELECT c.id, c.decision_date, c.court_department, c.source_url,
              json_agg(json_build_object('type', o.opinion_type, 'text', o.text,
                                          'author', o.author)
                       ORDER BY o.opinion_index NULLS LAST, o.id) AS opinions
       FROM cases c JOIN opinions o ON o.case_id = c.id
       WHERE c.id = ANY($1) GROUP BY c.id`, [ids.slice(i, i + 2000)]);
    out.push(...rows);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Roster bootstrap + panel-vote department
// ---------------------------------------------------------------------------
function buildRoster(records) {
  const votes = {};   // surname -> [_, d1, d2, d3, d4]
  for (const r of records) {
    if (!r.countyDept || r.countyConflict) continue;
    for (const j of r.panel) {
      (votes[j] ||= [0, 0, 0, 0, 0])[r.countyDept]++;
    }
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
function panelVoteDept(panel, roster) {
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

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
const pct = (a, b) => (b ? ((100 * a) / b).toFixed(1) + '%' : '—');

function reportCohort(label, recs, roster) {
  const n = recs.length;
  let anchored = 0, anyC = 0, panelHit = 0, panelSizeSum = 0, conflicts = 0;
  let resolvable = 0;            // county OR confident panel gives a dept
  const decade = {};
  for (const r of recs) {
    if (r.counties.length) anchored++;
    if (r.anyCounty) anyC++;
    if (r.panel.length) { panelHit++; panelSizeSum += r.panel.length; }
    if (r.countyConflict) conflicts++;
    const pv = panelVoteDept(r.panel, roster);
    if (r.countyDept || pv.dept) resolvable++;
    const d = r.date.slice(0, 3) + '0s';
    (decade[d] ||= { n: 0, county: 0, panel: 0, resolvable: 0 });
    decade[d].n++;
    if (r.counties.length) decade[d].county++;
    if (r.panel.length) decade[d].panel++;
    if (r.countyDept || pv.dept) decade[d].resolvable++;
  }
  console.log(`\n======== ${label}  (n=${n}) ========`);
  console.log(`county, court-anchored : ${pct(anchored, n)}`);
  console.log(`county, any mention    : ${pct(anyC, n)}`);
  console.log(`panel justices found   : ${pct(panelHit, n)}   avg size ${(panelSizeSum / Math.max(1, panelHit)).toFixed(2)}`);
  console.log(`county dept conflicts  : ${conflicts}`);
  console.log(`dept resolvable (county OR panel-roster): ${pct(resolvable, n)}`);
  console.log(`  -- by decade --`);
  for (const [d, v] of Object.entries(decade).sort())
    if (v.n >= 25)
      console.log(`  ${d}: n=${String(v.n).padStart(5)}  county ${pct(v.county, v.n).padStart(6)}` +
        `  panel ${pct(v.panel, v.n).padStart(6)}  resolvable ${pct(v.resolvable, v.n).padStart(6)}`);
}

function reportPdfValidation(recs, roster) {
  let countyChecked = 0, countyMatch = 0;
  let triple = 0, tripleStoredWrong = 0, tripleAllAgree = 0;
  const volStats = {};
  for (const r of recs) {
    const pv = panelVoteDept(r.panel, roster);
    if (r.countyDept && r.storedDept) {
      countyChecked++;
      if (r.countyDept === r.storedDept) countyMatch++;
      const vs = (volStats[r.volume] ||= { checked: 0, match: 0 });
      vs.checked++;
      if (r.countyDept === r.storedDept) vs.match++;
    }
    // triple cross-check: county AND panel-roster both gave a department
    if (r.countyDept && pv.dept && r.storedDept) {
      triple++;
      if (r.countyDept === pv.dept) {
        tripleAllAgree++;
        if (r.countyDept !== r.storedDept) tripleStoredWrong++;
      }
    }
  }
  console.log(`\n======== PDF cohort — court_department reliability ========`);
  console.log(`county vs stored      : ${countyChecked} checked, ${pct(countyMatch, countyChecked)} match`);
  console.log(`triple cross-check (cases with BOTH county + panel-roster dept):`);
  console.log(`  ${triple} cases; county==panel in ${pct(tripleAllAgree, triple)}`);
  console.log(`  of those agreeing, stored court_department is WRONG in ${pct(tripleStoredWrong, tripleAllAgree)}` +
    `  (${tripleStoredWrong} cases)`);
  const worst = Object.entries(volStats).filter(([, v]) => v.checked >= 15)
    .map(([vol, v]) => ({ vol, checked: v.checked, rate: v.match / v.checked }))
    .sort((a, b) => a.rate - b.rate);
  console.log(`  -- worst volumes (county vs stored match rate) --`);
  for (const w of worst.slice(0, 10))
    console.log(`  ${w.vol.padEnd(9)} ${pct(Math.round(w.rate * w.checked), w.checked).padStart(6)}  (n=${w.checked})`);
  console.log(`  -- best volumes --`);
  for (const w of worst.slice(-3))
    console.log(`  ${w.vol.padEnd(9)} ${pct(Math.round(w.rate * w.checked), w.checked).padStart(6)}  (n=${w.checked})`);
}

function reportRoster(roster) {
  const all = Object.values(roster);
  const conf = all.filter((e) => e.n >= 3 && e.conf >= 0.8);
  console.log(`\n======== Bootstrapped justice -> department roster ========`);
  console.log(`distinct surnames voted: ${all.length}`);
  console.log(`high-confidence entries (n>=3, conf>=0.8): ${conf.length}`);
  console.log(`mean confidence of those: ${(conf.reduce((s, e) => s + e.conf, 0) / conf.length).toFixed(3)}`);
}

(async () => {
  console.log(`Sampling CAP=${CAP_SAMPLE} PDF=${PDF_SAMPLE} ...`);
  const capRows = await fetchCases(await sampleIds('N.Y. App. Div.', CAP_SAMPLE));
  const pdfRows = await fetchCases(await sampleIds('N.Y.', PDF_SAMPLE));
  console.log(`Fetched CAP=${capRows.length} PDF=${pdfRows.length}.`);

  const cap = capRows.map((r) => extractCase(r, 'CAP'));
  const pdf = pdfRows.map((r) => extractCase(r, 'PDF'));
  const roster = buildRoster([...cap, ...pdf]);

  reportRoster(roster);
  reportCohort('CAP (Harvard, older)', cap, roster);
  reportCohort('PDF (bound-volume, newer)', pdf, roster);
  reportPdfValidation(pdf, roster);

  console.log(`\n-- sample PDF cases where stored court_department is corroborated WRONG --`);
  let shown = 0;
  for (const r of pdf) {
    const pv = panelVoteDept(r.panel, roster);
    if (shown < 10 && r.countyDept && pv.dept && r.countyDept === pv.dept
        && r.storedDept && r.storedDept !== r.countyDept) {
      console.log(`  #${r.id} ${r.date} vol ${r.volume}  county=${r.counties.join(',')}` +
        ` -> dept ${r.countyDept}; panel-vote dept ${pv.dept}; STORED ${r.storedDept}` +
        `  panel=[${r.panel.join(', ')}]`);
      shown++;
    }
  }
  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
