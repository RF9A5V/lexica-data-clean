/**
 * Audit court_department across the FULL PDF-parsed cohort of ny_appellate_division.
 *
 * For every PDF-cohort case (court_name_abbreviation = 'N.Y.') it extracts the
 * lower-court county and panel justices, resolves the AD department two ways
 * (county map + bootstrapped justice roster), and compares to the stored
 * court_department to flag banner-detection mislabels.
 *
 * READ-ONLY. Writes three artifacts to ./pass1_out/ :
 *   - pdf_department_corrections.csv   high/medium-confidence corrections
 *   - pdf_department_review.csv        low-confidence / signal-conflict cases
 *   - apply_pdf_department_corrections.sql   ready-to-run UPDATE (NOT executed)
 *
 *   node audit_pdf_departments.js
 */
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  extractCounties, deptOfCounties, extractPanel, buildRoster, panelVoteDept,
} from './pass1_lib.js';

const { Pool } = pg;
const pool = new Pool({
  host: 'localhost', port: 5432, database: 'ny_appellate_division',
  user: 'claude', password: 'claude', max: 4,
});
const OUT_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), 'pass1_out');

// ---------------------------------------------------------------------------
async function fetchAll() {
  const { rows: idRows } = await pool.query(
    `SELECT id FROM cases WHERE court_name_abbreviation = 'N.Y.' ORDER BY id`);
  const ids = idRows.map((r) => r.id);
  console.log(`PDF cohort: ${ids.length} cases. Fetching opinion text ...`);
  const records = [];
  for (let i = 0; i < ids.length; i += 2000) {
    const { rows } = await pool.query(
      `SELECT c.id, c.decision_date, c.court_department, c.source_url,
              json_agg(o.text ORDER BY o.opinion_index NULLS LAST, o.id) AS texts
       FROM cases c JOIN opinions o ON o.case_id = c.id
       WHERE c.id = ANY($1) GROUP BY c.id`, [ids.slice(i, i + 2000)]);
    for (const row of rows) {
      const joined = (row.texts || []).map((t) => t || '').join('\n~~~\n');
      const counties = extractCounties(joined);
      const { dept: countyDept, conflict } = deptOfCounties(counties.anchored);
      records.push({
        id: row.id,
        date: row.decision_date || '',
        storedDept: row.court_department,
        volume: (row.source_url || '').match(/bv\/([0-9A-Za-z]+)\.pdf/)?.[1] || '(none)',
        counties: counties.anchored,
        countyDept, countyConflict: conflict,
        panel: extractPanel(joined),
      });
    }
    process.stdout.write(`\r  extracted ${Math.min(i + 2000, ids.length)}/${ids.length}`);
  }
  process.stdout.write('\n');
  return records;
}

// ---------------------------------------------------------------------------
// Resolve a department for one case + assign a confidence.
//   HIGH     county + unanimous(>=3) panel-roster agree
//   MED      county only, or strong panel-roster only
//   CONFLICT county and strong panel disagree
//   LOW      only a weak panel signal
//   NONE     no signal
function resolve(r, roster) {
  const pv = panelVoteDept(r.panel, roster);
  const cd = r.countyConflict ? null : r.countyDept;
  const strongPanel = pv.used >= 3 && pv.agree ? pv.dept : null;
  let dept = null, conf = 'NONE';
  if (cd && strongPanel) {
    if (cd === strongPanel) { dept = cd; conf = 'HIGH'; }
    else { dept = cd; conf = 'CONFLICT'; }
  } else if (cd) { dept = cd; conf = 'MED'; }
  else if (strongPanel) { dept = strongPanel; conf = 'MED'; }
  else if (pv.dept) { dept = pv.dept; conf = 'LOW'; }
  return { dept, conf, panelDept: pv.dept, panelUsed: pv.used, panelAgree: pv.agree };
}

function csvCell(v) {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

// ---------------------------------------------------------------------------
(async () => {
  const records = await fetchAll();
  const roster = buildRoster(records);
  const confJustices = Object.values(roster).filter((e) => e.n >= 3 && e.conf >= 0.8);
  console.log(`Roster: ${Object.keys(roster).length} surnames, ` +
    `${confJustices.length} high-confidence (mean conf ` +
    `${(confJustices.reduce((s, e) => s + e.conf, 0) / confJustices.length).toFixed(3)}).`);

  const tally = { ok: 0, unresolved: 0, mislabel: 0 };
  const byConf = { HIGH: 0, MED: 0, LOW: 0, CONFLICT: 0, NONE: 0 };
  const corrections = [];   // HIGH + MED mislabels  -> apply
  const review = [];        // LOW + CONFLICT mislabels -> human review
  const vol = {};           // volume -> { n, checked, mislabel }

  for (const r of records) {
    const res = resolve(r, roster);
    byConf[res.conf]++;
    const v = (vol[r.volume] ||= { n: 0, checked: 0, mislabel: 0 });
    v.n++;

    if (res.dept == null) { tally.unresolved++; continue; }
    const isMislabel = r.storedDept != null && res.dept !== r.storedDept;
    if (['HIGH', 'MED'].includes(res.conf)) {
      v.checked++;
      if (isMislabel) v.mislabel++;
    }
    if (!isMislabel) { tally.ok++; continue; }
    tally.mislabel++;

    const rowOut = {
      case_id: r.id, volume: r.volume, decision_date: r.date,
      stored_dept: r.storedDept, corrected_dept: res.dept, confidence: res.conf,
      county: r.counties.join('+') || (r.countyConflict ? '(conflict)' : ''),
      panel_dept: res.panelDept ?? '', panel_used: res.panelUsed,
      panel: r.panel.join('; '),
    };
    (['HIGH', 'MED'].includes(res.conf) ? corrections : review).push(rowOut);
  }

  // ---- write artifacts ----
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const cols = ['case_id', 'volume', 'decision_date', 'stored_dept', 'corrected_dept',
    'confidence', 'county', 'panel_dept', 'panel_used', 'panel'];
  const toCsv = (rows) => [cols.join(','),
    ...rows.map((x) => cols.map((c) => csvCell(x[c])).join(','))].join('\n') + '\n';
  corrections.sort((a, b) => a.volume.localeCompare(b.volume) || a.case_id - b.case_id);
  review.sort((a, b) => a.volume.localeCompare(b.volume) || a.case_id - b.case_id);
  fs.writeFileSync(path.join(OUT_DIR, 'pdf_department_corrections.csv'), toCsv(corrections));
  fs.writeFileSync(path.join(OUT_DIR, 'pdf_department_review.csv'), toCsv(review));

  const sqlBlock = (label, rows) => {
    if (!rows.length) return `-- ${label}: none\n`;
    const values = rows.map((x) => `(${x.case_id},${x.corrected_dept})`).join(',\n  ');
    return `-- ${label}: ${rows.length} cases\n` +
      `UPDATE cases AS c SET court_department = v.dept\n` +
      `FROM (VALUES\n  ${values}\n) AS v(id, dept)\n` +
      `WHERE c.id = v.id AND c.court_department IS DISTINCT FROM v.dept;\n`;
  };
  const high = corrections.filter((x) => x.confidence === 'HIGH');
  const med = corrections.filter((x) => x.confidence === 'MED');
  fs.writeFileSync(path.join(OUT_DIR, 'apply_pdf_department_corrections.sql'),
    `-- court_department corrections for the ny_appellate_division PDF cohort.\n` +
    `-- Generated by audit_pdf_departments.js. REVIEW BEFORE RUNNING.\n` +
    `-- HIGH = county + unanimous panel-roster agree; MED = single strong signal.\n\n` +
    `BEGIN;\n\n${sqlBlock('HIGH confidence', high)}\n${sqlBlock('MED confidence', med)}\n` +
    `-- COMMIT;  -- uncomment to apply\nROLLBACK;\n`);

  // ---- summary ----
  const pct = (a, b) => (b ? ((100 * a) / b).toFixed(1) + '%' : '—');
  console.log(`\n==== court_department audit — PDF cohort (${records.length} cases) ====`);
  console.log(`confidence of resolution:  HIGH ${byConf.HIGH}  MED ${byConf.MED}  ` +
    `LOW ${byConf.LOW}  CONFLICT ${byConf.CONFLICT}  NONE ${byConf.NONE}`);
  console.log(`stored department:  OK ${tally.ok}  ` +
    `MISLABELLED ${tally.mislabel} (${pct(tally.mislabel, tally.ok + tally.mislabel)})  ` +
    `unresolved ${tally.unresolved}`);
  console.log(`  -> corrections (HIGH+MED): ${corrections.length}  ` +
    `[HIGH ${high.length}, MED ${med.length}]`);
  console.log(`  -> review (LOW+CONFLICT):  ${review.length}`);

  const volRows = Object.entries(vol).filter(([, v]) => v.checked >= 20)
    .map(([name, v]) => ({ name, ...v, rate: v.mislabel / v.checked }))
    .sort((a, b) => b.rate - a.rate);
  console.log(`\nworst volumes by mislabel rate (checked >= 20):`);
  for (const v of volRows.slice(0, 15))
    console.log(`  ${v.name.padEnd(10)} ${pct(v.mislabel, v.checked).padStart(6)}  ` +
      `(${v.mislabel}/${v.checked} mislabelled,  ${v.n} cases in volume)`);
  const clean = volRows.filter((v) => v.mislabel === 0).length;
  console.log(`\n${clean} of ${volRows.length} volumes have zero detected mislabels.`);
  console.log(`\nArtifacts written to ${OUT_DIR}/`);
  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
