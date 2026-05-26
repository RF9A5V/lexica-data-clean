/**
 * Re-derive court_department across the PDF cohort of an Appellate Division
 * source DB, using the text-based resolver (src/department.js) + the shipped
 * justice roster (configs/ad_justice_roster.json), and emit the corrections.
 *
 * court_department is DETERMINISTIC — a pure function of each case's opinion
 * text (recited county + panel justices). So the way to get correct
 * departments onto another environment is to RE-DERIVE there, not to copy
 * dev's column over: `cases` has no stable cross-environment key (no curie;
 * source_url is not unique — stacked memo decisions share a page; integer id
 * diverges). Run against staging, this reproduces exactly what dev computed.
 *
 * Target: local ny_appellate_division by default; --db-url for any other DB
 * (e.g. staging behind a `fly proxy`).
 *
 * READ-ONLY against the DB. Writes department_cleanup/:
 *   corrections.csv   high/medium-confidence residual corrections
 *   review.csv        low-confidence / signal-conflict cases
 *   apply.sql         ready-to-run UPDATE, keyed on the TARGET's case ids
 *                     (ends in ROLLBACK until reviewed)
 *
 *   node scripts/recheck_db_departments.js
 *   node scripts/recheck_db_departments.js --db-url=postgres://USER@HOST:PORT/DB
 *
 * To get correct court_department onto staging:
 *   1. fly proxy 15433:5432 -a <fly-pg-app>
 *   2. node scripts/recheck_db_departments.js \
 *        --db-url=postgres://postgres@localhost:15433/<db>
 *   3. review department_cleanup/apply.sql, flip ROLLBACK -> COMMIT, then
 *      psql postgres://postgres@localhost:15433/<db> -f department_cleanup/apply.sql
 */
import { spawn } from 'child_process';
import readline from 'readline';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  extractCountyDepartment, extractPanel, panelVoteDepartment, loadStaticRoster,
} from '../src/department.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, '..', 'department_cleanup');
const DB = { host: 'localhost', user: 'claude', password: 'claude', database: 'ny_appellate_division' };
// --db-url targets any DB (e.g. staging via a fly proxy); default is local.
const DB_URL = process.argv.slice(2).find((a) => a.startsWith('--db-url='))?.slice(9) || null;

const SQL = `\\set FETCH_COUNT 2000
SELECT json_build_object('id', c.id, 'd', c.court_department, 'txt', string_agg(o.text, ' '))
FROM cases c JOIN opinions o ON o.case_id = c.id
WHERE c.court_name_abbreviation = 'N.Y.'
GROUP BY c.id;
`;

const roster = loadStaticRoster();

// Resolve a department from opinion text. Confidence:
//   HIGH     county + unanimous(>=3) panel-roster agree
//   MED      county only, or strong panel-roster only
//   CONFLICT county and strong panel disagree
//   LOW      only a weak panel signal
//   NONE     no signal
function resolve(txt) {
  const cty = extractCountyDepartment(txt);
  const countyDept = cty.conflict ? null : cty.dept;
  const pv = panelVoteDepartment(extractPanel(txt), roster);
  const strong = pv.used >= 3 && pv.agree ? pv.dept : null;
  let dept = null, conf = 'NONE';
  if (countyDept && strong) { dept = countyDept; conf = countyDept === strong ? 'HIGH' : 'CONFLICT'; }
  else if (countyDept) { dept = countyDept; conf = 'MED'; }
  else if (strong) { dept = strong; conf = 'MED'; }
  else if (pv.dept) { dept = pv.dept; conf = 'LOW'; }
  return { dept, conf, countyDept, panelDept: pv.dept, panelUsed: pv.used };
}

function csvCell(v) {
  const s = String(v ?? '');
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  if (!Object.keys(roster).length) throw new Error('static roster is empty — run build_justice_roster.js first');
  console.log(`Roster loaded: ${Object.keys(roster).length} justices.`);
  console.log(`Streaming PDF cohort from ${DB_URL ? DB_URL.replace(/:[^:@/]+@/, ':***@') : DB.database} ...`);

  const proc = spawn('psql',
    DB_URL
      ? [DB_URL, '-t', '-A', '-q']
      : ['-h', DB.host, '-U', DB.user, '-d', DB.database, '-t', '-A', '-q'],
    { env: DB_URL ? process.env : { ...process.env, PGPASSWORD: DB.password } });
  proc.stdin.write(SQL);
  proc.stdin.end();
  let stderr = '';
  proc.stderr.on('data', (d) => { stderr += d.toString(); });

  const tally = { ok: 0, mislabel: 0, unresolved: 0 };
  const byConf = { HIGH: 0, MED: 0, LOW: 0, CONFLICT: 0, NONE: 0 };
  const corrections = [], review = [];
  const dirByStored = {};   // "stored->corrected" -> count, for sanity
  let scanned = 0;

  const rl = readline.createInterface({ input: proc.stdout });
  for await (const line of rl) {
    if (!line.trim()) continue;
    scanned++;
    let row;
    try { row = JSON.parse(line); } catch { continue; }
    const r = resolve(row.txt || '');
    byConf[r.conf]++;
    if (r.dept == null) { tally.unresolved++; continue; }
    if (r.dept === row.d) { tally.ok++; continue; }
    tally.mislabel++;
    const rec = {
      case_id: row.id, stored_dept: row.d, corrected_dept: r.dept, confidence: r.conf,
      county_dept: r.countyDept ?? '', panel_dept: r.panelDept ?? '', panel_used: r.panelUsed,
    };
    if (r.conf === 'HIGH' || r.conf === 'MED') {
      corrections.push(rec);
      const k = `${row.d ?? 'null'} -> ${r.dept}`;
      dirByStored[k] = (dirByStored[k] || 0) + 1;
    } else {
      review.push(rec);
    }
    if (scanned % 20000 === 0) process.stdout.write(`\r  ${scanned} cases`);
  }
  process.stdout.write('\n');
  const code = await new Promise((res) => proc.on('close', res));
  if (code !== 0) throw new Error(`psql exited ${code}\n${stderr}`);

  // ---- artifacts ----
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const cols = ['case_id', 'stored_dept', 'corrected_dept', 'confidence',
    'county_dept', 'panel_dept', 'panel_used'];
  const toCsv = (rows) => [cols.join(','),
    ...rows.map((x) => cols.map((c) => csvCell(x[c])).join(','))].join('\n') + '\n';
  corrections.sort((a, b) => a.case_id - b.case_id);
  review.sort((a, b) => a.case_id - b.case_id);
  fs.writeFileSync(path.join(OUT_DIR, 'corrections.csv'), toCsv(corrections));
  fs.writeFileSync(path.join(OUT_DIR, 'review.csv'), toCsv(review));

  const high = corrections.filter((x) => x.confidence === 'HIGH');
  const med = corrections.filter((x) => x.confidence === 'MED');
  const sqlBlock = (label, rows) => {
    if (!rows.length) return `-- ${label}: none\n`;
    const values = rows.map((x) => `(${x.case_id},${x.corrected_dept})`).join(',\n  ');
    return `-- ${label}: ${rows.length} cases\n` +
      `UPDATE cases AS c SET court_department = v.dept\n` +
      `FROM (VALUES\n  ${values}\n) AS v(id, dept)\n` +
      `WHERE c.id = v.id AND c.court_department IS DISTINCT FROM v.dept;\n`;
  };
  fs.writeFileSync(path.join(OUT_DIR, 'apply.sql'),
    `-- court_department corrections (Appellate Division PDF cohort).\n` +
    `-- Generated by recheck_db_departments.js — re-derived from opinion text;\n` +
    `-- case ids are the TARGET DB's. REVIEW, then change ROLLBACK to COMMIT.\n\n` +
    `BEGIN;\n\n${sqlBlock('HIGH confidence', high)}\n${sqlBlock('MED confidence', med)}\n` +
    `ROLLBACK;\n`);

  // ---- summary ----
  const pct = (a, b) => (b ? ((100 * a) / b).toFixed(1) + '%' : '—');
  console.log(`\n==== re-check — PDF cohort (${scanned} cases) ====`);
  console.log(`resolution confidence:  HIGH ${byConf.HIGH}  MED ${byConf.MED}  LOW ${byConf.LOW}  CONFLICT ${byConf.CONFLICT}  NONE ${byConf.NONE}`);
  console.log(`vs stored:  OK ${tally.ok}  MISLABELLED ${tally.mislabel}  unresolved ${tally.unresolved}`);
  console.log(`residual corrections (HIGH+MED): ${corrections.length}  [HIGH ${high.length}, MED ${med.length}]`);
  console.log(`review (LOW+CONFLICT): ${review.length}`);
  console.log(`\ncorrection directions (stored -> corrected):`);
  for (const [k, v] of Object.entries(dirByStored).sort((a, b) => b[1] - a[1]))
    console.log(`  ${k.padEnd(14)} ${v}`);
  console.log(`\nartifacts: ${OUT_DIR}/`);
}

main().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
