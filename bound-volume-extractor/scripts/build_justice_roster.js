/**
 * Build the static justice -> AD department roster used by src/department.js.
 *
 * Streams the PDF-parsed cohort of the `ny_appellate_division` source DB
 * (court_name_abbreviation = 'N.Y.', which carries a per-case
 * court_department), extracts each case's panel justices, and tallies a
 * justice -> department vote. A justice sits in exactly one department, so
 * the winning department's vote share is ~1.0 for a clean surname.
 *
 * Voting uses the stored court_department rather than re-deriving it from
 * the county: the Third Department's docket is dominated by agency review
 * and attorney discipline, which recite no lower-court county — a
 * county-only roster has almost no Third-Department coverage. The stored
 * court_department was corrected by caselaw-extractor/audit_pdf_departments
 * and per-justice majority voting washes out the residual ~few-percent noise.
 *
 * Output: configs/ad_justice_roster.json — the parser's fallback roster for
 * cases (esp. Third-Department) whose own volume supplies no county signal.
 *
 * Re-run when the bench turns over (justices serve 10+ years; a yearly
 * refresh is ample). Read-only against the DB.
 *
 *   node scripts/build_justice_roster.js
 */
import { spawn } from 'child_process';
import readline from 'readline';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { extractPanel, buildRoster } from '../src/department.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_FILE = path.join(__dirname, '..', 'configs', 'ad_justice_roster.json');

const DB = { host: 'localhost', user: 'claude', password: 'claude', database: 'ny_appellate_division' };

// FETCH_COUNT makes psql stream the result through a cursor instead of
// buffering the whole set. `-t -A` => one compact JSON object per line.
const SQL = `\\set FETCH_COUNT 2000
SELECT json_build_object('d', c.court_department, 'txt', string_agg(o.text, ' '))
FROM cases c JOIN opinions o ON o.case_id = c.id
WHERE c.court_name_abbreviation = 'N.Y.' AND c.court_department IS NOT NULL
GROUP BY c.id;
`;

async function main() {
  console.log('Streaming PDF-cohort AD cases (with stored court_department) ...');
  const proc = spawn('psql', [
    '-h', DB.host, '-U', DB.user, '-d', DB.database, '-t', '-A', '-q',
  ], { env: { ...process.env, PGPASSWORD: DB.password } });
  proc.stdin.write(SQL);
  proc.stdin.end();
  let stderr = '';
  proc.stderr.on('data', (d) => { stderr += d.toString(); });

  const rl = readline.createInterface({ input: proc.stdout });
  const records = [];
  let scanned = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    scanned++;
    let row;
    try { row = JSON.parse(line); } catch { continue; }
    const panel = extractPanel(row.txt || '');
    if (panel.length) records.push({ dept: row.d, panel });
    if (scanned % 20000 === 0) process.stdout.write(`\r  ${scanned} cases`);
  }
  process.stdout.write('\n');
  const code = await new Promise((r) => proc.on('close', r));
  if (code !== 0) throw new Error(`psql exited ${code}\n${stderr}`);

  const roster = buildRoster(records);
  // Drop single-mention noise (OCR-garbled surnames); keep n >= 3. The parser
  // also requires conf >= 0.8 at lookup time, but conf is kept in the file
  // for transparency / review.
  const justices = {};
  for (const [name, e] of Object.entries(roster).sort()) {
    if (e.n < 3) continue;
    justices[name] = { dept: e.dept, conf: Number(e.conf.toFixed(3)), n: e.n };
  }
  const confident = Object.values(justices).filter((e) => e.conf >= 0.8);
  const byDept = [0, 0, 0, 0, 0];
  for (const e of confident) byDept[e.dept]++;

  const doc = {
    generated: new Date().toISOString(),
    source: 'ny_appellate_division PDF cohort — stored (audit-corrected) court_department + extracted panels',
    cases_used: records.length,
    justice_count: Object.keys(justices).length,
    confident_count: confident.length,
    note: 'parser uses entries with conf >= 0.8 and n >= 3; others kept for review',
    justices,
  };
  fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(doc, null, 2) + '\n');

  console.log(`\nscanned ${scanned} cases, ${records.length} with an extractable panel.`);
  console.log(`roster: ${doc.justice_count} surnames (n>=3), ${confident.length} confident (conf>=0.8).`);
  console.log(`confident by department: 1=${byDept[1]} 2=${byDept[2]} 3=${byDept[3]} 4=${byDept[4]}`);
  console.log(`written: ${path.relative(path.join(__dirname, '..'), OUT_FILE)}`);
}

main().catch((e) => { console.error('FAILED:', e.message); process.exit(1); });
