/**
 * CAP-cohort Appellate Division court_department backfill.
 *
 * Re-uses the audited county+panel resolver from bound-volume-extractor (the
 * one that fixed the PDF cohort) to populate court_department for the ~657k
 * CAP-imported cases (court_name_abbreviation = "N.Y. App. Div.") that
 * currently have it NULL.
 *
 * Four phases, each replayable from disk:
 *   1. extract       cases  -> pass1_out/cap_extractions.jsonl
 *   2. build-roster  jsonl  -> pass1_out/cap_roster.json
 *   3. resolve       jsonl+rosters -> pass1_out/cap_department_updates.csv
 *                                  +  pass1_out/cap_department_review.csv
 *   4. apply         updates.csv -> batched UPDATE on cases (idempotent;
 *                                   only touches NULL rows; requires --confirm)
 *
 * `all` runs phases 1-3 (dry-run). Apply is always a separate invocation.
 *
 *   node backfillCapDepartments.js all                 # full dry-run, legacy ny_appellate_division DB
 *   node backfillCapDepartments.js all --limit 1000    # smoke test
 *   node backfillCapDepartments.js all --target=ny_caselaw   # run against merged DB (needs MERGE_TARGET_URL)
 *   node backfillCapDepartments.js apply --confirm     # write to DB
 *
 * Track B / B3 §1 — under --target=ny_caselaw, the script connects to the
 * merged DB via the MERGE_TARGET_URL env var and scopes all reads/writes to
 * source_ref='ny_appellate'. Legacy default connects to local
 * ny_appellate_division via claude:claude.
 *
 * Hard filters:
 *   - decision_date year < 1896 is skipped (3 General Term era rows).
 *   - apply only updates rows where court_department IS NULL.
 */
import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';
import pg from 'pg';
import {
  extractCountyDepartment,
  extractPanel,
  buildRoster,
  loadStaticRoster,
  panelVoteDepartment,
  resolveOne,
  loadHistoricalRoster,
  lookupHistorical,
} from '../bound-volume-extractor/src/department.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'pass1_out');
const EXTRACT_PATH = path.join(OUT_DIR, 'cap_extractions.jsonl');
const ROSTER_PATH = path.join(OUT_DIR, 'cap_roster.json');
const UPDATES_PATH = path.join(OUT_DIR, 'cap_department_updates.csv');
const REVIEW_PATH = path.join(OUT_DIR, 'cap_department_review.csv');
// Per-department historical-justice rosters live in this directory; missing
// files are silently tolerated (loadHistoricalRoster treats them as empty).
const HISTORICAL_PATHS = [1, 2, 3, 4].map((d) =>
  path.join(__dirname, 'configs', `historical_justices_dept_${d}.json`));

const args = process.argv.slice(2);
const cmd = args[0] || 'all';
const flag = (name) => args.includes(name);
const optStr = (name) => {
  // Supports both `--foo bar` and `--foo=bar` shapes
  const eqArg = args.find((a) => a.startsWith(`${name}=`));
  if (eqArg) return eqArg.split('=').slice(1).join('=');
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : null;
};
const optNum = (name, dflt) => {
  const v = optStr(name);
  return v != null ? parseInt(v, 10) : dflt;
};
const LIMIT = optNum('--limit', null);
const BATCH = optNum('--batch', 2000);
const APPLY_BATCH = optNum('--apply-batch', 5000);

// Track B / B3 §1: --target=ny_caselaw routes to the merged DB via
// MERGE_TARGET_URL. CAP cohort is appellate-specific by construction, so
// source_ref under merged is always 'ny_appellate'.
const TARGET = optStr('--target');
const MERGED = TARGET === 'ny_caselaw';
if (TARGET && !MERGED) {
  console.error(`--target=${TARGET} is not supported (use --target=ny_caselaw or omit for legacy).`);
  process.exit(2);
}
if (MERGED && !process.env.MERGE_TARGET_URL) {
  console.error('--target=ny_caselaw requires MERGE_TARGET_URL env var (set in co-data/.env or shell).');
  process.exit(2);
}
const SOURCE_REF = 'ny_appellate';
// Extra WHERE clause segment for source_ref scoping under merged; empty under legacy.
const SRC_FILTER = MERGED ? `AND c.source_ref = '${SOURCE_REF}'` : '';

const { Pool } = pg;
function pool() {
  if (MERGED) {
    return new Pool({ connectionString: process.env.MERGE_TARGET_URL, max: 4 });
  }
  return new Pool({
    host: 'localhost', port: 5432, database: 'ny_appellate_division',
    user: 'claude', password: 'claude', max: 4,
  });
}
// `panel-weak` (one or two confident roster votes, not unanimous-with-three)
// has unacceptable error rate for 3rd-Dept cases because the bootstrap
// roster is structurally thin there. Default apply skips it; pass
// --include-weak to opt in.
const APPLY_SOURCES = flag('--include-weak')
  ? new Set(['county', 'county+panel', 'panel', 'panel-weak'])
  : new Set(['county', 'county+panel', 'panel']);

// ---------------------------------------------------------------------------
// Phase 1: extract
// ---------------------------------------------------------------------------
async function extract() {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });
  const p = pool();
  const out = fs.createWriteStream(EXTRACT_PATH);
  // year-prefix filter is on a TEXT column; the CHECK below skips a handful
  // of legacy values that aren't full ISO dates ("1875-01" etc.).
  const baseWhere = `c.court_name_abbreviation = 'N.Y. App. Div.'
                     AND c.court_department IS NULL
                     AND substring(c.decision_date,1,4) ~ '^[0-9]{4}$'
                     AND substring(c.decision_date,1,4)::int >= 1896
                     ${SRC_FILTER}`;
  const totalRow = await p.query(`SELECT count(*)::int AS n FROM cases c WHERE ${baseWhere}`);
  const total = LIMIT ? Math.min(LIMIT, totalRow.rows[0].n) : totalRow.rows[0].n;
  process.stderr.write(`extract: ${total} CAP cases to process (batch ${BATCH})${MERGED ? ` [merged target, source_ref=${SOURCE_REF}]` : ' [legacy ny_appellate_division]'}\n`);

  let lastId = 0, done = 0;
  const t0 = Date.now();
  while (done < total) {
    const remaining = total - done;
    const take = Math.min(BATCH, remaining);
    const { rows } = await p.query(
      `SELECT c.id, c.decision_date,
              string_agg(coalesce(o.text, ''), E'\\n~~~\\n'
                         ORDER BY o.opinion_index NULLS LAST, o.id) AS joined
       FROM cases c LEFT JOIN opinions o ON o.case_id = c.id
       WHERE ${baseWhere} AND c.id > $1
       GROUP BY c.id
       ORDER BY c.id
       LIMIT $2`, [lastId, take]);
    if (!rows.length) break;
    for (const r of rows) {
      const text = r.joined || '';
      const cty = extractCountyDepartment(text);
      const panel = extractPanel(text);
      out.write(JSON.stringify({
        id: Number(r.id),
        date: r.decision_date || '',
        counties: cty.counties,
        countyDept: cty.dept,
        conflict: cty.conflict,
        panel,
      }) + '\n');
      lastId = Number(r.id);
    }
    done += rows.length;
    if (done % (BATCH * 10) === 0 || done === total) {
      const rate = Math.round(done / ((Date.now() - t0) / 1000));
      process.stderr.write(`  ${done}/${total}  (${rate}/s)\n`);
    }
  }
  out.end();
  await new Promise((res) => out.on('close', res));
  await p.end();
  process.stderr.write(`extract: wrote ${EXTRACT_PATH}\n`);
}

// ---------------------------------------------------------------------------
// Phase 2: build CAP-internal roster
// ---------------------------------------------------------------------------
async function readJsonl(p) {
  const out = [];
  const rl = readline.createInterface({ input: fs.createReadStream(p) });
  for await (const line of rl) if (line.trim()) out.push(JSON.parse(line));
  return out;
}

async function buildCapRoster() {
  const recs = await readJsonl(EXTRACT_PATH);
  // Only county-resolved, non-conflict cases vote in the bootstrap roster
  const roster = buildRoster(recs.map((r) => ({
    dept: r.conflict ? null : r.countyDept,
    panel: r.panel,
  })));
  fs.writeFileSync(ROSTER_PATH, JSON.stringify({
    built_at: new Date().toISOString(),
    n_records: recs.length,
    n_voted: recs.filter((r) => r.countyDept && !r.conflict).length,
    justices: roster,
  }, null, 2));
  const confident = Object.values(roster).filter((e) => e.n >= 3 && e.conf >= 0.8).length;
  process.stderr.write(
    `roster: ${Object.keys(roster).length} surnames; ${confident} confident (n>=3, conf>=0.8) -> ${ROSTER_PATH}\n`);
}

// ---------------------------------------------------------------------------
// Phase 3: resolve
// ---------------------------------------------------------------------------
function mergeRosters(capRoster, staticRoster) {
  // CAP-internal observations override the shipped roster for same surname.
  const merged = { ...staticRoster };
  for (const [name, e] of Object.entries(capRoster)) if (e.n >= 3) merged[name] = e;
  return merged;
}

const csvEsc = (v) => {
  const s = v == null ? '' : String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};

async function resolve() {
  const capRosterDoc = JSON.parse(fs.readFileSync(ROSTER_PATH, 'utf8'));
  const merged = mergeRosters(capRosterDoc.justices || {}, loadStaticRoster());
  process.stderr.write(
    `resolve: merged roster ${Object.keys(merged).length} entries (cap ${Object.keys(capRosterDoc.justices || {}).length} + static ${Object.keys(loadStaticRoster()).length})\n`);

  const historical = loadHistoricalRoster(HISTORICAL_PATHS);
  process.stderr.write(`resolve: historical roster ${Object.keys(historical).length} surnames\n`);

  const updates = fs.createWriteStream(UPDATES_PATH);
  const review = fs.createWriteStream(REVIEW_PATH);
  updates.write('id,decision_date,resolved_dept,source,conflict,counties,panel_size\n');
  review.write('id,decision_date,reason,counties,panel_size\n');

  const rl = readline.createInterface({ input: fs.createReadStream(EXTRACT_PATH) });
  let total = 0, resolved = 0, reviewed = 0, historicalAssisted = 0;
  const bySource = { county: 0, 'county+panel': 0, panel: 0, 'panel-weak': 0 };
  const byDept = [0, 0, 0, 0, 0];
  const byDecade = {};

  for await (const line of rl) {
    if (!line.trim()) continue;
    const r = JSON.parse(line);
    total++;
    const cty = { dept: r.countyDept, counties: r.counties, conflict: r.conflict };
    const year = parseInt((r.date || '').slice(0, 4), 10) || null;
    const result = resolveOne(cty, r.panel, merged, null, { historicalRoster: historical, year });
    const decade = r.date.slice(0, 3) + '0s';
    (byDecade[decade] ||= { n: 0, resolved: 0 }).n++;
    if (result.department) {
      resolved++;
      if (result.historicalHits) historicalAssisted++;
      byDecade[decade].resolved++;
      byDept[result.department]++;
      bySource[result.source] = (bySource[result.source] || 0) + 1;
      updates.write([
        r.id, r.date, result.department, result.source,
        result.conflict ? 1 : 0,
        (r.counties || []).join('|'),
        (r.panel || []).length,
      ].map(csvEsc).join(',') + '\n');
    } else {
      reviewed++;
      const reason = r.conflict ? 'county_multi_dept'
        : (r.counties && r.counties.length ? 'county_only_weak'
          : ((r.panel || []).length ? 'panel_only_weak' : 'no_signal'));
      review.write([
        r.id, r.date, reason,
        (r.counties || []).join('|'),
        (r.panel || []).length,
      ].map(csvEsc).join(',') + '\n');
    }
  }
  updates.end();
  review.end();
  await Promise.all([
    new Promise((res) => updates.on('close', res)),
    new Promise((res) => review.on('close', res)),
  ]);

  // report
  const pct = (a, b) => (b ? ((100 * a) / b).toFixed(1) + '%' : '—');
  process.stderr.write(`\nresolve: ${resolved}/${total} resolved (${pct(resolved, total)}); ${reviewed} left for review\n`);
  process.stderr.write(`  by source: ${JSON.stringify(bySource)}\n`);
  process.stderr.write(`  historical-assisted: ${historicalAssisted} (${pct(historicalAssisted, resolved)} of resolved)\n`);
  process.stderr.write(`  by dept:   1=${byDept[1]} 2=${byDept[2]} 3=${byDept[3]} 4=${byDept[4]}\n`);
  process.stderr.write(`  by decade:\n`);
  for (const [d, v] of Object.entries(byDecade).sort()) {
    if (v.n < 25) continue;
    process.stderr.write(`    ${d}: ${String(v.n).padStart(6)}  resolvable ${pct(v.resolved, v.n).padStart(7)}\n`);
  }
  process.stderr.write(`updates  -> ${UPDATES_PATH}\n`);
  process.stderr.write(`review   -> ${REVIEW_PATH}\n`);
}

// ---------------------------------------------------------------------------
// Phase 4: apply
// ---------------------------------------------------------------------------
async function apply() {
  if (!flag('--confirm')) {
    console.error('refusing to write without --confirm');
    process.exit(2);
  }
  if (!fs.existsSync(UPDATES_PATH)) {
    console.error(`missing ${UPDATES_PATH}; run "resolve" first`);
    process.exit(2);
  }
  const rl = readline.createInterface({ input: fs.createReadStream(UPDATES_PATH) });
  const p = pool();

  let header = true, batch = [], totalUpdated = 0, totalSkipped = 0, filtered = 0, batches = 0;
  const t0 = Date.now();
  process.stderr.write(`apply: source filter = {${[...APPLY_SOURCES].join(', ')}}\n`);

  async function flushBatch() {
    if (!batch.length) return;
    batches++;
    const ids = batch.map((b) => b.id);
    const depts = batch.map((b) => b.dept);
    // single-statement UPDATE via VALUES join; idempotent guard on NULL.
    const sql = `
      UPDATE cases AS c
         SET court_department = u.dept,
             updated_at = CURRENT_TIMESTAMP
        FROM UNNEST($1::bigint[], $2::smallint[]) AS u(id, dept)
       WHERE c.id = u.id
         AND c.court_department IS NULL
         AND c.court_name_abbreviation = 'N.Y. App. Div.'
         ${SRC_FILTER}`;
    const res = await p.query(sql, [ids, depts]);
    totalUpdated += res.rowCount;
    totalSkipped += batch.length - res.rowCount;
    const rate = Math.round(totalUpdated / ((Date.now() - t0) / 1000));
    process.stderr.write(`  batch ${batches}: +${res.rowCount} (skipped ${batch.length - res.rowCount}); total ${totalUpdated}  (${rate}/s)\n`);
    batch = [];
  }

  for await (const line of rl) {
    if (header) { header = false; continue; }
    if (!line.trim()) continue;
    const [id, , dept, source] = line.split(',');
    if (!APPLY_SOURCES.has(source)) { filtered++; continue; }
    batch.push({ id, dept: parseInt(dept, 10) });
    if (batch.length >= APPLY_BATCH) await flushBatch();
  }
  await flushBatch();
  await p.end();
  process.stderr.write(`\napply: updated ${totalUpdated} rows; ${totalSkipped} skipped (race/no-op guard); ${filtered} filtered by source\n`);
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------
(async () => {
  switch (cmd) {
    case 'extract': await extract(); break;
    case 'build-roster': await buildCapRoster(); break;
    case 'resolve': await resolve(); break;
    case 'apply': await apply(); break;
    case 'all':
      await extract();
      await buildCapRoster();
      await resolve();
      break;
    default:
      console.error(`usage: node backfillCapDepartments.js <extract|build-roster|resolve|apply|all> [--limit N] [--batch N] [--apply-batch N] [--confirm]`);
      process.exit(2);
  }
})().catch((e) => { console.error(e); process.exit(1); });
