#!/usr/bin/env node
/**
 * Slip-Op Index Resolver
 * ----------------------
 * Read NY Court of Appeals (and similar) monthly index pages (the
 * `/reporter/slipidx/...` archive HTML), extract each (decision_date,
 * case_name, slip_op_cite) tuple, and resolve it against an existing
 * source DB to produce a JSON file ready for bulk-import.
 *
 * Distinct from `main.js` (which scrapes the full slip-op HTML/PDF for
 * opinion content); this resolver only attaches a slip-op citation to a
 * pre-existing case row. Use it when you already have CAP-loaded cases
 * in the source DB and just want to bind the slip-op identifier without
 * reprocessing every per-case document.
 *
 * Input layout (default `in/index/`, override with --in=<dir>):
 *   in/index/<MM_YY_court>.html  e.g. 10_03_coa.html, 10_03_apd.html.
 * Sibling dirs `in/html/` (slip-op HTML pages) and `in/pdf/` (slip-op
 * PDFs) are consumed by main.js, NOT this script.
 *
 * Filename pattern: `MM_YY_<court>.html`. The court code maps to a source
 * DB and a court_id via COURT_SIGNALS below.
 *
 * Resolution algorithm
 *   1. Filter by (court_id, decision_date) — narrows to ~5–30 cases per
 *      day on a busy court.
 *   2. Score every candidate on `GREATEST(similarity(name), similarity(name_abbreviation))`,
 *      tiebreak on name_score, then abbr_score.
 *   3. Mark "resolved" when the top candidate clears SCORE_FLOOR AND
 *      has a margin of MARGIN_FLOOR over runner-up, OR (when tied on
 *      best_score) clears NAME_MARGIN_FLOOR via name_score.
 *   4. When several DB rows match (motion-order + substantive-opinion
 *      pattern), prefer the row with the most citations and the widest
 *      page span — the substantive opinion always wins, motion orders
 *      are short single-page entries with one citation.
 *
 * Output JSON shape
 *   {
 *     "schema_version": "0.1",
 *     "generated_at": "...",
 *     "input_files": [...],
 *     "stats": {...},
 *     "resolved":   [{slip_op_curie, slip_op_cite, target_case_id, target_case_curie, ...}],
 *     "unresolved": [{slip_op_curie, slip_op_cite, candidates: [...], reason, ...}]
 *   }
 *
 * The bulk-import side (Phase 2, separate task) consumes this JSON: for
 * each `resolved` entry it inserts a slip-op citation row pointing at
 * `target_case_id`; for each `unresolved` entry it writes a row to a
 * match-candidates queue table for human triage in the admin UI.
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import * as cheerio from 'cheerio';
import pg from 'pg';
import { parseLrbDate } from '../src/shared.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const SCHEMA_VERSION = '0.1';

// Resolution thresholds. Tuned against a 218-entry CoA sample (Oct 2003
// through Dec 2004); 99.5% top-match correctness with these.
const SCORE_FLOOR = 0.25;        // top.best_score must clear this
const MARGIN_FLOOR = 0.10;       // best_score margin to runner-up
const NAME_MARGIN_FLOOR = 0.05;  // name_score tiebreaker when best_scores tie

/**
 * Court signal table. Maps the `_<code>` token in the filename and the
 * page <h1> to the source DB + court_id pair we'll resolve against. The
 * filename gives a strong narrowing signal; the H1 cross-checks it so a
 * mistyped filename (or a mid-page redirect) doesn't quietly resolve
 * against the wrong court.
 */
/**
 * Each entry maps a filename `_<code>` token to the source DB the entries
 * land in. The CoA case is simple — one court, one court_id. The AD case
 * is multi-department: the four departments share court_id=8994 in CAP
 * (only court_department differs), and CAP didn't populate
 * court_department on older rows (2003 has 0% coverage). So we detect
 * the department from the page <h1> for downstream display, but don't
 * use it as a narrowing filter unless the DB row also has it populated.
 */
const COURT_SIGNALS = {
  coa: {
    db: 'ny_reporter',
    source_ref: 'ny_supreme',
    court_id: 24653,
    expected_h1: /Court of Appeals/i,
    label: 'NY Court of Appeals',
    detectDepartment: () => null,
  },
  apd: {
    db: 'ny_appellate_division',
    source_ref: 'ny_appellate',
    court_id: 8994,
    expected_h1: /Appellate Division/i,
    label: 'NY Supreme Court, Appellate Division',
    detectDepartment: (h1) => {
      const m = h1.match(/(First|Second|Third|Fourth)\s+Department/i);
      if (!m) return null;
      const map = { first: 1, second: 2, third: 3, fourth: 4 };
      return map[m[1].toLowerCase()];
    },
  },
};

// ---------- HTML parse ----------

/**
 * Locate Title and Slip Opinion column indices by reading <th> headers.
 * Layouts seen: 3-col 2003 (Title|Judge|Slip Op), 4-col 2004+ (Title|
 * Judge|Docket|Slip Op). Falls back to fixed positions when headers
 * are absent.
 */
function locateColumns($, $table) {
  const ths = $table.find('th').toArray().map(th => $(th).text().trim());
  let titleIdx = ths.findIndex(t => /^Title$/i.test(t));
  let slipIdx  = ths.findIndex(t => /Slip\s*Opinion/i.test(t));
  if (titleIdx < 0) titleIdx = 0;
  if (slipIdx  < 0) slipIdx  = ths.length === 4 ? 3 : 2;
  return { titleIdx, slipIdx };
}

/**
 * Pre-normalise the index title for similarity comparison. CAP stores
 * "In re Smith" while the LRB index says "Matter of Smith"; rewriting
 * the index side moves it toward the DB form. Whitespace is collapsed
 * to keep trigram tokenisation consistent across line-wrap noise.
 */
function normalizeIndexName(s) {
  return String(s)
    .replace(/^In the Matter of\b/i, 'In re')
    .replace(/^Matter of\b/i, 'In re')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseSlipOpFromText(text) {
  // Index variants: "2003 NYSlipOp 17894", "2004 NY Slip Op 09408".
  const m = text.match(/(\d{4})\s*NY\s*Slip\s*Op\s*(\d+)/i);
  if (!m) return null;
  const year = Number(m[1]);
  const num = String(parseInt(m[2], 10));
  return {
    year,
    number: num,
    cite: `${year} NY Slip Op ${m[2].padStart(5, '0')}`,
    curie: `nyslopop:${year}:${num}`,
  };
}

function parseIndexFile(filePath) {
  const html = fs.readFileSync(filePath, 'utf8');
  const $ = cheerio.load(html);

  const fname = path.basename(filePath);
  const m = fname.match(/^(\d{2})_(\d{2})_([a-z]+)\.html$/i);
  if (!m) throw new Error(`Filename "${fname}" does not match MM_YY_<court>.html`);
  const courtCode = m[3].toLowerCase();
  const sig = COURT_SIGNALS[courtCode];
  if (!sig) throw new Error(`Unknown court code "${courtCode}" in ${fname}`);
  const h1 = $('h1').first().text().trim();
  if (!sig.expected_h1.test(h1)) {
    throw new Error(`H1 "${h1}" doesn't match expected ${sig.expected_h1} for code ${courtCode}`);
  }
  const courtDepartment = sig.detectDepartment ? sig.detectDepartment(h1) : null;

  const entries = [];
  $('table').each((_, tbl) => {
    const $t = $(tbl);
    const captionText = $t.find('caption').text().trim();
    const cm = captionText.match(/Cases Decided\s+(.+)/i);
    if (!cm) return;
    const decisionDate = parseLrbDate(cm[1]);
    if (!decisionDate) return;

    const { titleIdx, slipIdx } = locateColumns($, $t);

    $t.find('tr').each((_, tr) => {
      const tds = $(tr).find('td');
      if (tds.length === 0) return;
      const titleText = $(tds[titleIdx]).text().replace(/\s+/g, ' ').trim();
      const slipText  = $(tds[slipIdx]).text().trim();
      if (!titleText || !slipText) return;
      const slip = parseSlipOpFromText(slipText);
      if (!slip) return;  // header row or malformed — skip
      entries.push({
        court_id: sig.court_id,
        court_department: courtDepartment,
        source_db: sig.db,
        source_ref: sig.source_ref,
        decision_date: decisionDate,
        index_name: titleText,
        slip_op_year: slip.year,
        slip_op_number: slip.number,
        slip_op_cite: slip.cite,
        slip_op_curie: slip.curie,
        url: $(tds[titleIdx]).find('a').attr('href') || null,
      });
    });
  });
  return { sig, entries, source_file: fname };
}

// ---------- DB resolve ----------

/**
 * Run the per-entry similarity query. Returns up to 5 candidates with
 * scores AND richness signals (citation_count, page_span,
 * total_opinion_text_len). The richness signals let us pick the
 * substantive opinion when memorandum-order and opinion rows share a
 * date+court (the index slip-op cite always belongs to the
 * substantive one — motion orders are short single-page entries).
 */
async function fetchCandidates(client, e, { merged = false } = {}) {
  const probe = normalizeIndexName(e.index_name);
  // Department filter is intentionally permissive: $4 = NULL means we
  // don't know (e.g., CoA), and rows where c.court_department IS NULL
  // pass even when we DO know — older CAP imports left it NULL across
  // the board. Tightens automatically as the DB backfills the column.
  // Under merged target, also filter by source_ref so we don't bleed
  // across source partitions (the merged DB unions all three).
  const srcFilter = merged ? `AND c.source_ref = $5` : '';
  const params = merged
    ? [probe, e.court_id, e.decision_date, e.court_department ?? null, e.source_ref]
    : [probe, e.court_id, e.decision_date, e.court_department ?? null];
  const { rows } = await client.query(
    `SELECT c.id,
            c.name_abbreviation,
            c.name,
            c.first_page,
            c.last_page,
            c.court_department,
            similarity(c.name, $1) AS name_score,
            similarity(c.name_abbreviation, $1) AS abbr_score,
            GREATEST(similarity(c.name, $1), similarity(c.name_abbreviation, $1)) AS best_score,
            (SELECT count(*) FROM citations WHERE case_id = c.id) AS citation_count,
            (SELECT COALESCE(sum(LENGTH(text)), 0) FROM opinions WHERE case_id = c.id) AS opinion_text_len
       FROM cases c
      WHERE c.court_id = $2
        AND c.decision_date = $3
        AND ($4::int IS NULL OR c.court_department IS NULL OR c.court_department = $4)
        ${srcFilter}
      ORDER BY best_score DESC, name_score DESC, abbr_score DESC
      LIMIT 10`,
    params
  );
  return rows.map(r => ({
    case_id: Number(r.id),
    name_abbreviation: r.name_abbreviation,
    name: r.name,
    first_page: r.first_page,
    last_page: r.last_page,
    court_department: r.court_department,
    name_score: Number(r.name_score),
    abbr_score: Number(r.abbr_score),
    best_score: Number(r.best_score),
    citation_count: Number(r.citation_count),
    opinion_text_len: Number(r.opinion_text_len),
  }));
}

/**
 * Compute a richness score for a candidate. Used to break ties when
 * multiple distinct DB rows share name+date (typically a motion-order
 * row alongside the substantive opinion row). The substantive row
 * always has 3 citations (NY3d official + NYS2d + NE2d parallels) and
 * a multi-page span; motion orders have 1 citation and a single page.
 */
function richness(c) {
  const pageSpan = (Number(c.last_page) || 0) - (Number(c.first_page) || 0);
  return {
    citations: c.citation_count,
    pageSpan: Number.isFinite(pageSpan) ? pageSpan : 0,
    textLen: c.opinion_text_len,
  };
}

function compareRichness(a, b) {
  const ra = richness(a), rb = richness(b);
  if (ra.citations !== rb.citations) return rb.citations - ra.citations;
  if (ra.pageSpan  !== rb.pageSpan)  return rb.pageSpan  - ra.pageSpan;
  if (ra.textLen   !== rb.textLen)   return rb.textLen   - ra.textLen;
  return a.case_id - b.case_id;  // deterministic final tiebreaker
}

/**
 * Decide whether the candidate set yields a confident match.
 *
 * Returns one of:
 *   { status: 'resolved', target, alternates, reason }
 *   { status: 'unresolved', candidates, reason }
 *
 * `alternates` is the set of OTHER same-date+court rows that scored as
 * high as the chosen target — surface them so a Phase 2 reviewer can
 * see the motion-order/substantive split if it matters.
 */
function decide(candidates) {
  if (candidates.length === 0) {
    return { status: 'unresolved', candidates: [], reason: 'no_candidates_on_date' };
  }
  const top = candidates[0];
  const second = candidates[1];

  if (top.best_score < SCORE_FLOOR) {
    return { status: 'unresolved', candidates, reason: 'top_below_score_floor' };
  }

  const margin = second ? (top.best_score - second.best_score) : top.best_score;
  if (margin >= MARGIN_FLOOR) {
    return { status: 'resolved', target: top, alternates: [], reason: 'decisive_margin' };
  }

  // Tied on best_score (typically same name_abbreviation among tied rows).
  // The candidates fall into one of three patterns; we test in order:
  //
  //   1. Asymmetric citation_count — substantive opinion (3 cites: NY3d
  //      official + NYS2d + NE2d) vs motion order (1 cite). The slip-op
  //      cite always points at the substantive entry, so prefer the
  //      higher citation count regardless of name_score. Critically:
  //      this MUST run before the name_score tiebreak — motion orders
  //      can have shorter `name` text than the substantive (no "et al.,
  //      Respondents" verbosity), which trgm rewards with a higher
  //      name_score even though the row is the wrong target.
  //
  //   2. Symmetric richness, name_score breaks it — different cases
  //      sharing an abbreviation (e.g., two "People v. Johnson" on the
  //      same day, distinguished by "Robert" vs "James" in the full
  //      caption). Both rows are full substantive opinions with the
  //      same citation count.
  //
  //   3. All signals symmetric — true CAP row duplicates (Moore's
  //      identical "Judge Graffeo taking no part" entries). Pick lowest
  //      case_id; surface the others as alternates for downstream dedup.
  //
  // Tied set membership: a row qualifies if it ties on best_score OR on
  // abbr_score. Including abbr-ties matters because the substantive
  // opinion can have a `name` so padded with party designations
  // ("...as Chair of the New York State Division of Parole, Respondent")
  // that its `name_score` falls below the motion row's, even though
  // both share the same abbreviation. Without this, the substantive is
  // excluded from richness comparison and the motion wins inadvertently.
  const tiedSet = candidates.filter(c =>
    Math.abs(c.best_score - top.best_score) <= 0.001 ||
    Math.abs(c.abbr_score - top.abbr_score) <= 0.001
  );
  if (tiedSet.length === 1) {
    // Top is alone above the next candidate but didn't clear MARGIN_FLOOR —
    // genuine low-confidence case, not a tied-rows scenario. Defer to
    // human review rather than auto-link a borderline match.
    return { status: 'unresolved', candidates, reason: 'low_confidence' };
  }

  const maxCites = Math.max(...tiedSet.map(c => c.citation_count));
  const minCites = Math.min(...tiedSet.map(c => c.citation_count));
  if (maxCites > minCites) {
    const rich = tiedSet.filter(c => c.citation_count === maxCites);
    rich.sort(compareRichness);
    return {
      status: 'resolved',
      target: rich[0],
      alternates: tiedSet.filter(c => c.case_id !== rich[0].case_id),
      reason: 'richness_tiebreak',
    };
  }

  const nameMargin = second ? (top.name_score - second.name_score) : top.name_score;
  if (nameMargin >= NAME_MARGIN_FLOOR) {
    return { status: 'resolved', target: top, alternates: [], reason: 'name_score_tiebreak' };
  }

  // Identical-looking rows. Lowest case_id wins; surface alternates.
  const sorted = [...tiedSet].sort((a, b) => a.case_id - b.case_id);
  return {
    status: 'resolved',
    target: sorted[0],
    alternates: sorted.slice(1),
    reason: 'identical_rows_lowest_id',
  };
}

/**
 * Look up the EXISTING DB curie on the official citation for the chosen
 * case. The bulk-import path can use this for tier-1 (CURIE) match
 * against the slip-op upload, alongside the explicit case_id pointer.
 */
async function fetchExistingCurie(client, caseId) {
  const { rows } = await client.query(
    `SELECT cite, curie
       FROM citations
      WHERE case_id = $1 AND citation_type = 'official'
      ORDER BY id ASC
      LIMIT 1`,
    [caseId]
  );
  return rows[0] || null;
}

// ---------- main ----------

function parseArgs(argv) {
  const args = { in: path.join(ROOT, 'in', 'index'), out: null };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const m = a.match(/^--([a-z-]+)(?:=(.*))?$/);
    if (!m) continue;
    const key = m[1].replace(/-/g, '_');
    const val = m[2] ?? argv[++i];
    args[key] = val;
  }
  return args;
}

const args = parseArgs(process.argv);
// Default points at in/index/. The slip-op-extractor's `in/` is split into
// three sibling subdirs by input type: index/ (this script), html/ and
// pdf/ (consumed by main.js). Keeping them apart prevents an opinion
// HTML from being mistaken for an index page (or vice versa).
const inDir = path.resolve(args.in);
const outDir = args.out_dir
  ? path.resolve(args.out_dir)
  : path.join(ROOT, 'out', 'index-resolution');

function ts() {
  const d = new Date();
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}

const indexFiles = fs.readdirSync(inDir)
  .filter(f => /^\d{2}_\d{2}_[a-z]+\.html$/i.test(f))
  .sort()
  .map(f => path.join(inDir, f));

if (indexFiles.length === 0) {
  console.error(`No index files matching MM_YY_<court>.html in ${inDir}`);
  process.exit(1);
}

console.log(`Resolving ${indexFiles.length} index files from ${inDir}\n`);

// Group entries by source DB so we open one pool per DB.
const allEntries = [];
const byDb = new Map();
for (const file of indexFiles) {
  const { sig, entries, source_file } = parseIndexFile(file);
  console.log(`  ${source_file}  (${sig.label}): ${entries.length} entries`);
  for (const e of entries) {
    e.source_file = source_file;
    allEntries.push(e);
    if (!byDb.has(sig.db)) byDb.set(sig.db, []);
    byDb.get(sig.db).push(e);
  }
}

// One result bucket per source_ref. The bulk-import endpoint expects a
// single source per upload, so we partition output along that dimension —
// mixing CoA and AD entries in one JSON would just force the operator to
// split it before upload.
const resultsByRef = new Map(); // source_ref → { resolved: [], unresolved: [] }
function bucketFor(sourceRef) {
  if (!resultsByRef.has(sourceRef)) {
    resultsByRef.set(sourceRef, { resolved: [], unresolved: [] });
  }
  return resultsByRef.get(sourceRef);
}

const PG_DEFAULTS = {
  host: process.env.PGHOST || 'localhost',
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER || 'claude',
  password: process.env.PGPASSWORD || 'claude',
};

// Track B / B3 §1: --target=ny_caselaw routes all per-source-DB reads to the
// merged ny_caselaw DB (via MERGE_TARGET_URL). source_ref filter on
// fetchCandidates keeps results scoped to the correct source partition.
const MERGED = args.target === 'ny_caselaw';
if (MERGED && !process.env.MERGE_TARGET_URL) {
  console.error('--target=ny_caselaw requires MERGE_TARGET_URL env var.');
  process.exit(2);
}
const mergedPool = MERGED ? new pg.Pool({ connectionString: process.env.MERGE_TARGET_URL }) : null;
if (MERGED) {
  console.log(`Target: MERGED DB (ny_caselaw) via MERGE_TARGET_URL — all source DBs route to one pool`);
}

for (const [db, entries] of byDb) {
  console.log(`\nResolving ${entries.length} entries originally keyed for ${db}${MERGED ? ' (routed to merged DB)' : ''}...`);
  const pool = MERGED ? mergedPool : new pg.Pool({ ...PG_DEFAULTS, database: db });
  const client = await pool.connect();
  try {
    for (const e of entries) {
      const candidates = await fetchCandidates(client, e, { merged: MERGED });
      const decision = decide(candidates);
      const bucket = bucketFor(e.source_ref);

      const baseRecord = {
        slip_op_curie: e.slip_op_curie,
        slip_op_cite: e.slip_op_cite,
        decision_date: e.decision_date,
        court_id: e.court_id,
        court_department: e.court_department,
        source_db: e.source_db,
        source_ref: e.source_ref,
        index: {
          source_file: e.source_file,
          title: e.index_name,
          url: e.url,
        },
      };

      if (decision.status === 'resolved') {
        const existingCite = await fetchExistingCurie(client, decision.target.case_id);
        bucket.resolved.push({
          ...baseRecord,
          target_case_id: decision.target.case_id,
          target_case_curie: existingCite?.curie ?? null,
          target_official_cite: existingCite?.cite ?? null,
          target_name_abbreviation: decision.target.name_abbreviation,
          match: {
            reason: decision.reason,
            best_score: round3(decision.target.best_score),
            name_score: round3(decision.target.name_score),
            abbr_score: round3(decision.target.abbr_score),
            citation_count: decision.target.citation_count,
            page_first: decision.target.first_page,
            page_last: decision.target.last_page,
          },
          alternates: decision.alternates.map(a => ({
            case_id: a.case_id,
            name_abbreviation: a.name_abbreviation,
            best_score: round3(a.best_score),
            citation_count: a.citation_count,
            page_first: a.first_page,
            page_last: a.last_page,
          })),
        });
      } else {
        bucket.unresolved.push({
          ...baseRecord,
          reason: decision.reason,
          candidates: decision.candidates.map(c => ({
            case_id: c.case_id,
            name_abbreviation: c.name_abbreviation,
            name: c.name,
            best_score: round3(c.best_score),
            name_score: round3(c.name_score),
            abbr_score: round3(c.abbr_score),
            citation_count: c.citation_count,
            page_first: c.first_page,
            page_last: c.last_page,
          })),
        });
      }
    }
  } finally {
    client.release();
    // Under merged, the shared mergedPool is closed once after the loop.
    if (!MERGED) await pool.end();
  }
}
if (MERGED && mergedPool) await mergedPool.end();

function round3(n) { return Math.round(Number(n) * 1000) / 1000; }
function tally(arr, key) {
  const out = {};
  for (const x of arr) {
    const k = key(x);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

// Per-source output. One JSON file per source_ref so each is uploadable
// to the bulk-import endpoint independently. Filenames encode source +
// timestamp so re-running on the same day creates a new file rather
// than overwriting.
const stamp = ts();
fs.mkdirSync(outDir, { recursive: true });
const written = [];
const totalsBySource = {};

for (const [sourceRef, bucket] of resultsByRef) {
  const total = bucket.resolved.length + bucket.unresolved.length;
  // Files which contributed entries to this source — useful for audit.
  const filesForSource = [...new Set(
    [...bucket.resolved, ...bucket.unresolved].map(e => e.index?.source_file).filter(Boolean)
  )].sort();
  const stats = {
    total,
    resolved: bucket.resolved.length,
    unresolved: bucket.unresolved.length,
    resolved_by_reason: tally(bucket.resolved, r => r.match.reason),
    unresolved_by_reason: tally(bucket.unresolved, r => r.reason),
  };
  const output = {
    schema_version: SCHEMA_VERSION,
    generated_at: new Date().toISOString(),
    source_ref: sourceRef,
    input_dir: inDir,
    input_files: filesForSource,
    thresholds: {
      score_floor: SCORE_FLOOR,
      margin_floor: MARGIN_FLOOR,
      name_margin_floor: NAME_MARGIN_FLOOR,
    },
    stats,
    resolved: bucket.resolved,
    unresolved: bucket.unresolved,
  };
  const outFile = path.join(outDir, `resolved-${sourceRef}-${stamp}.json`);
  fs.writeFileSync(outFile, JSON.stringify(output, null, 2));
  written.push({ outFile, sourceRef, stats });
  totalsBySource[sourceRef] = stats;
}

console.log(`\n=== Resolution complete ===`);
const grandTotal = Object.values(totalsBySource).reduce((s, x) => s + x.total, 0);
const grandResolved = Object.values(totalsBySource).reduce((s, x) => s + x.resolved, 0);
const grandUnresolved = Object.values(totalsBySource).reduce((s, x) => s + x.unresolved, 0);
console.log(`  total entries across all sources: ${grandTotal}`);
console.log(`  resolved   ${grandResolved}  ${pct(grandResolved, grandTotal)}`);
console.log(`  unresolved ${grandUnresolved}  ${pct(grandUnresolved, grandTotal)}`);
console.log(``);
for (const w of written) {
  console.log(`  ${w.sourceRef.padEnd(14)} → ${w.outFile}`);
  console.log(`    total=${w.stats.total} resolved=${w.stats.resolved} unresolved=${w.stats.unresolved}`);
  for (const [k, v] of Object.entries(w.stats.resolved_by_reason)) {
    console.log(`      ${k.padEnd(28)} ${v}`);
  }
  for (const [k, v] of Object.entries(w.stats.unresolved_by_reason)) {
    console.log(`      ${k.padEnd(28)} ${v}`);
  }
}

function pct(n, d) { return d ? `(${(100*n/d).toFixed(1)}%)` : ''; }
