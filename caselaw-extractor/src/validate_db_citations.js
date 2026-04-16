#!/usr/bin/env node

/**
 * DB Citation Sanity Check
 *
 * Cross-checks the official citation stored in the DB against the ground truth
 * encoded in the extracted-JSON filesystem layout:
 *
 *   data/extracted/<source_id>/<volume>.json
 *
 * The source directory dictates the reporter edition (per config), and the
 * filename encodes the volume. For every case in every JSON file we expect the
 * DB's official citation to parse as `{volume: <fileVol>, reporter: <config>, page: <first_page>}`.
 *
 * Flags four failure modes:
 *   - case_missing_from_db     JSON case has no row in cases
 *   - db_missing_official      Case in DB but no official citation row
 *   - db_reporter_mismatch     DB reporter differs from config (the CAP bug we're hunting)
 *   - db_volume_or_page_mismatch  DB volume/page differs from folder/first_page
 *   - db_cite_parse_error      DB cite unparseable
 *
 * Read-only. No writes under any flag.
 */

import fs from 'fs/promises';
import path from 'path';
import pg from 'pg';

const { Pool } = pg;

function usage() {
  console.log(`
Validate DB Citations Against Extracted JSON

USAGE:
  node src/validate_db_citations.js -c <config.json> [--source <id>] [--vol <n>] [--sample] [-v] [-o <out.log>]

OPTIONS:
  -c, --config <file>  Config JSON with database + sources[].{id, reporter}
  --source <id>        Restrict to a single source id (e.g. ny3d)
  --vol <n>            Restrict to a single volume within a source
  --sample             Process only the first file per source
  -v, --verbose        Verbose progress output
  -o, --out <file>     Output log file path (default: logs/validate-db-<config>-<timestamp>.log)
`);
}

function getCliArgs() {
  const args = process.argv.slice(2);
  const opts = { config: null, source: null, vol: null, sample: false, verbose: false, out: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case '-c':
      case '--config':    opts.config = args[++i]; break;
      case '--source':    opts.source = args[++i]; break;
      case '--vol':
      case '--volume':    opts.vol = parseInt(args[++i], 10); break;
      case '--sample':    opts.sample = true; break;
      case '-v':
      case '--verbose':   opts.verbose = true; break;
      case '-o':
      case '--out':       opts.out = args[++i]; break;
      case '-h':
      case '--help':      usage(); process.exit(0);
      default:
        if (!opts.config && !a.startsWith('-')) opts.config = a;
        break;
    }
  }
  if (!opts.config) {
    console.error('ERROR: --config is required');
    usage();
    process.exit(1);
  }
  return opts;
}

function normalizeReporter(r) {
  return (r || '').toString().replace(/\s+/g, '').replace(/[.]/g, '').toUpperCase();
}

// Greedy reporter match so embedded whitespace ("Misc. 2d") and series
// suffixes ("2d", "3d") stay attached. Page anchors to the last \d+.
function parseVolumeReporterPage(cite) {
  if (!cite || typeof cite !== 'string') return null;
  const m = cite.match(/^(\d+)\s+(.+)\s+(\d+)(.*)$/);
  if (!m) return null;
  return {
    volume: parseInt(m[1], 10),
    reporter: m[2].replace(/\s+/g, ' ').trim(),
    page: parseInt(m[3], 10),
  };
}

async function ensureDir(dir) {
  try { await fs.mkdir(dir, { recursive: true }); } catch (_) {}
}

async function loadConfig(configPath) {
  const content = await fs.readFile(configPath, 'utf8');
  return JSON.parse(content);
}

function createPool(dbConfig) {
  return new Pool({
    host: process.env.PGHOST || dbConfig.host,
    port: process.env.PGPORT || dbConfig.port,
    database: process.env.PGDATABASE || dbConfig.database,
    user: process.env.PGUSER || dbConfig.user,
    password: process.env.PGPASSWORD || dbConfig.password,
    max: 4,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });
}

// Bulk-load a Map<case_id, {cite, decision_date}> for all cases in a source.
// We identify a source's cases by matching the extracted JSON ids we collect first.
async function loadDbOfficialCites(pool, caseIds, verbose) {
  if (caseIds.length === 0) return new Map();
  if (verbose) console.log(`   Loading DB citations for ${caseIds.length} cases...`);

  const result = new Map();
  // Chunk to keep parameter count reasonable
  const chunkSize = 10000;
  for (let i = 0; i < caseIds.length; i += chunkSize) {
    const chunk = caseIds.slice(i, i + chunkSize);
    const res = await pool.query(
      `SELECT c.id AS case_id, c.decision_date,
              (SELECT cite FROM citations
                 WHERE case_id = c.id AND citation_type = 'official'
                 ORDER BY id LIMIT 1) AS official_cite
         FROM cases c
         WHERE c.id = ANY($1::bigint[])`,
      [chunk]
    );
    for (const row of res.rows) {
      result.set(String(row.case_id), {
        cite: row.official_cite,
        decision_date: row.decision_date,
      });
    }
  }
  return result;
}

async function processSource(pool, srcId, expectedReporter, options, log, summary) {
  const extractedDir = path.join(process.cwd(), 'data', 'extracted', srcId);
  let files;
  try {
    files = (await fs.readdir(extractedDir)).filter(f => f.endsWith('.json'));
  } catch (e) {
    log.push(`[source=${srcId}] ERROR: extracted dir not found: ${extractedDir}`);
    return;
  }
  files.sort((a, b) => parseInt(a, 10) - parseInt(b, 10));

  let toProcess = files;
  if (Number.isFinite(options.vol)) {
    toProcess = files.filter(f => parseInt(path.basename(f, '.json'), 10) === options.vol);
  } else if (options.sample) {
    toProcess = files.slice(0, 1);
  }

  if (options.verbose) {
    console.log(`[${srcId}] expected_reporter="${expectedReporter}" files=${toProcess.length}/${files.length}${options.sample && !Number.isFinite(options.vol) ? ' (sample)' : ''}`);
  }

  // First pass: read all JSON, build expected records.
  const expectations = []; // {caseId, fileVol, expectedCite, jsonOfficialCite, name, decisionDateJson}
  for (const file of toProcess) {
    const fileVol = parseInt(path.basename(file, '.json'), 10);
    const filePath = path.join(extractedDir, file);
    let arr;
    try {
      arr = JSON.parse(await fs.readFile(filePath, 'utf8'));
    } catch (e) {
      log.push(`[source=${srcId}] file=${file} ERROR: json_parse_failed msg=${e.message}`);
      continue;
    }
    if (!Array.isArray(arr)) {
      log.push(`[source=${srcId}] file=${file} ERROR: not_an_array`);
      continue;
    }
    for (const c of arr) {
      const citations = Array.isArray(c?.citations) ? c.citations : [];
      const official = citations.find(x => (x.type || x.citation_type) === 'official');
      const firstPage = c?.first_page != null ? parseInt(String(c.first_page), 10) : null;
      expectations.push({
        caseId: String(c.id),
        fileVol,
        expectedCite: Number.isFinite(firstPage) ? `${fileVol} ${expectedReporter} ${firstPage}` : null,
        expectedPage: firstPage,
        jsonOfficialCite: official?.cite || null,
        name: c?.name_abbreviation || c?.name || null,
        decisionDateJson: c?.decision_date || null,
      });
    }
  }

  summary.sources[srcId].cases = expectations.length;

  // Bulk-load DB state for these case ids.
  const caseIds = expectations.map(e => e.caseId);
  const dbMap = await loadDbOfficialCites(pool, caseIds, options.verbose);

  // Second pass: compare.
  for (const exp of expectations) {
    const dbRow = dbMap.get(exp.caseId);
    if (!dbRow) {
      summary.sources[srcId].case_missing_from_db++;
      log.push(`[source=${srcId}] vol=${exp.fileVol} case_id=${exp.caseId} issue=case_missing_from_db name=${JSON.stringify(exp.name)}`);
      continue;
    }
    if (!dbRow.cite) {
      summary.sources[srcId].db_missing_official++;
      log.push(`[source=${srcId}] vol=${exp.fileVol} case_id=${exp.caseId} issue=db_missing_official expected=${JSON.stringify(exp.expectedCite)} json_cite=${JSON.stringify(exp.jsonOfficialCite)}`);
      continue;
    }

    const parsed = parseVolumeReporterPage(dbRow.cite);
    if (!parsed) {
      summary.sources[srcId].db_cite_parse_error++;
      log.push(`[source=${srcId}] vol=${exp.fileVol} case_id=${exp.caseId} issue=db_cite_parse_error db_cite=${JSON.stringify(dbRow.cite)} expected=${JSON.stringify(exp.expectedCite)}`);
      continue;
    }

    const reporterMatch = normalizeReporter(parsed.reporter) === normalizeReporter(expectedReporter);
    const volumeMatch = Number.isFinite(exp.fileVol) && parsed.volume === exp.fileVol;
    const pageMatch = !Number.isFinite(exp.expectedPage) || parsed.page === exp.expectedPage;

    if (!reporterMatch) {
      summary.sources[srcId].db_reporter_mismatch++;
      log.push(`[source=${srcId}] vol=${exp.fileVol} case_id=${exp.caseId} issue=db_reporter_mismatch db_cite=${JSON.stringify(dbRow.cite)} db_reporter=${JSON.stringify(parsed.reporter)} expected_reporter=${JSON.stringify(expectedReporter)} decision_date=${dbRow.decision_date}`);
      continue;
    }
    if (!volumeMatch || !pageMatch) {
      summary.sources[srcId].db_volume_or_page_mismatch++;
      log.push(`[source=${srcId}] vol=${exp.fileVol} case_id=${exp.caseId} issue=db_volume_or_page_mismatch db_cite=${JSON.stringify(dbRow.cite)} db_vol=${parsed.volume} db_page=${parsed.page} expected_vol=${exp.fileVol} expected_page=${exp.expectedPage}`);
      continue;
    }

    summary.sources[srcId].ok++;
  }
}

function newSourceStats() {
  return {
    cases: 0,
    ok: 0,
    case_missing_from_db: 0,
    db_missing_official: 0,
    db_cite_parse_error: 0,
    db_reporter_mismatch: 0,
    db_volume_or_page_mismatch: 0,
  };
}

function formatSummary(summary) {
  const rows = [];
  rows.push('');
  rows.push('=== SUMMARY ===');
  const cols = ['cases', 'ok', 'case_missing_from_db', 'db_missing_official', 'db_cite_parse_error', 'db_reporter_mismatch', 'db_volume_or_page_mismatch'];
  for (const srcId of Object.keys(summary.sources)) {
    const s = summary.sources[srcId];
    rows.push(`[${srcId}] ` + cols.map(c => `${c}=${s[c]}`).join(' '));
  }
  rows.push('');
  return rows.join('\n');
}

async function main() {
  const options = getCliArgs();
  const config = await loadConfig(options.config);

  const cfgBase = path.basename(options.config, '.json');
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const logsDir = path.join(process.cwd(), 'logs');
  await ensureDir(logsDir);
  const outPath = options.out || path.join(logsDir, `validate-db-${cfgBase}-${ts}.log`);

  const pool = createPool(config.database);
  const summary = { sources: {} };
  const log = [];

  try {
    const sources = Array.isArray(config.sources) ? config.sources : [];
    for (const source of sources) {
      const srcId = source.id;
      if (options.source && options.source !== srcId) continue;
      const expectedReporter = source.reporter;
      if (!expectedReporter) {
        log.push(`[source=${srcId}] ERROR: config.sources[].reporter is missing; skipping`);
        continue;
      }
      summary.sources[srcId] = newSourceStats();
      await processSource(pool, srcId, expectedReporter, options, log, summary);
    }
  } finally {
    await pool.end();
  }

  const summaryText = formatSummary(summary);
  await fs.writeFile(outPath, log.join('\n') + '\n' + summaryText + '\n', 'utf8');
  console.log(summaryText);
  console.log(`\nFull log: ${outPath}`);
  console.log(`Findings: ${log.length}`);
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
