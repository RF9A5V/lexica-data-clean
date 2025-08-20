/**
 * Regex-based citation extractor for opinion texts
 * - Parses citations like: "Foo v. Bar, 123 N.Y.3d 456" or "123 N.Y. 456"
 * - Normalizes reporter strings using allowlist at configs/reporters.json
 * - Upserts extracted rows into collection app DB table extracted_citations
 */

import fs from 'fs/promises';
import path from 'path';
import pg from 'pg';
import dotenv from 'dotenv';
import { loadConfig } from './config_file.js';

// Load env
dotenv.config();

const { Pool } = pg;

// ---------- Reporter config ----------
async function loadReporterConfig() {
  const cfgPath = path.resolve('./configs/reporters.json');
  const raw = await fs.readFile(cfgPath, 'utf8');
  const cfg = JSON.parse(raw);
  return cfg;
}

function buildReporterIndex(cfg) {
  const rules = cfg.normalize || { removeDots: true, removeSpaces: true, uppercase: true };
  const aliasToNorm = new Map();
  const tokens = new Set();

  const normToken = (s) => {
    let t = s || '';
    if (rules.removeDots) t = t.replace(/\./g, '');
    if (rules.removeSpaces) t = t.replace(/\s+/g, '');
    if (rules.uppercase) t = t.toUpperCase();
    return t;
  };

  for (const rep of cfg.reporters || []) {
    const entries = new Set([rep.canonical, ...(rep.aliases || [])]);
    for (const e of entries) {
      const key = normToken(e);
      aliasToNorm.set(key, rep.norm);
      tokens.add(e);
    }
    // Ensure canonical also maps
    aliasToNorm.set(normToken(rep.canonical), rep.norm);
  }

  // Build regex alternation of reporter tokens (escaped, flexible spaces)
  const escapeRegex = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const tokenPatterns = Array.from(tokens).map((t) => {
    // allow optional spaces after dots and between segments
    const escaped = escapeRegex(t)
      .replace(/\\\./g, '\\.?') // optional dots
      .replace(/\s+/g, '\\s*'); // flexible spaces
    return `(?:${escaped})`;
  });
  const reporterAlt = tokenPatterns.length ? tokenPatterns.join('|') : '[A-Za-z\\.\\s]+';

  // Denylist combined regex
  const denylist = (cfg.denylist_patterns || []).map((p) => new RegExp(p, 'i'));

  return { rules, aliasToNorm, reporterAlt, denylist, normToken };
}

// ---------- Text normalization ----------
function normalizeOpinionText(text) {
  if (!text) return '';
  let t = text;
  // Unicode normalize
  try { t = t.normalize('NFKC'); } catch {}
  // de-hyphenation across line breaks
  t = t.replace(/-\s*\n\s*/g, '');
  // normalize line breaks to spaces
  t = t.replace(/[\r\n]+/g, ' ');
  // collapse whitespace
  t = t.replace(/\s+/g, ' ').trim();
  return t;
}

// ---------- Citation extraction ----------
function extractCitationsFromText(text, repIndex) {
  const results = [];
  if (!text) return results;
  const t = normalizeOpinionText(text);

  // Optional case name (greedy but bounded), then volume, reporter, first page
  // Example matches:
  //   Foo v. Bar, 123 N.Y.3d 456
  //   123 N.Y. 456
  const VOL = '(?<vol>\\d{1,4})';
  const REP = `(?<rep>${repIndex.reporterAlt})`;
  const PAGE = '(?<page>\\d{1,5})';
  const CASE_NAME = '(?<casename>[A-Z][^,\n]{3,}? v\\\.? [^,\n]{2,}?)';

  const pattern = new RegExp(
    `(?:${CASE_NAME}\\s*[,;:]\\s*)?${VOL}\\s+${REP}\\s+${PAGE}`,
    'g'
  );

  for (const m of t.matchAll(pattern)) {
    const raw = m[0];
    // denylist check on raw
    if (repIndex.denylist.some((rx) => rx.test(raw))) continue;

    const vol = parseInt(m.groups?.vol || '0', 10);
    const repRaw = (m.groups?.rep || '').trim();
    const page = parseInt(m.groups?.page || '0', 10);
    const caseName = (m.groups?.casename || '').trim() || null;

    // Normalize reporter and validate against allowlist
    const repNormKey = repIndex.normToken(repRaw);
    const repNorm = repIndex.aliasToNorm.get(repNormKey);
    if (!repNorm) continue; // not an allowed reporter token

    // Basic guards
    if (!vol || !page) continue;

    results.push({
      raw_citation_text: raw,
      case_name: caseName,
      cited_volume: vol,
      cited_reporter: repRaw,
      cited_reporter_norm: repNorm,
      cited_first_page: page,
    });
  }

  return results;
}

function dedupeCitations(items) {
  const map = new Map();
  for (const it of items) {
    const key = `${it.cited_reporter_norm}|${it.cited_volume}|${it.cited_first_page}`;
    if (!map.has(key)) map.set(key, it);
  }
  return Array.from(map.values());
}

// ---------- DB helpers ----------
function makeDbUrl(dbCfg) {
  // Construct postgres URL
  const host = dbCfg.host || 'localhost';
  const port = dbCfg.port || 5432;
  const db = dbCfg.database || 'postgres';
  const user = dbCfg.user || 'postgres';
  const pass = encodeURIComponent(dbCfg.password || '');
  return pass
    ? `postgresql://${user}:${pass}@${host}:${port}/${db}`
    : `postgresql://${user}@${host}:${port}/${db}`;
}

async function insertExtractedCitations(appPool, citingSourceRef, citingCaseId, citingOriginalId, items, { verbose = false } = {}) {
  if (!items.length) return 0;
  let inserted = 0;
  const sql = `
    INSERT INTO extracted_citations (
      citing_source_ref, citing_case_id, citing_original_id,
      raw_citation_text, case_name,
      cited_volume, cited_reporter, cited_reporter_norm, cited_first_page
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (citing_source_ref, citing_case_id, cited_reporter_norm, cited_volume, cited_first_page)
    DO NOTHING
  `;
  const client = await appPool.connect();
  try {
    await client.query('BEGIN');
    for (const it of items) {
      await client.query(sql, [
        citingSourceRef,
        citingCaseId,
        citingOriginalId,
        it.raw_citation_text,
        it.case_name,
        it.cited_volume,
        it.cited_reporter,
        it.cited_reporter_norm,
        it.cited_first_page,
      ]);
      inserted++;
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
  if (verbose) console.log(`    Inserted ${inserted} extracted citations`);
  return inserted;
}

// ---------- Main processing for a source ----------
async function extractCitationsForSource({ configPath, sourceId, sourceRef = null, limit = null, offset = 0, verbose = false, dryRun = false } = {}) {
  if (!configPath) throw new Error('configPath is required');
  const cfg = await loadConfig(configPath);
  const src = (cfg.sources || []).find((s) => s.id === sourceId);
  if (!src) throw new Error(`Source id not found in config: ${sourceId}`);

  const reportersCfg = await loadReporterConfig();
  const repIndex = buildReporterIndex(reportersCfg);

  // Pools: source DB and app DB
  const srcDbUrl = makeDbUrl(cfg.database);
  const srcPool = new Pool({ connectionString: srcDbUrl });
  const appDbUrl = process.env.APP_DATABASE_URL;
  const appPool = (!dryRun && appDbUrl) ? new Pool({ connectionString: appDbUrl }) : null;
  if (!dryRun && !appDbUrl) throw new Error('APP_DATABASE_URL is required to insert into extracted_citations');

  const citingSourceRef = sourceRef || sourceId;
  const pageSize = 500; // cases per page
  let processedCases = 0;
  let totalExtracted = 0;

  try {
    // Determine total cases
    const { rows: cntRows } = await srcPool.query('SELECT COUNT(*)::int AS n FROM cases');
    const totalCases = cntRows[0]?.n || 0;
    const start = offset || 0;
    const endExclusive = limit ? Math.min(totalCases, start + Number(limit)) : totalCases;

    if (verbose) {
      console.log(`Source DB: ${srcDbUrl}`);
      if (!dryRun) console.log(`App DB: ${new URL(appDbUrl).host}`);
      console.log(`Processing cases ${start}..${endExclusive - 1} (pageSize=${pageSize})`);
      console.log(`Citing source_ref='${citingSourceRef}'`);
    }

    for (let cursor = start; cursor < endExclusive; cursor += pageSize) {
      const take = Math.min(pageSize, endExclusive - cursor);
      const { rows: caseRows } = await srcPool.query(
        'SELECT id, name, original_id FROM cases ORDER BY id OFFSET $1 LIMIT $2',
        [cursor, take]
      );
      if (!caseRows.length) break;
      const caseIds = caseRows.map((r) => r.id);

      const { rows: opRows } = await srcPool.query(
        'SELECT case_id, text FROM opinions WHERE case_id = ANY($1::bigint[])',
        [caseIds]
      );
      const byCaseId = new Map();
      for (const r of opRows) {
        const arr = byCaseId.get(r.case_id) || [];
        arr.push(r.text || '');
        byCaseId.set(r.case_id, arr);
      }

      for (const c of caseRows) {
        const texts = byCaseId.get(c.id) || [];
        if (!texts.length) { processedCases++; continue; }
        const joined = texts.join(' \n ');
        const extracted = extractCitationsFromText(joined, repIndex);
        const deduped = dedupeCitations(extracted);

        if (deduped.length && !dryRun) {
          await insertExtractedCitations(appPool, citingSourceRef, c.id, c.original_id, deduped, { verbose });
        }
        if (verbose && processedCases % 200 === 0) {
          console.log(`  Processed ${processedCases} cases... (extracted so far: ${totalExtracted})`);
        }
        totalExtracted += deduped.length;
        processedCases++;
      }
    }
  } finally {
    await srcPool.end();
    if (appPool) await appPool.end();
  }

  if (verbose) console.log(`Done. Processed ${processedCases} cases; extracted ${totalExtracted} citations.`);
  return { processedCases, totalExtracted };
}

// Via app DB source reference (no local config)
async function extractCitationsFromSourceRef({ sourceRef, limit = null, offset = 0, verbose = false, dryRun = false } = {}) {
  if (!sourceRef) throw new Error('sourceRef is required');
  const appDbUrl = process.env.APP_DATABASE_URL;
  if (!appDbUrl) throw new Error('APP_DATABASE_URL is required to look up source DB connection by reference');

  const appPool = new Pool({ connectionString: appDbUrl });
  let srcPool = null;
  let processedCases = 0;
  let totalExtracted = 0;
  try {
    // Lookup source
    const { rows } = await appPool.query('SELECT reference, database_url, enabled FROM sources WHERE reference = $1', [sourceRef]);
    const src = rows[0];
    if (!src) throw new Error(`Source not found in app DB: ${sourceRef}`);
    if (src.enabled === false) throw new Error(`Source is disabled: ${sourceRef}`);

    const srcDbUrl = src.database_url;
    if (!srcDbUrl) throw new Error(`Source ${sourceRef} missing database_url`);
    srcPool = new Pool({ connectionString: srcDbUrl });

    const reportersCfg = await loadReporterConfig();
    const repIndex = buildReporterIndex(reportersCfg);

    const pageSize = 500;
    // Determine total cases
    const { rows: cntRows } = await srcPool.query('SELECT COUNT(*)::int AS n FROM cases');
    const totalCases = cntRows[0]?.n || 0;
    const start = offset || 0;
    const endExclusive = limit ? Math.min(totalCases, start + Number(limit)) : totalCases;

    if (verbose) {
      console.log(`Source DB: ${srcDbUrl}`);
      console.log(`App DB: ${new URL(appDbUrl).host}`);
      console.log(`Processing cases ${start}..${endExclusive - 1} (pageSize=${pageSize})`);
      console.log(`Citing source_ref='${sourceRef}'`);
    }

    for (let cursor = start; cursor < endExclusive; cursor += pageSize) {
      const take = Math.min(pageSize, endExclusive - cursor);
      const { rows: caseRows } = await srcPool.query(
        'SELECT id, name, original_id FROM cases ORDER BY id OFFSET $1 LIMIT $2',
        [cursor, take]
      );
      if (!caseRows.length) break;
      const caseIds = caseRows.map((r) => r.id);

      const { rows: opRows } = await srcPool.query(
        'SELECT case_id, text FROM opinions WHERE case_id = ANY($1::bigint[])',
        [caseIds]
      );
      const byCaseId = new Map();
      for (const r of opRows) {
        const arr = byCaseId.get(r.case_id) || [];
        arr.push(r.text || '');
        byCaseId.set(r.case_id, arr);
      }

      for (const c of caseRows) {
        const texts = byCaseId.get(c.id) || [];
        if (!texts.length) { processedCases++; continue; }
        const joined = texts.join(' \n ');
        const extracted = extractCitationsFromText(joined, repIndex);
        const deduped = dedupeCitations(extracted);

        if (deduped.length && !dryRun) {
          await insertExtractedCitations(appPool, sourceRef, c.id, c.original_id, deduped, { verbose });
        }
        if (verbose && processedCases % 200 === 0) {
          console.log(`  Processed ${processedCases} cases... (extracted so far: ${totalExtracted})`);
        }
        totalExtracted += deduped.length;
        processedCases++;
      }
    }
  } finally {
    if (srcPool) await srcPool.end();
    await appPool.end();
  }

  if (verbose) console.log(`Done. Processed ${processedCases} cases; extracted ${totalExtracted} citations.`);
  return { processedCases, totalExtracted };
}

// ---------- CLI ----------
function parseCli() {
  const args = process.argv.slice(2);
  const configPath = args.find((a) => a.startsWith('--config='))?.split('=')[1];
  const sourceId = args.find((a) => a.startsWith('--source='))?.split('=')[1];
  const sourceRef = args.find((a) => a.startsWith('--source-ref='))?.split('=')[1] || null;
  const verbose = args.includes('--verbose') || args.includes('-v');
  const dryRun = args.includes('--dry-run');
  const limit = args.find((a) => a.startsWith('--limit='))?.split('=')[1] || null;
  const offset = parseInt(args.find((a) => a.startsWith('--offset='))?.split('=')[1] || '0', 10);
  const help = args.includes('--help') || args.includes('-h');
  return { configPath, sourceId, sourceRef, verbose, dryRun, limit, offset, help };
}

async function main() {
  try {
    const { configPath, sourceId, sourceRef, verbose, dryRun, limit, offset, help } = parseCli();
    if (help) {
      console.log(`Citation Extractor\n\nUsage:\n  node src/citation_extractor.js --config=./configs/ny_coa.json --source=ny3d [--source-ref=ny_reporter] [--limit=1000] [--offset=0] [--verbose] [--dry-run]\n  node src/citation_extractor.js --source=nycoa [--limit=1000] [--offset=0] [--verbose] [--dry-run]\n`);
      process.exit(0);
    }

    let res;
    if (configPath && sourceId) {
      res = await extractCitationsForSource({ configPath, sourceId, sourceRef, limit, offset, verbose, dryRun });
    } else if (sourceId && !configPath) {
      // Interpret --source as app DB source reference
      res = await extractCitationsFromSourceRef({ sourceRef: sourceId, limit, offset, verbose, dryRun });
    } else {
      console.log(`Missing required options. Provide either:\n  --config=... --source=<id>   (config file mode)\n  --source=<reference>          (app DB mode)`);
      process.exit(1);
    }
    console.log(`Extracted ${res.totalExtracted} citations from ${res.processedCases} cases.`);
  } catch (e) {
    console.error('❌ Citation extraction failed:', e.message);
    if (process.env.NODE_ENV === 'development') console.error(e.stack);
    process.exit(1);
  }
}

export { loadReporterConfig, buildReporterIndex, normalizeOpinionText, extractCitationsFromText, dedupeCitations, extractCitationsForSource, extractCitationsFromSourceRef };

if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
