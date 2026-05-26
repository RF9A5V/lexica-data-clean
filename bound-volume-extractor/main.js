#!/usr/bin/env node
/**
 * Bound-Volume Extractor — parse NY official-reports bound-volume PDFs into
 * structured JSON / SQL for import into Curia Obscura source DBs.
 *
 * Usage:
 *   node main.js parse-all                    # parse every PDF in ./in/, skipping fresh ones
 *   node main.js parse-all --force            # re-parse everything regardless of freshness
 *   node main.js parse-all --dry-run          # report what would be parsed/skipped, no work
 *   node main.js parse-all --concurrency=4    # parse N PDFs in parallel (default 4)
 *   node main.js parse <pdf>                  # parse a single PDF (full pipeline)
 *   node main.js extract <pdf>                # just the Python extraction step (writes raw NDJSON)
 *   node main.js status                       # list each PDF in in/ and its parse state
 *   node main.js audit list                   # list all parsed batches
 *   node main.js audit show <id>              # show one batch's audit record
 *
 * Place input PDFs under ./in/. Per-PDF outputs land under
 * ./out/<pdf-stem>/ with canonical filenames:
 *   cases.json   — structured parse output (this is what the admin UI ingests)
 *   cases.sql    — equivalent SQL script (kept for parity / out-of-band psql apply)
 *   raw.ndjson   — page-by-page Python extraction (debugging artifact)
 *   audit.json   — per-batch audit record copied here for self-containment
 *
 * Audits also accumulate at ./audit/ so re-parses don't lose history.
 *
 * Freshness: parse-all skips a PDF when its existing cases.json has both
 *   parser_version === current AND source_pdf_sha256 === current PDF's sha.
 * Either mismatch (parser bump or PDF replaced) triggers a re-parse.
 *
 * Prereqs (one-time):
 *   python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
 */

import { randomUUID } from 'crypto';
import { mkdir, writeFile, readFile, readdir, copyFile, stat } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

import { sha256OfFile } from './src/hash.js';
import { extractPdfPages } from './src/pdf_runner.js';
import { detectVolume } from './src/volume_detector.js';
import { parseCases } from './src/parser.js';
import { writeJson, writeSql } from './src/output.js';
import { writeAudit, listAudits, readAudit } from './src/audit.js';
import { validateAllOutputs, printValidationReport } from './src/validate.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = __dirname;
const PDFS_DIR   = path.join(ROOT, 'in');
const OUTPUT_DIR = path.join(ROOT, 'out');
const AUDIT_DIR  = path.join(ROOT, 'audit');

const PARSER_VERSION = '0.6.61';

async function ensureDirs() {
  await mkdir(PDFS_DIR,   { recursive: true });
  await mkdir(OUTPUT_DIR, { recursive: true });
  await mkdir(AUDIT_DIR,  { recursive: true });
}

function sliceForLog(s, n = 80) {
  const t = String(s).replace(/\s+/g, ' ').trim();
  return t.length > n ? t.slice(0, n) + '…' : t;
}

function pdfStem(pdfPath) {
  return path.basename(pdfPath, path.extname(pdfPath));
}

function outDirFor(pdfPath) {
  return path.join(OUTPUT_DIR, pdfStem(pdfPath));
}

/**
 * Run an async `fn` over `items` with at most `limit` jobs in flight at once.
 * Each job calls `fn(item, index)` and its return value is captured at the
 * matching index of the result array. Failures are caught into `{ error }`
 * objects so one bad job doesn't sink the whole pool — the caller decides
 * how to surface them.
 */
async function runWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let cursor = 0;
  const n = Math.max(1, Math.min(limit, items.length));
  const workers = Array.from({ length: n }, async () => {
    while (true) {
      const i = cursor++;
      if (i >= items.length) break;
      try {
        results[i] = { ok: true, value: await fn(items[i], i) };
      } catch (err) {
        results[i] = { ok: false, error: err };
      }
    }
  });
  await Promise.all(workers);
  return results;
}

/**
 * Decide whether a PDF needs (re)parsing.
 *
 * Returns one of:
 *   { state: 'new' }                              — never parsed
 *   { state: 'fresh', existing }                  — parser_version + sha match
 *   { state: 'stale', existing, reason }          — parsed before but stale
 *   { state: 'unparsable', reason }               — couldn't read cases.json
 */
async function assessPdf(pdfPath, currentSha) {
  const out = outDirFor(pdfPath);
  const casesPath = path.join(out, 'cases.json');
  let raw;
  try {
    raw = await readFile(casesPath, 'utf8');
  } catch {
    return { state: 'new' };
  }
  let doc;
  try {
    doc = JSON.parse(raw);
  } catch (err) {
    return { state: 'unparsable', reason: `cases.json present but unreadable: ${err.message}` };
  }
  const existing = {
    parser_version: doc.parser_version || null,
    source_pdf_sha256: doc.source_pdf_sha256 || null,
    parsed_at: doc.parsed_at || null,
    case_count: doc.cases?.length ?? null,
    batch_id: doc.batch_id || null,
  };
  if (existing.parser_version !== PARSER_VERSION) {
    return { state: 'stale', existing, reason: `parser_version ${existing.parser_version} → ${PARSER_VERSION}` };
  }
  if (existing.source_pdf_sha256 !== currentSha) {
    return { state: 'stale', existing, reason: `PDF changed (sha mismatch)` };
  }
  return { state: 'fresh', existing };
}

async function listPdfs() {
  await ensureDirs();
  const entries = await readdir(PDFS_DIR);
  return entries.filter(f => f.toLowerCase().endsWith('.pdf')).sort().map(f => path.join(PDFS_DIR, f));
}

async function cmdExtract(pdfPath) {
  await ensureDirs();
  const out = outDirFor(pdfPath);
  await mkdir(out, { recursive: true });
  const rawPath = path.join(out, 'raw.ndjson');
  console.log(`[extract] ${pdfPath} → ${rawPath}`);
  const t0 = Date.now();
  let pageCount = 0, errCount = 0, meta = null;
  for await (const rec of extractPdfPages(pdfPath, { rawOutputPath: rawPath })) {
    if (rec.kind === 'meta') {
      meta = rec;
      console.log(`  pages: ${rec.page_count}, metadata: ${JSON.stringify(rec.metadata)}`);
    } else if (rec.kind === 'page') {
      pageCount++;
      if (pageCount % 50 === 0) console.log(`  ...${pageCount} pages`);
    } else if (rec.kind === 'page_error') {
      errCount++;
      console.warn(`  page ${rec.page_index} ERROR: ${rec.error}`);
    }
  }
  const dt = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`  done. pages=${pageCount} errors=${errCount} time=${dt}s`);
  console.log(`  raw extraction saved: ${rawPath}`);
  return { pageCount, errCount, meta, rawPath };
}

/**
 * Parse a single PDF end-to-end. Writes canonical artifacts under
 * out/<stem>/ and copies the audit record into the same directory for
 * self-containment.
 */
async function cmdParse(pdfPath, opts = {}) {
  // `logger.log` / `logger.warn` defaults to console; concurrent parse-all
  // jobs swap in a buffering logger so per-PDF output stays a single block.
  const logger = opts.logger || console;
  await ensureDirs();
  const out = outDirFor(pdfPath);
  await mkdir(out, { recursive: true });

  const t0 = Date.now();
  const batch_id = randomUUID();
  const stem = pdfStem(pdfPath);
  const sha = await sha256OfFile(pdfPath);

  logger.log(`[parse${opts.cached ? ':cached' : ''}] ${pdfPath}`);
  logger.log(`  batch_id: ${batch_id}`);
  logger.log(`  sha256:   ${sha}`);
  logger.log(`  out:      ${out}/`);

  const rawPath = path.join(out, 'raw.ndjson');
  const pages = [];
  let meta = null;
  let pageErrors = 0;

  if (opts.cached) {
    // Reuse a previously-extracted NDJSON to skip the Python pdfplumber pass.
    // Look in the new per-PDF dir first; fall back to legacy flat layout for
    // older runs.
    let cachedPath = path.join(out, 'raw.ndjson');
    try {
      await readFile(cachedPath, 'utf8');
    } catch {
      const legacyPrimary = path.join(OUTPUT_DIR, `${stem}.raw.ndjson`);
      try {
        await readFile(legacyPrimary, 'utf8');
        cachedPath = legacyPrimary;
      } catch {
        const candidates = (await readdir(OUTPUT_DIR))
          .filter(f => f.startsWith(`${stem}.`) && f.endsWith('.raw.ndjson'))
          .sort();
        if (!candidates.length) throw new Error(`no cached NDJSON found for ${stem}`);
        cachedPath = path.join(OUTPUT_DIR, candidates[0]);
      }
    }
    logger.log(`  using cached extraction: ${cachedPath}`);
    const text = await readFile(cachedPath, 'utf8');
    for (const line of text.split('\n')) {
      if (!line.trim()) continue;
      const rec = JSON.parse(line);
      if (rec.kind === 'meta')        meta = rec;
      else if (rec.kind === 'page')   pages.push(rec);
      else if (rec.kind === 'page_error') pageErrors++;
    }
  } else {
    for await (const rec of extractPdfPages(pdfPath, { rawOutputPath: rawPath })) {
      if (rec.kind === 'meta')        meta = rec;
      else if (rec.kind === 'page')   pages.push(rec);
      else if (rec.kind === 'page_error') pageErrors++;
    }
  }
  logger.log(`  extracted ${pages.length} page(s), ${pageErrors} page error(s)`);

  // Detect volume from the first ~10 pages (front matter usually carries the title page).
  const frontText = pages.slice(0, 10).map(p => p.text_raw || '').join('\n');
  const volume = detectVolume(frontText);
  if (volume) {
    logger.log(`  detected: ${volume.reporter} vol ${volume.volume} (${volume.court}) → ${volume.source_db}`);
  } else {
    logger.warn(`  WARN: could not detect volume metadata from front matter`);
  }

  const { cases, warnings } = parseCases(pages, volume);
  logger.log(`  parsed ${cases.length} case(s); warnings=${warnings.length}`);
  for (const w of warnings) logger.log(`    warn: ${sliceForLog(w, 200)}`);

  const opinions = cases.reduce((n, c) => n + (c.opinions?.length || 0), 0);
  const stats = {
    page_count: meta?.page_count ?? pages.length,
    pages_extracted: pages.length,
    page_errors: pageErrors,
    case_count: cases.length,
    opinion_count: opinions,
  };

  const result = {
    batch_id,
    parser_version: PARSER_VERSION,
    source_pdf: path.relative(ROOT, pdfPath),
    source_pdf_sha256: sha,
    parsed_at: new Date().toISOString(),
    volume,
    cases,
    stats,
    warnings,
  };

  const jsonOut = path.join(out, 'cases.json');
  const sqlOut  = path.join(out, 'cases.sql');
  await writeJson(jsonOut, result);
  await writeSql(sqlOut, result);

  const auditRec = {
    batch_id,
    parser_version: PARSER_VERSION,
    parsed_at: result.parsed_at,
    duration_ms: Date.now() - t0,
    source_pdf: result.source_pdf,
    source_pdf_sha256: sha,
    target_source_db: volume?.source_db || null,
    volume,
    stats,
    warnings,
    artifacts: {
      raw_extraction: path.relative(ROOT, rawPath),
      json_output:    path.relative(ROOT, jsonOut),
      sql_output:     path.relative(ROOT, sqlOut),
    },
  };
  const auditPath = await writeAudit(AUDIT_DIR, auditRec);
  // Also drop a copy in the per-PDF dir so the directory is self-contained.
  const auditLocalPath = path.join(out, 'audit.json');
  try {
    await copyFile(auditPath, auditLocalPath);
  } catch (err) {
    logger.warn(`  WARN: could not copy audit into ${auditLocalPath}: ${err.message}`);
  }

  logger.log(`  json:  ${jsonOut}`);
  logger.log(`  sql:   ${sqlOut}`);
  logger.log(`  audit: ${auditPath}`);
  logger.log(`  done in ${((Date.now() - t0)/1000).toFixed(1)}s`);

  return { batch_id, sha, stem, jsonOut, stats };
}

/**
 * Parse every PDF under ./in/. Skips those whose existing cases.json was
 * produced by the current parser_version against the current PDF sha.
 *
 * Flags:
 *   --force     re-parse even if fresh
 *   --dry-run   only report what would happen; no parsing
 *   --cached    reuse existing raw.ndjson where available (much faster)
 */
async function cmdParseAll(opts = {}) {
  await ensureDirs();
  const pdfs = await listPdfs();
  if (pdfs.length === 0) {
    console.log(`(no PDFs found under ${PDFS_DIR})`);
    return;
  }
  console.log(`Found ${pdfs.length} PDF(s) under ${path.relative(ROOT, PDFS_DIR)}/\n`);

  // First pass: assess every PDF so the user sees the full plan upfront.
  const plan = [];
  for (const pdf of pdfs) {
    const sha = await sha256OfFile(pdf);
    const assessment = await assessPdf(pdf, sha);
    plan.push({ pdf, sha, assessment });
  }

  const willParse = plan.filter(p => opts.force || p.assessment.state !== 'fresh');
  const willSkip  = plan.filter(p => !opts.force && p.assessment.state === 'fresh');

  console.log('Plan:');
  for (const p of plan) {
    const stem = pdfStem(p.pdf);
    const a = p.assessment;
    let label;
    if (opts.force) {
      label = `REPARSE (--force)`;
    } else if (a.state === 'fresh') {
      label = `skip — fresh (parser=${a.existing.parser_version}, ${a.existing.case_count} cases, parsed ${a.existing.parsed_at?.slice(0, 19)})`;
    } else if (a.state === 'new') {
      label = `PARSE (new)`;
    } else if (a.state === 'stale') {
      label = `REPARSE — ${a.reason}`;
    } else {
      label = `REPARSE — ${a.reason}`;
    }
    console.log(`  ${stem.padEnd(20)} ${label}`);
  }
  console.log(`\n${willParse.length} to parse, ${willSkip.length} to skip.`);

  if (opts.dryRun) {
    console.log('\n(--dry-run, no parsing performed)');
    return;
  }
  if (willParse.length === 0) {
    console.log('\nNothing to do.');
    return;
  }

  console.log('');
  const concurrency = Math.max(1, parseInt(opts.concurrency || '4', 10) || 4);
  if (concurrency > 1 && willParse.length > 1) {
    console.log(`Running ${Math.min(concurrency, willParse.length)} parses in parallel.\n`);
  }

  // Each parallel job buffers its log lines and flushes them on completion
  // so per-PDF output stays a single contiguous block instead of being
  // interleaved with other jobs' output.
  let completed = 0;
  const total = willParse.length;
  const results = await runWithConcurrency(willParse, concurrency, async ({ pdf }) => {
    const buf = [];
    const logger = {
      log:  (...args) => buf.push(args.join(' ')),
      warn: (...args) => buf.push(args.join(' ')),
    };
    try {
      const out = await cmdParse(pdf, { cached: !!opts.cached, logger });
      completed++;
      console.log(`\n[${completed}/${total}] ─────────────────────────`);
      for (const line of buf) console.log(line);
      return { ok: true, pdf, out };
    } catch (err) {
      completed++;
      console.log(`\n[${completed}/${total}] ─────────────────────────`);
      for (const line of buf) console.log(line);
      console.error(`  FAILED: ${err.message}`);
      if (process.env.DEBUG) console.error(err.stack);
      throw err;
    }
  });

  const okCount = results.filter(r => r.ok).length;
  const failures = [];
  for (let i = 0; i < results.length; i++) {
    if (!results[i].ok) {
      failures.push({ pdf: willParse[i].pdf, error: results[i].error.message });
    }
  }

  console.log(`\n═══════════════════════════════════════════`);
  console.log(`Done. ${okCount} succeeded, ${failures.length} failed, ${willSkip.length} skipped.`);
  if (failures.length) {
    console.log(`\nFailures:`);
    for (const f of failures) console.log(`  ${pdfStem(f.pdf)}: ${f.error}`);
    process.exit(1);
  }
}

/**
 * Show, for each PDF in ./in/, whether it's been parsed and whether the
 * parse is fresh against the current parser_version + PDF sha.
 */
async function cmdStatus() {
  await ensureDirs();
  const pdfs = await listPdfs();
  if (pdfs.length === 0) {
    console.log(`(no PDFs found under ${PDFS_DIR})`);
    return;
  }
  console.log(`PDF                    state     parser    cases  parsed_at            reason`);
  console.log(`─────────────────────  ────────  ────────  ─────  ───────────────────  ─────────────────────────────`);
  for (const pdf of pdfs) {
    const sha = await sha256OfFile(pdf);
    const a = await assessPdf(pdf, sha);
    const stem = pdfStem(pdf).padEnd(20);
    const state = a.state.padEnd(8);
    const pv = (a.existing?.parser_version || '—').padEnd(8);
    const cc = String(a.existing?.case_count ?? '—').padStart(5);
    const at = (a.existing?.parsed_at || '—').slice(0, 19).padEnd(19);
    const reason = a.reason || '';
    console.log(`${stem}  ${state}  ${pv}  ${cc}  ${at}  ${reason}`);
  }
  console.log(`\nCurrent parser_version: ${PARSER_VERSION}`);
}

async function cmdAuditList() {
  const records = await listAudits(AUDIT_DIR);
  if (!records.length) { console.log('(no audit records)'); return; }
  console.log(`batch_id                              parsed_at             pdf                          cases  ops  source`);
  for (const r of records) {
    const id = (r.batch_id || '').padEnd(36);
    const at = (r.parsed_at || '').slice(0, 19).padEnd(20);
    const pdf = sliceForLog(r.source_pdf, 28).padEnd(28);
    const cases = String(r.stats?.case_count ?? '?').padStart(5);
    const ops   = String(r.stats?.opinion_count ?? '?').padStart(4);
    const src = r.target_source_db || '?';
    console.log(`${id}  ${at}  ${pdf}  ${cases} ${ops}  ${src}`);
  }
}

async function cmdAuditShow(batchId) {
  const r = await readAudit(AUDIT_DIR, batchId);
  console.log(JSON.stringify(r, null, 2));
}

async function cmdValidate(opts = {}) {
  const result = await validateAllOutputs(OUTPUT_DIR, {
    parserVersion: PARSER_VERSION,
    volume: opts.volume || null,
  });
  if (opts.json) {
    console.log(JSON.stringify(result, null, 2));
  } else {
    printValidationReport(result, { verbose: !!opts.verbose });
  }
  if (opts.strict && result.rollup.volumes_with_hard_issues > 0) {
    process.exit(1);
  }
}

function showHelp() {
  console.log(`
Bound-Volume Extractor

  node main.js parse-all                  Parse every PDF in ./in/, skipping fresh ones
  node main.js parse-all --force          Re-parse everything regardless of freshness
  node main.js parse-all --dry-run        Report what would be parsed/skipped, no work
  node main.js parse-all --cached         Reuse existing raw.ndjson when present (faster)
  node main.js parse-all --concurrency=N  Run N parses in parallel (default 4)
  node main.js parse <pdf>                Parse a single PDF (full pipeline)
  node main.js extract <pdf>              Run extraction only (writes raw NDJSON)
  node main.js status                     Per-PDF parse state under ./in/
  node main.js audit list                 List all parsed batches
  node main.js audit show <id>            Show one batch's audit record
  node main.js validate                   Quality-check every out/<vol>/cases.json
  node main.js validate --volume=<v>      Validate a single volume directory
  node main.js validate --verbose         Include sample refs for soft issues
  node main.js validate --json            Emit a JSON report on stdout
  node main.js validate --strict          Exit 1 if any hard issues exist

Layout:
  in/<stem>.pdf      input
  out/<stem>/
    cases.json         structured parse output (admin UI uploads this)
    cases.sql          equivalent SQL script
    raw.ndjson         page-by-page Python extraction
    audit.json         per-batch audit (also kept under audit/ for cross-batch history)
`);
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  // Flags can be `--name` (boolean) or `--name=value` (string).
  const flags = Object.fromEntries(rest.filter(a => a.startsWith('--')).map(a => {
    const eq = a.indexOf('=');
    return eq >= 0 ? [a.slice(2, eq), a.slice(eq + 1)] : [a.slice(2), true];
  }));
  const positional = rest.filter(a => !a.startsWith('--'));
  try {
    if (cmd === 'parse-all')  await cmdParseAll({
      force: !!flags.force,
      dryRun: !!flags['dry-run'],
      cached: !!flags.cached,
      concurrency: flags.concurrency,
    });
    else if (cmd === 'parse')    await cmdParse(positional[0], { cached: !!flags.cached });
    else if (cmd === 'extract')  await cmdExtract(positional[0]);
    else if (cmd === 'status')   await cmdStatus();
    else if (cmd === 'audit' && positional[0] === 'list') await cmdAuditList();
    else if (cmd === 'audit' && positional[0] === 'show') await cmdAuditShow(positional[1]);
    else if (cmd === 'validate') await cmdValidate({
      volume: typeof flags.volume === 'string' ? flags.volume : null,
      json: !!flags.json,
      verbose: !!flags.verbose,
      strict: !!flags.strict,
    });
    else if (!cmd || cmd === 'help' || cmd === '--help' || cmd === '-h') showHelp();
    else {
      console.error(`unknown command: ${cmd}`);
      showHelp();
      process.exit(2);
    }
  } catch (err) {
    console.error('FATAL:', err.message);
    if (process.env.DEBUG) console.error(err.stack);
    process.exit(1);
  }
}

main();
