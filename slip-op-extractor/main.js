#!/usr/bin/env node
/**
 * Slip-Op Extractor — parse NY slip-opinion HTML or PDF documents into
 * the bulk-ingest JSON contract (schema_version 0.3, no volume{} block,
 * rich citations[] array).
 *
 * Per-extractor convention:
 *   in/html/<file>.html          — slip-opinion HTML pages
 *   in/pdf/<file>.pdf            — slip-opinion PDFs
 *   in/index/<MM_YY_court>.html  — LRB monthly index pages, used by
 *                                  scripts/resolve_index.js (NOT this
 *                                  command). Kept in a sibling dir so
 *                                  parse-all can't accidentally treat
 *                                  an index page as an opinion.
 *   out/<stem>/cases.json        — one parsed case per input file
 *   out/<stem>/source.meta.json  — sha256 + parsed-at + parser_version
 *   compiled/<source>-<window>.json   — merged batch payloads ready to
 *                                       upload (one POST applies many cases)
 *   compiled/<source>-<window>.manifest.json — provenance: which
 *                                       per-stem files contributed which
 *                                       case_curies
 *
 * Commands:
 *   node main.js parse <file>            # parse one file → out/<stem>/cases.json
 *   node main.js parse-all               # parse every .html in in/html/ and .pdf in in/pdf/, skip-fresh
 *   node main.js parse-all --force       # re-parse regardless of freshness
 *   node main.js parse-all --in=<dir>    # custom input dir (overrides the default in/html/ + in/pdf/ pair)
 *   node main.js status                  # list each input + parse state
 *   node main.js compile [--source=<ref>] [--window=<label>]
 *                                        # merge per-stem cases.json into
 *                                        # compiled/<source>-<window>.json
 *
 * Window labels default to today's date (YYYY-MM-DD) so re-running compile
 * on the same day overwrites the same artifact. Pass --window for explicit
 * batch labels (e.g. --window=2026-W18).
 *
 * Compile groups outputs by `target_source_db` so each compiled artifact
 * targets a single source DB. Within a source, cases are ordered by
 * decision_date asc for deterministic, diff-friendly output.
 *
 * The output JSON is what you'd POST to co-collection's
 * `/admin/api/bulk-ingest/upload` endpoint. The `target_source_db` field
 * indicates which source DB the case is destined for (ny_supreme /
 * ny_appellate / ny_trial), derived from the document's court line.
 */

import { readFile, readdir, mkdir, writeFile, stat } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createHash, randomUUID } from 'crypto';

import { detectFormat } from './src/detect.js';
import { parseHtml } from './src/parser_html.js';
import { parsePdf } from './src/parser_pdf.js';
import { buildPayload } from './src/output.js';
import { sha256OfBuffer, PARSER_VERSION } from './src/shared.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = __dirname;
const IN_DIR       = path.join(ROOT, 'in');
const HTML_DIR     = path.join(IN_DIR, 'html');
const PDF_DIR      = path.join(IN_DIR, 'pdf');
const OUT_DIR      = path.join(ROOT, 'out');
const COMPILED_DIR = path.join(ROOT, 'compiled');

const INPUT_EXTENSIONS_RE = /\.(html?|pdf)$/i;

/**
 * Collect opinion-input files. Default sweeps in/html/ + in/pdf/ — the
 * format-segregated layout keeps index pages (in/index/) safely out of
 * scope; an index would parse to garbage as a slip-op. An explicit
 * --in=<dir> override walks just that one directory.
 */
async function discoverOpinionFiles(args) {
  if (args.in) {
    const dir = path.resolve(args.in);
    return await listOpinionsIn(dir, dir);
  }
  const dirs = [HTML_DIR, PDF_DIR];
  const out = [];
  for (const d of dirs) out.push(...await listOpinionsIn(d, d));
  return out.sort();
}

async function listOpinionsIn(dir, _label) {
  if (!existsSync(dir)) return [];
  const entries = await readdir(dir);
  return entries
    .filter(f => INPUT_EXTENSIONS_RE.test(f))
    .map(f => path.join(dir, f));
}

function usage() {
  console.log(`
Usage:
  node main.js parse <file>
  node main.js parse-all [--in=<dir>] [--force]
  node main.js status     [--in=<dir>]
  node main.js compile    [--source=<ref>] [--window=<label>]

Convention:
  in/html/<file>.html          slip-opinion HTML pages
  in/pdf/<file>.pdf            slip-opinion PDFs
  in/index/<MM_YY_court>.html  LRB monthly index pages (resolve_index.js, not this command)
  out/<stem>/cases.json        per-file parsed payload
  compiled/<source>-<win>.json merged batch ready to upload
`.trim());
}

// --- core parse ---

async function parseOne(filePath) {
  const buf = await readFile(filePath);
  const sourceSha256 = sha256OfBuffer(buf);
  const format = detectFormat(filePath, buf);

  let caseObj = null;
  if (format === 'html-modern' || format === 'html-legacy') {
    caseObj = parseHtml(buf.toString('utf8'), format, {});
  } else if (format === 'pdf') {
    caseObj = parsePdf(filePath);
  } else {
    throw new Error(`unrecognised input format for ${path.basename(filePath)}`);
  }

  if (!caseObj) {
    return { format, payload: buildPayload({ caseObj: null, sourceSha256 }), error: 'parse-returned-null' };
  }

  const payload = buildPayload({ caseObj, sourceSha256 });
  return { format, payload, sourceSha256 };
}

async function writeStemOutput(stem, format, payload, sourceSha256) {
  const stemDir = path.join(OUT_DIR, stem);
  await mkdir(stemDir, { recursive: true });
  const casesPath = path.join(stemDir, 'cases.json');
  const metaPath  = path.join(stemDir, 'source.meta.json');
  await writeFile(casesPath, JSON.stringify(payload, null, 2));
  await writeFile(metaPath, JSON.stringify({
    stem,
    format,
    parser_version: PARSER_VERSION,
    source_sha256: sourceSha256,
    parsed_at: new Date().toISOString(),
    target_source_db: payload.target_source_db || null,
    case_count: payload.cases.length,
    case_curies: payload.cases.map(c => c.case_curie).filter(Boolean),
  }, null, 2));
  return { casesPath, metaPath };
}

async function isFreshForStem(stem, sourceSha256) {
  // Skip parse when out/<stem>/source.meta.json exists with matching
  // parser_version + source_sha256.
  const metaPath = path.join(OUT_DIR, stem, 'source.meta.json');
  if (!existsSync(metaPath)) return false;
  try {
    const m = JSON.parse(await readFile(metaPath, 'utf8'));
    return m.parser_version === PARSER_VERSION && m.source_sha256 === sourceSha256;
  } catch {
    return false;
  }
}

function basenameNoExt(f) {
  return path.basename(f).replace(INPUT_EXTENSIONS_RE, '');
}

// --- commands ---

async function cmdParse(args) {
  const filePath = args._[1];
  if (!filePath) { usage(); process.exit(2); }
  const stem = basenameNoExt(filePath);
  const { format, payload, error, sourceSha256 } = await parseOne(path.resolve(filePath));
  if (error) {
    console.error(`ERROR (${format}): ${error}`);
    process.exit(1);
  }
  const { casesPath } = await writeStemOutput(stem, format, payload, sourceSha256);
  console.error(`OK    ${path.basename(filePath)} → ${path.relative(ROOT, casesPath)} (format=${format}, target=${payload.target_source_db})`);
}

async function cmdParseAll(args) {
  const force = !!args.force;
  await mkdir(HTML_DIR, { recursive: true });
  await mkdir(PDF_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const files = await discoverOpinionFiles(args);

  if (files.length === 0) {
    const where = args.in
      ? path.relative(ROOT, path.resolve(args.in)) || args.in
      : `${path.relative(ROOT, HTML_DIR)}/ + ${path.relative(ROOT, PDF_DIR)}/`;
    console.error(`No .html / .pdf files under ${where}`);
    return;
  }

  let ok = 0, skipped = 0, failed = 0;
  for (const f of files) {
    const stem = basenameNoExt(f);
    try {
      const buf = await readFile(f);
      const sourceSha256 = sha256OfBuffer(buf);
      if (!force && await isFreshForStem(stem, sourceSha256)) {
        console.error(`SKIP  ${path.basename(f)} (fresh)`);
        skipped++;
        continue;
      }
      const { format, payload, error } = await parseOne(f);
      if (error) {
        console.error(`FAIL  ${path.basename(f)} (${format}) — ${error}`);
        failed++;
        continue;
      }
      const { casesPath } = await writeStemOutput(stem, format, payload, sourceSha256);
      console.error(`OK    ${path.basename(f)} → ${path.relative(ROOT, casesPath)} (format=${format}, target=${payload.target_source_db})`);
      ok++;
    } catch (e) {
      console.error(`FAIL  ${path.basename(f)} — ${e.message}`);
      failed++;
    }
  }
  console.error(`\nparse-all done: ok=${ok} skipped=${skipped} failed=${failed} (parser ${PARSER_VERSION})`);
  if (failed > 0) process.exit(1);
}

async function cmdStatus(args) {
  const files = await discoverOpinionFiles(args);
  if (files.length === 0) {
    const where = args.in
      ? path.relative(ROOT, path.resolve(args.in)) || args.in
      : `${path.relative(ROOT, HTML_DIR)}/ + ${path.relative(ROOT, PDF_DIR)}/`;
    console.log(`(no .html / .pdf files under ${where})`);
    return;
  }
  console.log('file                                                state    parser    target              parsed_at');
  console.log('───────────────────────────────────────────────────  ───────  ────────  ──────────────────  ────────────────────');
  for (const f of files) {
    const stem = basenameNoExt(f);
    const display = path.relative(IN_DIR, f);
    const metaPath = path.join(OUT_DIR, stem, 'source.meta.json');
    if (!existsSync(metaPath)) {
      console.log(`${pad(display, 51)}  ${pad('unparsed', 7)}  -         -                   -`);
      continue;
    }
    try {
      const m = JSON.parse(await readFile(metaPath, 'utf8'));
      const buf = await readFile(f);
      const sha = sha256OfBuffer(buf);
      const fresh = m.parser_version === PARSER_VERSION && m.source_sha256 === sha;
      console.log(`${pad(display, 51)}  ${pad(fresh ? 'fresh' : 'stale', 7)}  ${pad(m.parser_version, 8)}  ${pad(m.target_source_db || '-', 18)}  ${m.parsed_at || '-'}`);
    } catch (e) {
      console.log(`${pad(display, 51)}  ${pad('error', 7)}  -         -                   ${e.message}`);
    }
  }
}

function pad(s, w) {
  s = String(s ?? '');
  if (s.length >= w) return s;
  return s + ' '.repeat(w - s.length);
}

// --- compile ---

async function cmdCompile(args) {
  const filterSource = args.source || null;
  const window = args.window || todayLabel();

  if (!existsSync(OUT_DIR)) {
    console.error(`No out/ dir found. Run parse-all first.`);
    process.exit(1);
  }
  await mkdir(COMPILED_DIR, { recursive: true });

  const stems = (await readdir(OUT_DIR, { withFileTypes: true }))
    .filter(d => d.isDirectory())
    .map(d => d.name);

  // Bucket per source. Each stem contributes 0+ cases (well, exactly 1 in
  // the slip-op extractor's per-doc model, but we keep this generic so
  // future per-doc-multi-case formats Just Work).
  const bySource = new Map();   // source_ref -> { cases: [...], stems: Map<stem, [curies]> }

  for (const stem of stems) {
    const casesPath = path.join(OUT_DIR, stem, 'cases.json');
    if (!existsSync(casesPath)) continue;
    let payload;
    try {
      payload = JSON.parse(await readFile(casesPath, 'utf8'));
    } catch (e) {
      console.error(`SKIP  ${stem} — failed to parse cases.json: ${e.message}`);
      continue;
    }
    const sourceRef = payload.target_source_db;
    if (!sourceRef) {
      console.error(`SKIP  ${stem} — no target_source_db`);
      continue;
    }
    if (filterSource && sourceRef !== filterSource) continue;
    if (!Array.isArray(payload.cases) || payload.cases.length === 0) continue;

    if (!bySource.has(sourceRef)) {
      bySource.set(sourceRef, { cases: [], stems: new Map() });
    }
    const bucket = bySource.get(sourceRef);
    for (const c of payload.cases) {
      bucket.cases.push(c);
    }
    bucket.stems.set(stem, payload.cases.map(c => c.case_curie).filter(Boolean));
  }

  if (bySource.size === 0) {
    console.error('Nothing to compile (no per-stem cases.json files matched).');
    return;
  }

  for (const [sourceRef, bucket] of bySource) {
    // Deterministic ordering for diff-friendliness.
    bucket.cases.sort((a, b) => {
      const da = a.decision_date || '';
      const db = b.decision_date || '';
      if (da !== db) return da < db ? -1 : 1;
      return (a.case_curie || '').localeCompare(b.case_curie || '');
    });

    // Synthetic top-level metadata. source_pdf is a comma-list of stem
    // names; source_pdf_sha256 is a sha256 of the sorted curie list so
    // re-running compile against the same set of inputs produces the same
    // hash (and hits the bulk-ingest dedup if re-uploaded).
    const stemNames = [...bucket.stems.keys()].sort();
    const allCuries = [...new Set(
      [...bucket.stems.values()].flat()
    )].sort();
    const compiledHash = createHash('sha256')
      .update(allCuries.join('\n'))
      .digest('hex');

    const compiled = {
      schema_version: '0.3',
      batch_id: `slip-${sourceRef}-${window}-${randomUUID().slice(0, 8)}`,
      parser_version: PARSER_VERSION,
      source_pdf: `slip-batch:${sourceRef}:${window}:${stemNames.length}-files`,
      source_pdf_sha256: compiledHash,
      target_source_db: sourceRef,
      // volume omitted — slip-op compile carries no reporter metadata.
      cases: bucket.cases,
    };

    const compiledPath = path.join(COMPILED_DIR, `${sourceRef}-${window}.json`);
    await writeFile(compiledPath, JSON.stringify(compiled, null, 2));

    const manifest = {
      compiled_path: path.relative(ROOT, compiledPath),
      source_ref: sourceRef,
      window,
      generated_at: new Date().toISOString(),
      parser_version: PARSER_VERSION,
      compiled_sha256: compiledHash,
      case_count: bucket.cases.length,
      stem_count: stemNames.length,
      // Per-stem provenance: which input file contributed which case_curies.
      // Drives revert/audit ("which slip-op file produced this case?").
      contributions: stemNames.map(stem => ({
        stem,
        case_curies: bucket.stems.get(stem),
      })),
    };
    const manifestPath = path.join(COMPILED_DIR, `${sourceRef}-${window}.manifest.json`);
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2));

    console.error(`OK    compiled ${sourceRef} → ${path.relative(ROOT, compiledPath)} (${bucket.cases.length} cases from ${stemNames.length} stems)`);
    console.error(`      manifest → ${path.relative(ROOT, manifestPath)}`);
  }
}

function todayLabel() {
  const d = new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

// --- arg parsing ---

function parseArgs(argv) {
  const out = { _: [] };
  for (const a of argv.slice(2)) {
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      if (eq === -1) out[a.slice(2)] = true;
      else out[a.slice(2, eq)] = a.slice(eq + 1);
    } else {
      out._.push(a);
    }
  }
  return out;
}

const args = parseArgs(process.argv);
const cmd = args._[0];

const HANDLERS = {
  parse:        cmdParse,
  'parse-all':  cmdParseAll,
  status:       cmdStatus,
  compile:      cmdCompile,
};

if (HANDLERS[cmd]) {
  HANDLERS[cmd](args).catch(e => { console.error(e.stack || e.message); process.exit(1); });
} else {
  usage();
  process.exit(cmd === 'help' ? 0 : 2);
}
