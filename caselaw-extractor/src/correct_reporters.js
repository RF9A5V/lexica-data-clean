#!/usr/bin/env node

/**
 * Reporter Correction Script
 *
 * Goal: Correct mismatched official reporters in citations based on source config reporter
 * and filename volume, with full provenance and optional application.
 *
 * Modes:
 *  - Dry-run (default): detect and log corrections, no DB writes
 *  - Apply (--apply): update citations.cite, insert citation_corrections and case_notes
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { createPool } from './database.js';

function usage() {
  console.log(`
Correct Reporter Tokens in Official Citations

USAGE:
  node src/correct_reporters.js -c <config.json> [--apply] [--sample] [-v] [-o <out.log>]

OPTIONS:
  -c, --config <file>  Config JSON with database + sources[].{id, reporter}
  --apply              Apply changes to DB (default: dry-run)
  --sample             Process only the first file per source
  -v, --verbose        Verbose output
  -o, --out <file>     Output log file path (default: logs/corrections-<config>-<timestamp>.log)
`);
}

function normalizeReporter(r) {
  return (r || '')
    .toString()
    .replace(/\s+/g, '')
    .replace(/[.]/g, '')
    .toUpperCase();
}

function parseVolumeReporterPage(cite) {
  if (!cite || typeof cite !== 'string') return null;
  const norm = cite.replace(/\s+/g, ' ').trim();
  // Be tolerant of missing spaces between tokens (e.g., "282 A.D.651" or "282A.D. 651")
  const m = cite.match(/^(\d+)\s*([A-Za-z0-9.\s]+?)\s*(\d+)(.*)$/);
  if (!m) return null;
  return {
    volume: parseInt(m[1], 10),
    reporter: m[2].replace(/\s+/g, ' ').trim(),
    page: parseInt(m[3], 10),
    tail: m[4] || '', // keep any trailing punctuation/parenthetical
    normalized: norm,
  };
}

function rebuildCite(parsed, expectedReporter) {
  // Preserve original spacing around volume/page; use single space between tokens
  return `${parsed.volume} ${expectedReporter} ${parsed.page}${parsed.tail}`.trim();
}

function getCliArgs() {
  const args = process.argv.slice(2);
  const opts = { config: null, apply: false, sample: false, verbose: false, out: null, source: null, vol: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case '-c':
      case '--config':
        opts.config = args[++i];
        break;
      case '--apply':
        opts.apply = true;
        break;
      case '--sample':
        opts.sample = true;
        break;
      case '--source':
        opts.source = args[++i];
        break;
      case '--vol':
      case '--volume':
        opts.vol = parseInt(args[++i], 10);
        break;
      case '-v':
      case '--verbose':
        opts.verbose = true;
        break;
      case '-o':
      case '--out':
        opts.out = args[++i];
        break;
      case '-h':
      case '--help':
        usage();
        process.exit(0);
      default:
        if (!opts.config && !a.startsWith('-')) opts.config = a;
        break;
    }
  }
  if (!opts.config) {
    console.error('❌ --config is required');
    usage();
    process.exit(1);
  }
  return opts;
}

async function ensureDir(dir) {
  try { await fs.mkdir(dir, { recursive: true }); } catch (_) {}
}

async function loadConfig(configPath) {
  const content = await fs.readFile(configPath, 'utf8');
  return JSON.parse(content);
}

async function main() {
  const options = getCliArgs();
  const config = await loadConfig(options.config);

  const cfgBase = path.basename(options.config, '.json');
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const logsDir = path.join(process.cwd(), 'logs');
  await ensureDir(logsDir);
  const outPath = options.out || path.join(logsDir, `corrections-${cfgBase}-${ts}.log`);

  // DB pool using config DB settings
  const pool = createPool(config.database);
  const client = await pool.connect();

  const lines = [];
  const summary = { totalCases: 0, toCorrect: 0, corrected: 0, sources: {} };

  try {
    const sources = Array.isArray(config.sources) ? config.sources : [];

    for (const source of sources) {
      const srcId = source.id;
      if (options.source && options.source !== srcId) continue;
      const expectedReporter = source.reporter || null;
      summary.sources[srcId] = { cases: 0, toCorrect: 0, corrected: 0, missing_official: 0, parse_error: 0, no_citation_in_db: 0 };

      const extractedDir = path.join(process.cwd(), 'data', 'extracted', srcId);
      let files;
      try {
        files = (await fs.readdir(extractedDir)).filter(f => f.endsWith('.json'));
      } catch (e) {
        lines.push(`[source=${srcId}] ERROR: extracted dir not found: ${extractedDir}`);
        continue;
      }

      files.sort((a, b) => parseInt(a, 10) - parseInt(b, 10));
      let toProcess = files;
      if (Number.isFinite(options.vol)) {
        toProcess = files.filter(f => parseInt(path.basename(f, '.json'), 10) === options.vol);
      } else if (options.sample) {
        toProcess = files.slice(0, 1);
      }
      if (options.verbose) console.log(`📁 ${srcId}: processing ${toProcess.length}/${files.length} files${options.sample && !Number.isFinite(options.vol) ? ' (sample)' : ''}`);

      for (const file of toProcess) {
        const fileVol = parseInt(path.basename(file, '.json'), 10);
        const filePath = path.join(extractedDir, file);
        let arr;
        try {
          arr = JSON.parse(await fs.readFile(filePath, 'utf8'));
        } catch (e) {
          lines.push(`[source=${srcId}] file=${file} ERROR: JSON parse failed: ${e.message}`);
          continue;
        }
        if (!Array.isArray(arr)) {
          lines.push(`[source=${srcId}] file=${file} ERROR: not an array`);
          continue;
        }

        for (const caseData of arr) {
          summary.totalCases++;
          summary.sources[srcId].cases++;

          const citations = Array.isArray(caseData?.citations) ? caseData.citations : [];
          const official = citations.find(c => (c.type || c.citation_type) === 'official');
          if (!official || !official.cite) {
            summary.sources[srcId].missing_official++;
            lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} name=${JSON.stringify(caseData?.name)} WARN: missing_official_citation`);
            continue;
          }

          const parsed = parseVolumeReporterPage(official.cite);
          if (!parsed) {
            summary.sources[srcId].parse_error++;
            lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} cite=${JSON.stringify(official.cite)} WARN: parse_error`);
            continue;
          }

          if (!expectedReporter) {
            // Nothing to compare against in config
            continue;
          }

          const normExpected = normalizeReporter(expectedReporter);
          const normActual = normalizeReporter(parsed.reporter);

          const volumeOk = Number.isFinite(fileVol) && parsed.volume === fileVol;
          const correctedCanonical = rebuildCite(parsed, expectedReporter);
          const needsReporterSwap = volumeOk && normExpected !== normActual;
          const needsFormatFix = volumeOk && normExpected === normActual && parsed.normalized !== correctedCanonical;

          if (needsReporterSwap || needsFormatFix) {
            const correctedCite = correctedCanonical;
            summary.toCorrect++;
            summary.sources[srcId].toCorrect++;

            // Look up citation in DB
            let citRow;
            try {
              const res = await client.query(
                `SELECT id, cite, citation_type FROM citations 
                 WHERE case_id = $1 AND (citation_type = 'official' OR citation_type IS NULL)
                 ORDER BY CASE WHEN citation_type = 'official' THEN 0 ELSE 1 END, id LIMIT 1`,
                [caseData.id]
              );
              citRow = res.rows[0];
            } catch (e) {
              lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} cite=${JSON.stringify(official.cite)} ERROR: db_select_failed msg=${e.message}`);
              continue;
            }

            if (!citRow) {
              summary.sources[srcId].no_citation_in_db++;
              lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} cite=${JSON.stringify(official.cite)} WARN: no_citation_row_in_db`);
              continue;
            }

            const observed = official.cite;
            const observedReporter = parsed.reporter;
            const reason = needsReporterSwap
              ? 'source reporter override based on directory+config'
              : 'format normalization (spacing) based on directory+config';

            if (!options.apply) {
              lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} citation_id=${citRow.id} WOULD_CORRECT observed=${JSON.stringify(observed)} -> corrected=${JSON.stringify(correctedCite)} expected_reporter=${JSON.stringify(expectedReporter)} file_vol=${fileVol} reason=${reason}`);
              continue;
            }

            // Apply changes in a small transaction per correction for safety
            try {
              await client.query('BEGIN');

              await client.query(
                `INSERT INTO citation_corrections 
                  (citation_id, case_id, observed_cite, observed_reporter, corrected_cite, expected_reporter, source_id, file_volume, reason, confidence, applied, applied_at)
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,TRUE, NOW())`,
                [citRow.id, caseData.id, observed, observedReporter, correctedCite, expectedReporter, srcId, fileVol, reason, 'high']
              );

              await client.query(
                `UPDATE citations SET cite = $1 WHERE id = $2`,
                [correctedCite, citRow.id]
              );

              const note = needsReporterSwap
                ? `Corrected official reporter from ${observedReporter} to ${expectedReporter} based on source=${srcId}, file=${file}`
                : `Normalized official citation formatting (inserted spacing) using source=${srcId}, file=${file}`;
              const metadata = {
                observed_cite: observed,
                corrected_cite: correctedCite,
                observed_reporter: observedReporter,
                expected_reporter: expectedReporter,
                source_id: srcId,
                file_volume: fileVol,
                reason
              };

              await client.query(
                `INSERT INTO case_notes (case_id, note_type, note, metadata) VALUES ($1, $2, $3, $4::jsonb)`,
                [caseData.id, 'data_quality', note, JSON.stringify(metadata)]
              );

              await client.query('COMMIT');

              summary.corrected++;
              summary.sources[srcId].corrected++;
              lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} citation_id=${citRow.id} CORRECTED ${JSON.stringify(observed)} -> ${JSON.stringify(correctedCite)}`);
            } catch (e) {
              await client.query('ROLLBACK');
              lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} citation_id=${citRow?.id} ERROR: apply_failed msg=${e.message}`);
            }
          }
        }
      }
    }
  } finally {
    client.release();
    await pool.end();
  }

  // Write log
  const header = [`Corrections run: ${new Date().toISOString()}`, `Config: ${options.config}`, `Mode: ${options.apply ? 'apply' : 'dry-run'}`, ''];
  await fs.writeFile(outPath, header.concat(lines).join('\n'), 'utf8');

  // Summary
  console.log(`\n🧾 Corrections Summary (${options.apply ? 'apply' : 'dry-run'})`);
  console.log(`  Cases scanned: ${summary.totalCases}`);
  console.log(`  To correct:    ${summary.toCorrect}`);
  console.log(`  Corrected:     ${summary.corrected}`);
  for (const [src, s] of Object.entries(summary.sources)) {
    console.log(`  - ${src}: cases=${s.cases} toCorrect=${s.toCorrect} corrected=${s.corrected} missing_official=${s.missing_official} parse_error=${s.parse_error} no_citation_in_db=${s.no_citation_in_db}`);
  }
  console.log(`\n📝 Log written to: ${outPath}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err => {
    console.error('❌ Correction failed:', err.message);
    process.exit(1);
  });
}
