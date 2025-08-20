#!/usr/bin/env node

/**
 * Audit script for extracted case data quality
 *
 * Validates per source (from config):
 *  - Each case has an official citation
 *  - Official citation reporter matches config reporter
 *  - Official citation volume matches the extracted filename (e.g., data/extracted/<source>/<volume>.json)
 *
 * Writes errors to a log file and prints a summary.
 */

import fs from 'fs/promises';
import path from 'path';

function usage() {
  console.log(`
Audit Extracted Cases

USAGE:
  node src/audit.js -c <config.json> [--sample] [-v] [-o <out.log>]

OPTIONS:
  -c, --config <file>  Config JSON with sources[].id and sources[].reporter
  --sample             Process only the first file per source
  -v, --verbose        Verbose output
  -o, --out <file>     Output log file path (default: logs/audit-<config>-<timestamp>.log)
`);
}

async function loadConfig(configPath) {
  const content = await fs.readFile(configPath, 'utf8');
  return JSON.parse(content);
}

function normalizeReporter(r) {
  return (r || '')
    .toString()
    .replace(/\s+/g, '')
    .replace(/[.]/g, '')
    .toUpperCase();
}

function parseVolumeReporter(cite) {
  if (!cite || typeof cite !== 'string') return null;
  const norm = cite.replace(/\s+/g, ' ').trim();
  // Tolerate missing spaces between tokens (e.g., "282 A.D.651" or "282A.D. 651")
  const m = cite.match(/^(\d+)\s*([A-Za-z0-9.\s]+?)\s*(\d+)(?:\b|:|,|\(|$)/);
  if (!m) return null;
  return { volume: parseInt(m[1], 10), reporter: (m[2] || '').replace(/\s+/g, ' ').trim(), page: parseInt(m[3], 10) };
}

function getCliArgs() {
  const args = process.argv.slice(2);
  const opts = { config: null, verbose: false, sample: false, out: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case '-c':
      case '--config':
        opts.config = args[++i];
        break;
      case '--sample':
        opts.sample = true;
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

async function main() {
  const options = getCliArgs();
  const config = await loadConfig(options.config);

  const cfgBase = path.basename(options.config, '.json');
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const logsDir = path.join(process.cwd(), 'logs');
  await ensureDir(logsDir);
  const outPath = options.out || path.join(logsDir, `audit-${cfgBase}-${ts}.log`);

  const lines = [];
  const summary = { totalCases: 0, totalErrors: 0, sources: {} };

  const sources = Array.isArray(config.sources) ? config.sources : [];
  for (const source of sources) {
    const srcId = source.id;
    const expectedReporter = source.reporter || null;
    summary.sources[srcId] = { cases: 0, errors: 0, missing_official: 0, reporter_mismatch: 0, volume_mismatch: 0, parse_error: 0 };

    const extractedDir = path.join(process.cwd(), 'data', 'extracted', srcId);
    let files;
    try {
      files = (await fs.readdir(extractedDir)).filter(f => f.endsWith('.json'));
    } catch (e) {
      lines.push(`[source=${srcId}] ERROR: extracted dir not found: ${extractedDir}`);
      continue;
    }

    files.sort((a, b) => parseInt(a, 10) - parseInt(b, 10));
    const toProcess = options.sample ? files.slice(0, 1) : files;
    if (options.verbose) console.log(`📁 ${srcId}: processing ${toProcess.length}/${files.length} files${options.sample ? ' (sample)' : ''}`);

    for (const file of toProcess) {
      const fileVol = parseInt(path.basename(file, '.json'), 10);
      const filePath = path.join(extractedDir, file);
      let arr;
      try {
        arr = JSON.parse(await fs.readFile(filePath, 'utf8'));
      } catch (e) {
        lines.push(`[source=${srcId}] file=${file} ERROR: JSON parse failed: ${e.message}`);
        summary.sources[srcId].errors++;
        continue;
      }
      if (!Array.isArray(arr)) {
        lines.push(`[source=${srcId}] file=${file} ERROR: not an array`);
        summary.sources[srcId].errors++;
        continue;
      }

      for (const caseData of arr) {
        summary.totalCases++;
        summary.sources[srcId].cases++;

        const citations = Array.isArray(caseData?.citations) ? caseData.citations : [];
        const official = citations.find(c => (c.type || c.citation_type) === 'official');
        if (!official || !official.cite) {
          lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} name=${JSON.stringify(caseData?.name)} ERROR: missing_official_citation`);
          summary.totalErrors++;
          summary.sources[srcId].errors++;
          summary.sources[srcId].missing_official++;
          continue;
        }

        const parsed = parseVolumeReporter(official.cite);
        if (!parsed) {
          lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} name=${JSON.stringify(caseData?.name)} cite=${JSON.stringify(official.cite)} ERROR: official_parse_error`);
          summary.totalErrors++;
          summary.sources[srcId].errors++;
          summary.sources[srcId].parse_error++;
          continue;
        }

        // Reporter match (if config specifies)
        if (expectedReporter) {
          const normExpected = normalizeReporter(expectedReporter);
          const normActual = normalizeReporter(parsed.reporter);
          if (normExpected !== normActual) {
            lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} name=${JSON.stringify(caseData?.name)} cite=${JSON.stringify(official.cite)} ERROR: reporter_mismatch expected=${JSON.stringify(expectedReporter)} actual=${JSON.stringify(parsed.reporter)}`);
            summary.totalErrors++;
            summary.sources[srcId].errors++;
            summary.sources[srcId].reporter_mismatch++;
          }
        }

        // Volume match to filename
        if (Number.isFinite(fileVol) && parsed.volume !== fileVol) {
          lines.push(`[source=${srcId}] file=${file} case_id=${caseData?.id} name=${JSON.stringify(caseData?.name)} cite=${JSON.stringify(official.cite)} ERROR: volume_mismatch expected=${fileVol} actual=${parsed.volume}`);
          summary.totalErrors++;
          summary.sources[srcId].errors++;
          summary.sources[srcId].volume_mismatch++;
        }
      }
    }
  }

  // Write log
  const header = [`Audit run: ${new Date().toISOString()}`, `Config: ${options.config}`, ''];
  await fs.writeFile(outPath, header.concat(lines).join('\n'), 'utf8');

  // Print summary
  console.log(`\n📊 Audit Summary`);
  console.log(`  Cases audited: ${summary.totalCases}`);
  console.log(`  Errors found:  ${summary.totalErrors}`);
  for (const [src, stats] of Object.entries(summary.sources)) {
    console.log(`  - ${src}: cases=${stats.cases} errors=${stats.errors} missing_official=${stats.missing_official} reporter_mismatch=${stats.reporter_mismatch} volume_mismatch=${stats.volume_mismatch} parse_error=${stats.parse_error}`);
  }
  console.log(`\n📝 Log written to: ${outPath}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err => {
    console.error('❌ Audit failed:', err.message);
    process.exit(1);
  });
}
