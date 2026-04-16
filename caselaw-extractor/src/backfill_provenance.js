#!/usr/bin/env node

/**
 * Backfill Archive Provenance into Extracted JSON
 *
 * Injects `_source_id` (folder name) and `_file_volume` (filename number) into
 * every case in `data/extracted/<source>/<volume>.json`. This lets the loader
 * rebuild official citations from filesystem ground truth without needing a
 * full re-extraction from the upstream ZIPs.
 *
 * Run this after pulling the extractor fix; then re-run the combine + load
 * steps as usual.
 *
 * Idempotent: re-running over already-backfilled files is a no-op.
 * Dry-run by default; pass --apply to write changes.
 */

import fs from 'fs/promises';
import path from 'path';

function usage() {
  console.log(`
Backfill _source_id and _file_volume into extracted JSON

USAGE:
  node src/backfill_provenance.js -c <config.json> [--source <id>] [--vol <n>] [--apply] [-v]

OPTIONS:
  -c, --config <file>  Config JSON with sources[].id
  --source <id>        Restrict to a single source id (e.g. ny3d)
  --vol <n>            Restrict to a single volume within a source
  --apply              Write changes (default: dry-run)
  -v, --verbose        Per-file progress
`);
}

function getCliArgs() {
  const args = process.argv.slice(2);
  const opts = { config: null, source: null, vol: null, apply: false, verbose: false };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case '-c':
      case '--config':  opts.config = args[++i]; break;
      case '--source':  opts.source = args[++i]; break;
      case '--vol':
      case '--volume': opts.vol = parseInt(args[++i], 10); break;
      case '--apply':   opts.apply = true; break;
      case '-v':
      case '--verbose': opts.verbose = true; break;
      case '-h':
      case '--help':    usage(); process.exit(0);
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

async function loadConfig(configPath) {
  return JSON.parse(await fs.readFile(configPath, 'utf8'));
}

async function processFile(filePath, sourceId, fileVolume, options) {
  const raw = await fs.readFile(filePath, 'utf8');
  let arr;
  try {
    arr = JSON.parse(raw);
  } catch (e) {
    return { error: `parse_failed: ${e.message}` };
  }
  if (!Array.isArray(arr)) {
    return { error: 'not_an_array' };
  }

  let mutated = 0;
  let alreadyPresent = 0;
  for (const c of arr) {
    if (!c || typeof c !== 'object') continue;
    const hasSource = c._source_id != null;
    const hasVolume = c._file_volume != null;
    if (hasSource && hasVolume) {
      alreadyPresent++;
      continue;
    }
    if (!hasSource) c._source_id = sourceId;
    if (!hasVolume) c._file_volume = fileVolume;
    mutated++;
  }

  if (mutated > 0 && options.apply) {
    // Preserve the existing pretty-print format (matches extractor.js output).
    await fs.writeFile(filePath, JSON.stringify(arr, null, 2), 'utf8');
  }

  return { mutated, alreadyPresent, total: arr.length };
}

async function main() {
  const options = getCliArgs();
  const config = await loadConfig(options.config);

  const summary = {};
  const sources = Array.isArray(config.sources) ? config.sources : [];

  for (const source of sources) {
    const sourceId = source.id;
    if (options.source && options.source !== sourceId) continue;
    summary[sourceId] = { files: 0, cases_mutated: 0, cases_already_present: 0, cases_total: 0, errors: [] };

    const extractedDir = path.join(process.cwd(), 'data', 'extracted', sourceId);
    let files;
    try {
      files = (await fs.readdir(extractedDir)).filter(f => f.endsWith('.json'));
    } catch (e) {
      summary[sourceId].errors.push(`extracted dir not found: ${extractedDir}`);
      continue;
    }
    files.sort((a, b) => parseInt(a, 10) - parseInt(b, 10));

    let toProcess = files;
    if (Number.isFinite(options.vol)) {
      toProcess = files.filter(f => parseInt(path.basename(f, '.json'), 10) === options.vol);
    }

    for (const file of toProcess) {
      const volumeRaw = parseInt(path.basename(file, '.json'), 10);
      const fileVolume = Number.isFinite(volumeRaw) ? volumeRaw : null;
      const filePath = path.join(extractedDir, file);
      const result = await processFile(filePath, sourceId, fileVolume, options);
      if (result.error) {
        summary[sourceId].errors.push(`${file}: ${result.error}`);
        continue;
      }
      summary[sourceId].files++;
      summary[sourceId].cases_mutated += result.mutated;
      summary[sourceId].cases_already_present += result.alreadyPresent;
      summary[sourceId].cases_total += result.total;
      if (options.verbose) {
        console.log(`  [${sourceId}] ${file}: ${result.mutated}/${result.total} cases updated (${result.alreadyPresent} already present)`);
      }
    }
  }

  console.log(`\n=== ${options.apply ? 'APPLIED' : 'DRY RUN'} ===`);
  for (const [sourceId, s] of Object.entries(summary)) {
    console.log(`[${sourceId}] files=${s.files} cases_total=${s.cases_total} cases_mutated=${s.cases_mutated} already_present=${s.cases_already_present}${s.errors.length ? ` errors=${s.errors.length}` : ''}`);
    for (const err of s.errors) console.log(`  ERROR: ${err}`);
  }
  if (!options.apply) {
    console.log('\n(Dry run — pass --apply to write changes.)');
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
