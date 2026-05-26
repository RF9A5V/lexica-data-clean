#!/usr/bin/env node
import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { NysenateClient } from './client/nysenate.js';
import { fetchAllLawTrees, fetchRepealed } from './fetch/fetch_laws.js';
import { transformAllCachedLaws } from './transform/transform.js';
import { loadNdjsonToDatabase } from './load/loader.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const CACHE_DIR = path.join(ROOT, 'data', 'cache');
const NDJSON_PATH = path.join(ROOT, 'data', 'ndjson', 'nys.ndjson');

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const eq = a.indexOf('=');
      if (eq >= 0) args[a.slice(2, eq)] = a.slice(eq + 1);
      else if (argv[i + 1] && !argv[i + 1].startsWith('-')) args[a.slice(2)] = argv[++i];
      else args[a.slice(2)] = true;
    } else {
      args._.push(a);
    }
  }
  return args;
}

function listFromArg(v) {
  if (!v || v === true) return null;
  return String(v).split(',').map((s) => s.trim()).filter(Boolean);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = args._[0];

  if (!cmd || cmd === 'help' || cmd === '-h' || cmd === '--help') {
    console.log(`
Usage:
  node src/index.js fetch [--only=PEN,CPL] [--force] [--skip-repealed]
  node src/index.js transform [--only=PEN]
  node src/index.js load [--no-truncate] [--verbose]
  node src/index.js all [--only=PEN,CPL] [--force]
`);
    return;
  }

  const only = listFromArg(args.only);

  if (cmd === 'fetch' || cmd === 'all') {
    const client = new NysenateClient({});
    await fetchAllLawTrees(client, { cacheDir: CACHE_DIR, force: !!args.force, only });
    if (!args['skip-repealed']) {
      await fetchRepealed(client, { cacheDir: CACHE_DIR, force: !!args.force });
    }
  }

  if (cmd === 'transform' || cmd === 'all') {
    await transformAllCachedLaws({ cacheDir: CACHE_DIR, outFile: NDJSON_PATH, only });
  }

  if (cmd === 'load' || cmd === 'all') {
    const dbConfig = {
      host: process.env.PGHOST || 'localhost',
      port: parseInt(process.env.PGPORT || '5432', 10),
      user: process.env.PGUSER || 'dev',
      password: process.env.PGPASSWORD || 'dev',
      database: process.env.PGDATABASE || 'nys_legislative',
    };
    const opts = { truncate: !args['no-truncate'], verbose: !!args.verbose || !!args.v };
    const counts = await loadNdjsonToDatabase(NDJSON_PATH, dbConfig, opts);
    console.log('\nload counts:', counts);
  }
}

main().catch((e) => {
  console.error('FAILED:', e);
  process.exit(1);
});
