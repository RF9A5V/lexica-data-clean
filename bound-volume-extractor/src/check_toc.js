#!/usr/bin/env node
/**
 * Dev probe: parse the Table of Cases section, report stats and a sample.
 *
 * Usage:
 *   node src/check_toc.js <ndjson> [--reporter=NY3d] [--volume=30]
 */

import { readFile } from 'fs/promises';
import { classifyPages } from './sections.js';
import { buildTocMap } from './toc_parser.js';

function parseArgs() {
  const args = process.argv.slice(2);
  const positional = args.filter(a => !a.startsWith('--'));
  const flags = Object.fromEntries(
    args.filter(a => a.startsWith('--'))
        .map(a => a.replace(/^--/, '').split('='))
  );
  return {
    ndjson: positional[0],
    reporter: flags.reporter || 'NY3d',
    volume:   flags.volume ? parseInt(flags.volume, 10) : null,
  };
}

async function main() {
  const { ndjson, reporter, volume } = parseArgs();
  if (!ndjson) {
    console.error('usage: node src/check_toc.js <ndjson> [--reporter=NY3d] [--volume=30]');
    process.exit(2);
  }
  const raw = await readFile(ndjson, 'utf8');
  const pages = [];
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const r = JSON.parse(line);
    if (r.kind === 'page') pages.push(r);
  }
  pages.sort((a, b) => a.page_index - b.page_index);
  const cls = classifyPages(pages);
  const map = buildTocMap(pages, cls, { reporter, volume });
  console.log(`ToC entries: ${map.size}`);
  // Show first 15 + last 5 entries
  const sorted = [...map.entries()].sort((a, b) => a[0] - b[0]);
  console.log('\nFirst 15:');
  for (const [page, name] of sorted.slice(0, 15)) {
    console.log(`  ${String(page).padStart(5)}  ${name.slice(0, 80)}`);
  }
  console.log('\nLast 5:');
  for (const [page, name] of sorted.slice(-5)) {
    console.log(`  ${String(page).padStart(5)}  ${name.slice(0, 80)}`);
  }
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
