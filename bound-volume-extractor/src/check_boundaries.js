#!/usr/bin/env node
/**
 * Dev probe: load an NDJSON file, classify sections, walk case boundaries,
 * and print the resulting case ranges.
 *
 * Usage:
 *   node src/check_boundaries.js <ndjson> [--reporter=NY3d] [--volume=30]
 */

import { readFile } from 'fs/promises';
import { classifyPages } from './sections.js';
import { detectCaseBoundaries } from './case_boundaries.js';

function parseArgs() {
  const args = process.argv.slice(2);
  const positional = args.filter(a => !a.startsWith('--'));
  const flags = Object.fromEntries(
    args.filter(a => a.startsWith('--'))
        .map(a => a.replace(/^--/, '').split('='))
        .map(([k, v]) => [k, v ?? true])
  );
  return {
    ndjson: positional[0],
    reporter: flags.reporter || 'NY3d',
    volume:   flags.volume ? parseInt(flags.volume, 10) : null,
  };
}

async function loadPages(ndjsonPath) {
  const raw = await readFile(ndjsonPath, 'utf8');
  const pages = [];
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const rec = JSON.parse(line);
    if (rec.kind === 'page') pages.push(rec);
  }
  pages.sort((a, b) => a.page_index - b.page_index);
  return pages;
}

async function main() {
  const { ndjson, reporter, volume } = parseArgs();
  if (!ndjson) {
    console.error('usage: node src/check_boundaries.js <ndjson> [--reporter=NY3d] [--volume=30]');
    process.exit(2);
  }

  const pages = await loadPages(ndjson);
  const classification = classifyPages(pages);
  const cases = detectCaseBoundaries(pages, classification, { reporter, volume });

  const opinions  = cases.filter(c => c.section === 'opinions');
  const memoranda = cases.filter(c => c.section === 'memoranda');
  console.log(`opinions:  ${opinions.length} case(s)`);
  console.log(`memoranda: ${memoranda.length} case(s)`);
  console.log(`total:     ${cases.length}`);

  console.log(`\n=== OPINIONS ===`);
  for (const c of opinions) {
    const span = c.end_page_index - c.start_page_index + 1;
    console.log(`  pp ${String(c.start_page_index).padStart(4)}–${String(c.end_page_index).padStart(4)} (${String(span).padStart(3)}p)  ${(c.citation || '?').padEnd(14)}  parallel: ${c.parallel_cites.join(' / ')}`);
  }

  console.log(`\n=== MEMORANDA (first 10) ===`);
  for (const c of memoranda.slice(0, 10)) {
    const span = c.end_page_index - c.start_page_index + 1;
    console.log(`  pp ${String(c.start_page_index).padStart(4)}–${String(c.end_page_index).padStart(4)} (${String(span).padStart(3)}p)  ${(c.citation || '?').padEnd(14)}  parallel: ${c.parallel_cites.join(' / ')}`);
  }
  if (memoranda.length > 10) console.log(`  ... ${memoranda.length - 10} more`);
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
