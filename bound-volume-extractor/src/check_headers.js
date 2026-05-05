#!/usr/bin/env node
/**
 * Dev probe: detect case boundaries, then run the case-header extractor on
 * each first-page. Prints summary fields for spot-checking.
 *
 * Usage:
 *   node src/check_headers.js <ndjson> [--reporter=NY3d] [--volume=30] [--limit=10]
 */

import { readFile } from 'fs/promises';
import { classifyPages } from './sections.js';
import { detectCaseBoundaries, extractRunningHeadName } from './case_boundaries.js';
import { extractCaseHeader } from './case_header.js';

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
    limit:    flags.limit  ? parseInt(flags.limit, 10)  : 10,
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
  const { ndjson, reporter, volume, limit } = parseArgs();
  if (!ndjson) {
    console.error('usage: node src/check_headers.js <ndjson> [--reporter=NY3d] [--volume=30] [--limit=10]');
    process.exit(2);
  }

  const pages = await loadPages(ndjson);
  const classification = classifyPages(pages);
  const cases = detectCaseBoundaries(pages, classification, { reporter, volume });

  let printed = 0;
  for (const c of cases) {
    if (printed >= limit) break;
    const firstPage = pages.find(p => p.page_index === c.start_page_index);
    const h = extractCaseHeader(firstPage, c.parallel_cites);
    const headName = extractRunningHeadName(pages, c, reporter);
    console.log(`\n=== ${c.citation || '?'}  pp ${c.start_page_index}–${c.end_page_index}  (${c.section}) ===`);
    console.log(`  short name:  ${headName || '<none>'}`);
    console.log(`  parallel:    ${c.parallel_cites.join(' / ')}`);
    console.log(`  decided:     ${h.decision_date || '?'}  (argued: ${h.argued_date || '?'}, ${h.argued_or_submitted || '?'})`);
    console.log(`  caption:     ${(h.caption_text || '<none>').slice(0, 180)}`);
    if (h.warnings.length)  console.log(`  warnings:    ${h.warnings.join('; ')}`);
    printed++;
  }
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
