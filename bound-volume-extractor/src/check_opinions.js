#!/usr/bin/env node
/**
 * Dev probe: detect cases, extract opinions, print per-case opinion summary.
 *
 * Usage:
 *   node src/check_opinions.js <ndjson> [--reporter=NY3d] [--volume=30] [--limit=10]
 */

import { readFile } from 'fs/promises';
import { classifyPages } from './sections.js';
import { detectCaseBoundaries } from './case_boundaries.js';
import { extractOpinions } from './opinions.js';

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
    console.error('usage: node src/check_opinions.js <ndjson> [--reporter=NY3d] [--volume=30] [--limit=10]');
    process.exit(2);
  }

  const pages = await loadPages(ndjson);
  const classification = classifyPages(pages);
  const cases = detectCaseBoundaries(pages, classification, { reporter, volume });

  let printed = 0;
  let totalOpinions = 0;
  for (const c of cases) {
    if (printed >= limit) break;
    const ops = extractOpinions(pages, c);
    totalOpinions += ops.length;
    console.log(`\n=== ${c.citation || '?'} (${c.section}) — ${ops.length} opinion(s) ===`);
    for (const op of ops) {
      const len = op.text.length;
      console.log(`  [${op.opinion_index}] ${op.opinion_type.padEnd(11)}  author=${(op.author || '<none>').padEnd(15)}  pp ${op.start_page_index}–${op.end_page_index}  text=${len}c`);
    }
    printed++;
  }
  console.log(`\nprinted ${printed} of ${cases.length} cases; total opinions across printed: ${totalOpinions}`);
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
