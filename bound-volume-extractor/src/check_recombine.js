#!/usr/bin/env node
/**
 * Dev probe: load an NDJSON file, run recombineWords on a chosen page, show
 * before/after for inspection. Lets us iterate on small_caps.js heuristics
 * without re-running the whole PDF extraction.
 *
 * Usage:
 *   node src/check_recombine.js <ndjson> [page_index] [--max=N]
 */

import { readFile } from 'fs/promises';
import { recombineWords, joinWords } from './small_caps.js';

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
    pageIndex: positional[1] != null ? parseInt(positional[1], 10) : 68,
    max: flags.max ? parseInt(flags.max, 10) : 30,
  };
}

async function loadPage(ndjsonPath, pageIndex) {
  const raw = await readFile(ndjsonPath, 'utf8');
  for (const line of raw.split('\n')) {
    if (!line.trim()) continue;
    const rec = JSON.parse(line);
    if (rec.kind === 'page' && rec.page_index === pageIndex) return rec;
  }
  throw new Error(`page ${pageIndex} not found in ${ndjsonPath}`);
}

function fmtWord(w) {
  const flag = w._recombined ? `[${w._components}p]` : '   ';
  return `  top=${String(w.top).padStart(6)} x0=${String(w.x0).padStart(6)} sz=${String(w.size).padStart(4)} ${flag} ${JSON.stringify(w.text)}`;
}

async function main() {
  const { ndjson, pageIndex, max } = parseArgs();
  if (!ndjson) {
    console.error('usage: node src/check_recombine.js <ndjson> [page_index] [--max=N]');
    process.exit(2);
  }
  const page = await loadPage(ndjson, pageIndex);
  console.log(`page ${pageIndex} — ${page.words.length} words total\n`);

  const recombined = recombineWords(page.words);
  const merged = recombined.filter(w => w._recombined);
  console.log(`recombined: ${merged.length} merged words from ${page.words.length} input words → ${recombined.length} output words\n`);

  console.log(`=== BEFORE (first ${max}) ===`);
  page.words.slice(0, max).forEach(w => console.log(fmtWord(w)));

  console.log(`\n=== AFTER (first ${max}) ===`);
  recombined.slice(0, max).forEach(w => console.log(fmtWord(w)));

  if (merged.length) {
    console.log(`\n=== ALL MERGED WORDS (${merged.length}) ===`);
    for (const m of merged) {
      console.log(`  ${m._components}p  ${JSON.stringify(m.text)}`);
    }
  }

  console.log(`\n=== JOINED LINE FROM AFTER ===`);
  console.log(`  ${joinWords(recombined.slice(0, max))}`);
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
