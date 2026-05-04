#!/usr/bin/env node
/**
 * Dev probe: load an NDJSON file, run the section classifier, and print the
 * detected section ranges. Useful for verifying banners against a new volume
 * before driving the full pipeline.
 *
 * Usage:
 *   node src/check_sections.js <ndjson>
 */

import { readFile } from 'fs/promises';
import { classifyPages, sectionRanges } from './sections.js';

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
  const ndjson = process.argv[2];
  if (!ndjson) {
    console.error('usage: node src/check_sections.js <ndjson>');
    process.exit(2);
  }
  const pages = await loadPages(ndjson);
  console.log(`loaded ${pages.length} pages`);

  const classification = classifyPages(pages);
  const ranges = sectionRanges(classification);

  console.log(`\n=== SECTION RANGES ===`);
  for (const r of ranges) {
    const span = r.end - r.start + 1;
    console.log(`  ${r.section.padEnd(13)} pp ${String(r.start).padStart(4)}–${String(r.end).padStart(4)} (${span} pp)`);
  }

  console.log(`\n=== BANNER PAGES ===`);
  for (const c of classification) {
    if (c.banner) console.log(`  p${String(c.page_index).padStart(4)} → ${c.section}`);
  }
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
