// Script to populate usc_elements table from all section_text.ndjson files
// Usage: node scripts/populate_usc_elements.js

import path from 'path';
import { fileURLToPath } from 'url';
import { openPg, createElementsTableOnly, insertElement } from '../db.js';
import { iterSectionTextFiles } from '../text_utils.js';
import { loadElementsFromNdjson } from '../elementLoader.js';
import dotenv from 'dotenv';
dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PARSED_DIR = path.resolve(__dirname, '../../../../data/parsed');

async function validateInsert(pg, identifiers) {
  // Check how many identifiers are present in the table
  if (!identifiers.length) return;
  const res = await pg.query(
    `SELECT element_id, element_type, heading FROM usc_elements WHERE element_id = ANY($1)`,
    [identifiers]
  );
  console.log(`[VALIDATION] Inserted ${res.rows.length} elements. Sample:`);
  res.rows.slice(0, 3).forEach(row => {
    console.log(` - [${row.element_id}] (${row.element_type}): ${row.heading}`);
  });
}

async function main() {
  const titleArg = process.argv[2]; // optional
  const pg = await openPg();
  await createElementsTableOnly(pg);
  let totalInserted = 0;
  for await (const { title, sectionTextFile } of iterSectionTextFiles(PARSED_DIR)) {
    if (titleArg) {
      const normalizedTitle = title.startsWith('title_') ? title.slice(6) : title;
      if (normalizedTitle !== titleArg && title !== titleArg) continue;
    }
    console.log(`Processing ${title} ...`);
    const elements = await loadElementsFromNdjson(sectionTextFile);
    // --- Synthesize missing containers ---
    const seen = new Set(elements.map(e => e.identifier));
    const allElementsMap = new Map(elements.map(e => [e.identifier, e]));
    const synthesized = [];
    function getAncestorPaths(identifier) {
      const segments = identifier.split('/');
      const ancestors = [];
      // Start after '/us/usc', so index 4 (['', 'us', 'usc', 't1', ...])
      // For a path of length N, generate all paths from 4 to N-1
      for (let i = 4; i < segments.length; ++i) {
        ancestors.push(segments.slice(0, i).join('/'));
      }
      return ancestors;
    }
    function inferTypeFromIdentifier(identifier) {
  const segments = identifier.split('/').filter(Boolean);
  // Heuristic: look at last segment or pattern
  const last = segments[segments.length - 1];
  const depth = segments.length;

  // US Code convention: depth-based typing
  // 4: title (t1), 5: section (s112b), 6: subsection (a), 7: paragraph (1), 8: subparagraph (A), 9: clause (i), 10: subclause (I)
  if (/^t\d+$/.test(last) && depth === 4) return 'title';
  if (/^ch\d+$/.test(last)) return 'chapter';
  if (/^s\d+[a-z]*$/.test(last) && depth === 5) return 'section';
  if (/^[a-z]+$/.test(last) && depth === 6) return 'subsection';
  if (/^\d+$/.test(last) && depth === 7) return 'paragraph';
  if (/^[A-Z]+$/.test(last) && depth === 8) return 'subparagraph';
  if (/^[ivxlc]+$/.test(last) && depth === 9) return 'clause';
  if (/^[IVXLCDM]+$/.test(last) && depth === 10) return 'subclause';

  // Fallbacks if pattern doesn't match depth
  if (/^s\d+[a-z]*$/.test(last)) return 'section';
  if (/^[a-z]+$/.test(last)) return 'subsection';
  if (/^\d+$/.test(last)) return 'paragraph';
  if (/^[A-Z]+$/.test(last)) return 'subparagraph';
  if (/^[ivxlc]+$/.test(last)) return 'clause';
  if (/^[IVXLCDM]+$/.test(last)) return 'subclause';
  return 'container'; // fallback
}
    for (const elem of elements) {
      for (const ancestor of getAncestorPaths(elem.identifier)) {
        if (!seen.has(ancestor)) {
          const type = inferTypeFromIdentifier(ancestor);
          const synth = {
            identifier: ancestor,
            type,
            heading: null,
            original_text: null,
            synthesized: true
          };
          synthesized.push(synth);
          seen.add(ancestor);
          allElementsMap.set(ancestor, synth);
        }
      }
    }
    // Merge and sort so parents come before children
    const allElements = Array.from(allElementsMap.values());
    allElements.sort((a, b) => a.identifier.split('/').length - b.identifier.split('/').length);
    // --- Insert into DB ---
    let inserted = 0;
    const ids = [];
    for (const elem of allElements) {
      await insertElement(pg, elem.identifier, elem.type, elem.heading, elem.original_text);
      inserted++;
      ids.push(elem.identifier);
    }
    totalInserted += inserted;
    const synthCount = synthesized.length;
    console.log(`[${title}] Inserted ${inserted} elements (${synthCount} synthesized containers).`);
    if (synthCount > 0) {
      console.log(`[${title}] Sample synthesized:`, synthesized.slice(0, 3));
    }
    await validateInsert(pg, ids);
  }
  // Final count
  const { rows } = await pg.query('SELECT COUNT(*) FROM usc_elements');
  console.log(`\n[COMPLETE] Total elements in usc_elements: ${rows[0].count}`);
  await pg.end();
}

main().catch(e => { console.error(e); process.exit(1); });
