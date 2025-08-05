// parseUslm.js
// Next-gen USLM XML parser for NDJSON extraction, using identifier conventions and hierarchical logic.
// Only emits entries for elements with identifiers, attaches non-parseable children as metadata.
// Usage: node parseUslm.js <uscXML> <outputNDJSON>

import fs from 'fs/promises';
import { DOMParser } from 'xmldom-qsa';

// List of parseable elements (from identifier_conventions.md)
const PARSEABLE_ELEMENTS = [
  'title', 'subtitle', 'chapter', 'subchapter', 'part', 'subpart', 'section',
  'subsection', 'paragraph', 'subparagraph', 'clause', 'subclause', 'item'
];

function hasIdentifier(node) {
  return node.nodeType === 1 && node.hasAttribute('identifier');
}

function extractMetadata(node) {
  // Collect non-parseable children as metadata (e.g., notes, toc, etc.)
  const meta = {};
  for (let child of Array.from(node.childNodes)) {
    if (child.nodeType === 1 && !PARSEABLE_ELEMENTS.includes(child.localName)) {
      if (!meta[child.localName]) meta[child.localName] = [];
      meta[child.localName].push(child.toString());
    }
  }
  return meta;
}

function nodeToJson(node, seenMap) {
  // Extract identifier
  let identifier = node.getAttribute && node.getAttribute('identifier');
  // Determine type
  const type = node.localName;
  // Prepare outputs
  const rows = [];
  // Extract heading
  let heading = null;
  let hasHeading = false;
  for (let child of Array.from(node.childNodes || [])) {
    if (child.nodeType === 1 && child.localName === 'heading' && child.textContent.trim()) {
      heading = child.textContent.trim();
      hasHeading = true;
      break;
    }
  }
  if (hasHeading) {
    rows.push({ identifier, type, contentType: 'heading', content: heading });
  }
  // Utility to strip leading marker patterns like (a), (1), (A), (i), possibly chained
  function stripLeadingMarkers(text) {
    return text.replace(/^(\s*(\([a-zA-Z0-9ivxlcdmIVXLCDM]+\))+)+\s*/, '');
  }
  // Only extract body content from direct <content>, <chapeau>, <p> children, not from <num> or concatenated text
  let bodyParts = [];
  for (let child of Array.from(node.childNodes || [])) {
    if (child.nodeType === 1) {
      // Remove footnote text from content, chapeau, and p
      if (['content', 'chapeau', 'p'].includes(child.localName)) {
        let text = '';
        // If this element has a footnote child, remove its text
        if (Array.from(child.childNodes).some(n => n.nodeType === 1 && n.localName === 'footnote')) {
          // Remove all <footnote> text from the aggregate
          let clone = child.cloneNode(true);
          for (let fn of Array.from(clone.getElementsByTagName('footnote'))) {
            fn.parentNode.removeChild(fn);
          }
          text = clone.textContent.trim();
        } else {
          text = child.textContent.trim();
        }
        if (text) bodyParts.push(text);
      }
    }
  }
  // Emit a single aggregated body row if any body parts exist
  if (bodyParts.length > 0) {
    rows.push({ identifier, type, contentType: 'body', content: stripLeadingMarkers(bodyParts.join('\n')) });
  }
  // Only emit fallback body if there are no parseable children (leaf node)
  const hasParseableChild = Array.from(node.childNodes || []).some(
    child => child.nodeType === 1 &&
             PARSEABLE_ELEMENTS.includes(child.localName) &&
             child.hasAttribute && child.hasAttribute('identifier')
  );
  if (rows.length === 0 && !hasParseableChild && node.textContent && node.textContent.trim()) {
    rows.push({ identifier, type, contentType: 'body', content: stripLeadingMarkers(node.textContent.trim()) });
  }
  // Uniquify rows by identifier|type|contentType
  const outRows = [];
  for (const row of rows) {
    const key = `${row.identifier}|${row.type}|${row.contentType}`;
    if (!seenMap[key]) {
      seenMap[key] = 1;
      outRows.push(row);
    } else {
      row.identifier = `${row.identifier}#dup${seenMap[key]}`;
      seenMap[key] += 1;
      outRows.push(row);
    }
  }
  return outRows;
}

function* traverseParseables(node) {
  if (hasIdentifier(node)) {
    yield node;
  }
  for (let child of Array.from(node.childNodes)) {
    if (child.nodeType === 1 && PARSEABLE_ELEMENTS.includes(child.localName)) {
      yield* traverseParseables(child);
    }
  }
}

async function main() {
  const [,, xmlPath, outPath] = process.argv;
  if (!xmlPath || !outPath) {
    console.error('Usage: node parseUslm.js <uscXML> <outputNDJSON>');
    process.exit(1);
  }
  const xml = await fs.readFile(xmlPath, 'utf8');
  const doc = new DOMParser().parseFromString(xml, 'text/xml');
  // Find <main> element to start traversal
  const main = doc.getElementsByTagName('main')[0];
  if (!main) {
    console.error('No <main> element found in XML.');
    process.exit(2);
  }
  const ndjsonLines = [];
  const seenMap = {};
  for (const node of traverseParseables(main)) {
    const rows = nodeToJson(node, seenMap);
    for (const row of rows) {
      ndjsonLines.push(JSON.stringify(row));
    }
  }
  await fs.writeFile(outPath, ndjsonLines.join('\n'), 'utf8');
  console.log(`Wrote ${ndjsonLines.length} entries to ${outPath}`);
}

if (import.meta && import.meta.url && process.argv[1] === new URL(import.meta.url).pathname) {
  main().catch(e => { console.error(e); process.exit(1); });
}
