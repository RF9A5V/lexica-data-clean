#!/usr/bin/env node
import fs from 'fs/promises';
import { DOMParser, XMLSerializer } from 'xmldom-qsa';

// List of parseable elements by identifier conventions
const PARSEABLE_ELEMENTS = [
  'title', 'subtitle', 'chapter', 'subchapter', 'part', 'section',
  'subsection', 'paragraph', 'subparagraph', 'clause', 'subclause', 'item'
];

function getElementByIndexOrId(doc, type, indexOrId) {
  const elems = Array.from(doc.getElementsByTagName(type));
  if (!indexOrId) return null;
  if (/^\d+$/.test(indexOrId)) {
    // 1-based index
    return elems[parseInt(indexOrId, 10) - 1] || null;
  } else {
    // Try to match by identifier attribute
    return elems.find(e => e.getAttribute && e.getAttribute('identifier') === indexOrId) || null;
  }
}

function pruneChildParseables(node, parseableTypes) {
  // Count and remove direct child parseable elements, insert <childCount> summary
  const childCounts = {};
  const toRemove = [];
  for (let child of Array.from(node.childNodes)) {
    if (child.nodeType === 1 && parseableTypes.includes(child.localName)) {
      childCounts[child.localName] = (childCounts[child.localName] || 0) + 1;
      toRemove.push(child);
    }
  }
  for (let child of toRemove) {
    node.removeChild(child);
  }
  // Insert <childCount> elements for each parseable type found
  for (const [el, count] of Object.entries(childCounts)) {
    const cc = node.ownerDocument.createElement('childCount');
    cc.setAttribute('element', el);
    cc.setAttribute('count', count.toString());
    node.appendChild(cc);
  }
}

async function main() {
  // Support optional --hide-content flag
  let args = process.argv.slice(2);
  let hideContent = false;
  if (args.includes('--hide-content')) {
    hideContent = true;
    args = args.filter(a => a !== '--hide-content');
  }
  const [xmlPath, elementType, indexOrId, outPath] = args;
  if (!xmlPath || !elementType || !indexOrId) {
    console.error('Usage: node xml_pull.js <uscXML> <elementType> <index|identifier> [outputFile] [--hide-content]');
    process.exit(1);
  }
  if (!PARSEABLE_ELEMENTS.includes(elementType)) {
    console.error(`Element type must be one of: ${PARSEABLE_ELEMENTS.join(', ')}`);
    process.exit(2);
  }
  const xml = await fs.readFile(xmlPath, 'utf8');
  const doc = new DOMParser().parseFromString(xml, 'text/xml');
  const found = getElementByIndexOrId(doc, elementType, indexOrId);
  if (!found) {
    console.error(`${elementType} ${indexOrId} not found in ${xmlPath}`);
    process.exit(3);
  }
  if (hideContent) {
    // Remove all child nodes
    while (found.firstChild) found.removeChild(found.firstChild);
    // Get the original children from the unpruned node
    const original = getElementByIndexOrId(doc, elementType, indexOrId);
    for (let child of Array.from(original.childNodes)) {
      if (child.nodeType === 1) {
        const emptyTag = doc.createElement(child.localName);
        emptyTag.appendChild(doc.createTextNode('[empty]'));
        // Copy attributes (except for children)
        for (let attr of Array.from(child.attributes || [])) {
          emptyTag.setAttribute(attr.name, attr.value);
        }
        found.appendChild(emptyTag);
      }
    }
  } else {
    // Remove direct child parseable elements and insert <childCount> summaries
    pruneChildParseables(found, PARSEABLE_ELEMENTS);
  }
  const xmlOut = new XMLSerializer().serializeToString(found);
  if (outPath) {
    await fs.writeFile(outPath, xmlOut, 'utf8');
    console.log(`${elementType} ${indexOrId} written to ${outPath}`);
  } else {
    console.log(xmlOut);
  }
}

main().catch(e => { console.error(e); process.exit(1); });
