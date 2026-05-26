/**
 * RCNY parser — single-purpose ALP XML extractor.
 *
 * Walks the LEVEL/RECORD tree, emits one NDJSON unit per legal subdivision
 * (Title / Chapter / Subchapter / Section / etc.). Section bodies are folded
 * from nested "Normal Level" RECORDs. Internal LINKs and external citations
 * are captured for downstream resolution.
 *
 * Output NDJSON record types:
 *   - { type: 'unit', ... }
 *   - { type: 'internal_link', source_record_id, target_destination, raw_text }
 *   - { type: 'citation', source_record_id, raw_citation, target_kind, external_curie, context_snippet }
 *
 * Stable identity:
 *   record_id          — ALP <RECORD id="0-0-0-NNNNN"> (used as DB primary key)
 *   canonical_address  — <DESTINATION name="T15C041_41-01"> (used for hierarchy + cross-refs)
 *   canonical_id       — derived CURIE (rcny:15-41-01) for the case-side join
 */

import fs from 'fs/promises';
import path from 'path';
import { XMLParser } from 'fast-xml-parser';

// ALP style-name → DB unit_type. Anything not listed is skipped as
// wrapping. Section is the lowest-emitted granularity by design — the
// previous build also emitted Subsection units, but their body text
// never made it into unit_text_versions (only sections invoked the
// body-folding step), and the splits at the subsection level proved
// unreliable when ALP tagged the same content inconsistently across
// titles. We now fold subsection bodies into the parent section's
// `text_plain` and skip the subsection emit entirely. The hierarchy
// above section (title / chapter / part / etc.) is preserved.
const STYLE_TO_TYPE = {
  Title: 'title',
  Subtitle: 'subtitle',
  Chapter: 'chapter',
  Subchapter: 'subchapter',
  Part: 'part',
  Subpart: 'subpart',
  Section: 'section',
};

// Style-names whose RECORDs are body paragraphs, not their own units.
const BODY_HOLDER_STYLES = new Set(['Normal Level']);

// Style-names that wrap a sub-region of a Section's body (a sub-heading
// plus more Normal-Level paragraphs). When `collectSectionBody`
// encounters one of these, it descends and folds the inner content
// into the section's consolidated text instead of treating it as a
// standalone unit. Add new ALP styles here as they're observed.
const BODY_SUBDIVIDER_STYLES = new Set(['Subsection']);

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  allowBooleanAttributes: true,
  parseAttributeValue: false,
  parseTagValue: false,
  trimValues: true,
  preserveOrder: false,
});

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

export async function parseRcnyToNdjson(stagingDir, ndjsonOutput, options = {}) {
  const { verbose = false } = options;

  const xmlFiles = await listCanonicalXmlFiles(stagingDir);
  if (verbose) console.log(`  ${xmlFiles.length} canonical XML files (excluding _1/_2/_3 duplicates)`);

  await fs.mkdir(path.dirname(ndjsonOutput), { recursive: true });
  const out = await fs.open(ndjsonOutput, 'w');

  const counts = { units: 0, links: 0, citations: 0, files: 0, skippedRecords: 0 };
  const seenRecordIds = new Set();
  const seenCanonicalAddresses = new Set();

  try {
    for (const file of xmlFiles) {
      const xmlText = await fs.readFile(file, 'utf-8');
      const parsed = xmlParser.parse(xmlText);
      const root = parsed.DOCUMENT;
      if (!root) {
        if (verbose) console.warn(`  ${path.basename(file)}: no <DOCUMENT> root, skipping`);
        continue;
      }

      const fileCounts = await emitFromDocument(root, out, {
        seenRecordIds,
        seenCanonicalAddresses,
      });
      counts.units += fileCounts.units;
      counts.links += fileCounts.links;
      counts.citations += fileCounts.citations;
      counts.skippedRecords += fileCounts.skippedRecords;
      counts.files += 1;

      if (verbose && counts.files % 100 === 0) {
        console.log(`  ${counts.files} files, ${counts.units} units, ${counts.citations} citations`);
      }
    }
  } finally {
    await out.close();
  }

  return counts;
}

// ---------------------------------------------------------------------------
// File walk — accept only the canonical filename; reject _1/_2/_3 duplicates.
// ---------------------------------------------------------------------------

async function listCanonicalXmlFiles(stagingDir) {
  const entries = await fs.readdir(stagingDir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    if (!entry.name.endsWith('.xml')) continue;
    if (/_[123]\.xml$/.test(entry.name)) continue;
    files.push(path.join(stagingDir, entry.name));
  }
  files.sort();
  return files;
}

// ---------------------------------------------------------------------------
// Document → unit emission
// ---------------------------------------------------------------------------

async function emitFromDocument(documentNode, out, dedup) {
  const counts = { units: 0, links: 0, citations: 0, skippedRecords: 0 };

  const topLevels = asArray(documentNode.LEVEL);
  for (const level of topLevels) {
    await walkLevel(level, null, null, out, counts, dedup);
  }
  return counts;
}

async function walkLevel(level, parentRecordId, ancestorAddress, out, counts, dedup) {
  const styleName = level['@_style-name'];

  // Body-holder LEVELs are not their own units; their RECORDs are paragraphs
  // attached to the enclosing section. They are handled inline by the parent
  // section's body-collection pass, so skip here.
  if (BODY_HOLDER_STYLES.has(styleName)) return;

  const unitType = STYLE_TO_TYPE[styleName];

  // First RECORD child of this LEVEL is the heading record (if any).
  const headingRecord = pickFirstRecord(level);

  let emittedRecordId = null;
  let emittedCanonicalAddress = null;

  if (unitType && headingRecord) {
    const unit = await buildUnit(unitType, headingRecord, parentRecordId, ancestorAddress, level, out, counts, dedup);
    if (unit) {
      emittedRecordId = unit.record_id;
      emittedCanonicalAddress = unit.canonical_address;
    } else {
      counts.skippedRecords += 1;
    }
  }

  // Choose the ancestor address to thread into descendants. Prefer the
  // just-emitted unit's address (when it's a chapter or higher); otherwise
  // inherit the caller's. Sections are leaves and don't extend the chain.
  const childAncestor = (unitType !== 'section' && emittedCanonicalAddress)
    ? emittedCanonicalAddress
    : ancestorAddress;

  // Recurse into nested LEVELs, threading the just-emitted unit's id as parent.
  const nestedLevels = asArray(level.LEVEL);
  for (const child of nestedLevels) {
    if (BODY_HOLDER_STYLES.has(child['@_style-name'])) continue; // handled by buildUnit
    await walkLevel(child, emittedRecordId || parentRecordId, childAncestor, out, counts, dedup);
  }
}

async function buildUnit(unitType, headingRecord, parentRecordId, ancestorAddress, levelNode, out, counts, dedup) {
  const recordId = headingRecord['@_id'];
  if (!recordId) return null;
  if (dedup.seenRecordIds.has(recordId)) return null;
  dedup.seenRecordIds.add(recordId);

  let canonicalAddress = findDestinationName(headingRecord);

  // Section-level fallback: if no DESTINATION on this section but we have
  // a chapter-style ancestor address and a heading-derived section number,
  // synthesize <ancestor>_<sec> so deriveCanonicalId can yield a CURIE.
  // Recovers sections in chapters whose own DESTINATION was missing/malformed.
  if (!canonicalAddress && unitType === 'section' && ancestorAddress) {
    const headingTextEarly = extractHeadingText(headingRecord);
    const { number: numEarly } = splitHeading(unitType, headingTextEarly);
    if (numEarly && /^[\w.-]+$/.test(numEarly) && /^T\d+(?:C[A-Za-z0-9.]+|App[A-Z]+)?$/.test(ancestorAddress)) {
      canonicalAddress = `${ancestorAddress}_${numEarly}`;
    }
  }
  // We dedup on canonical address too, since the same section can appear in
  // a TOC file and its chapter file; the canonical address is the legal
  // identity, the record_id is the per-file identity.
  if (canonicalAddress && dedup.seenCanonicalAddresses.has(canonicalAddress)) {
    return null;
  }
  if (canonicalAddress) dedup.seenCanonicalAddresses.add(canonicalAddress);

  const headingText = extractHeadingText(headingRecord);
  const { number, label } = splitHeading(unitType, headingText);

  // For sections, fold sibling Normal-Level RECORDs into a single body.
  let textPlain = '';
  let internalLinks = [];
  if (unitType === 'section') {
    const folded = collectSectionBody(levelNode);
    textPlain = folded.text;
    internalLinks = folded.links;
  }

  const canonicalId = deriveCanonicalId(unitType, canonicalAddress, number);

  const unit = {
    type: 'unit',
    record_id: recordId,
    canonical_address: canonicalAddress,
    canonical_id: canonicalId,
    parent_record_id: parentRecordId,
    unit_type: unitType,
    number,
    label,
    citation: formatCitation(canonicalId, unitType, number, label),
    sort_key: buildSortKey(unitType, number),
    text_plain: textPlain || null,
    source_id: 'rcny',
  };
  await writeJsonLine(out, unit);
  counts.units += 1;

  // Internal links: emit one record per LINK pointing to another RCNY destination.
  for (const link of internalLinks) {
    await writeJsonLine(out, {
      type: 'internal_link',
      source_record_id: recordId,
      target_destination: link.targetDestination,
      raw_text: link.rawText,
    });
    counts.links += 1;
  }

  // External citations: scan the consolidated body text with the canonical regex set.
  if (textPlain) {
    const citations = extractExternalCitations(textPlain);
    for (const c of citations) {
      await writeJsonLine(out, {
        type: 'citation',
        source_record_id: recordId,
        raw_citation: c.raw,
        target_kind: c.targetKind,
        external_curie: c.curie,
        context_snippet: c.context,
      });
      counts.citations += 1;
    }
  }

  return unit;
}

// ---------------------------------------------------------------------------
// Heading + body extraction
// ---------------------------------------------------------------------------

function pickFirstRecord(levelNode) {
  const records = asArray(levelNode.RECORD);
  return records.length ? records[0] : null;
}

function findDestinationName(record) {
  // <DESTINATION name="T15C041_41-01"/> can sit inside any PARA descendant.
  const paras = asArray(record.PARA);
  for (const para of paras) {
    const dest = findFirstDescendant(para, 'DESTINATION');
    if (dest && dest['@_name']) return dest['@_name'];
  }
  return null;
}

function extractHeadingText(record) {
  const heading = record.HEADING;
  if (heading) return extractText(heading);
  // Fallback: first PARA contents minus DESTINATION/CHARFORMAT artifacts.
  const paras = asArray(record.PARA);
  if (paras.length) return extractText(paras[0]);
  return '';
}

/**
 * Collect body text + internal links from the descendants of a Section.
 *
 * Walks the Section's nested LEVELs and folds the entire subtree into a
 * single consolidated body, with these rules:
 *   - Normal-Level RECORDs contribute their paragraph text.
 *   - Subsection (and any future BODY_SUBDIVIDER_STYLES) levels are
 *     recursed into. Their HEADING is emitted as an inline sub-heading
 *     line so the rendered text reads like "(a) Definition. ..." rather
 *     than just running the subsection paragraphs together.
 *   - Anything else (nested Section, Chapter, etc.) is left alone — those
 *     are independent units and shouldn't be inhaled here.
 */
function collectSectionBody(sectionLevel) {
  const parts = [];
  const links = [];

  function walk(node, depth) {
    const nested = asArray(node.LEVEL);
    for (const child of nested) {
      const style = child['@_style-name'];
      if (BODY_HOLDER_STYLES.has(style)) {
        const records = asArray(child.RECORD);
        for (const rec of records) {
          const paras = asArray(rec.PARA);
          for (const para of paras) {
            const text = extractText(para);
            if (text) parts.push(text);
            collectLinks(para, links);
          }
        }
      } else if (BODY_SUBDIVIDER_STYLES.has(style)) {
        // Subsection-style level: emit its heading inline (so the
        // reader sees the sub-heading), then recurse into its body.
        const subRecord = pickFirstRecord(child);
        if (subRecord) {
          const subHeading = extractHeadingText(subRecord);
          if (subHeading) parts.push(subHeading);
          collectLinks(subRecord, links);
        }
        walk(child, depth + 1);
      }
      // Other styles (nested Section, Chapter, etc.) are ignored —
      // they belong to their own emit pass.
    }
  }

  walk(sectionLevel, 0);

  return {
    text: parts.join('\n\n').replace(/\s+\n/g, '\n').trim(),
    links,
  };
}

/**
 * Recursively walk a node, returning concatenated text content. Inline LINK
 * elements emit their own text content (so "15 RCNY § 41-05" round-trips
 * correctly). DESTINATION elements are skipped (they're anchors, not text).
 * TAB and LINEBRK become spaces.
 */
function extractText(node) {
  if (node === null || node === undefined) return '';
  if (typeof node === 'string') return node;
  if (typeof node !== 'object') return String(node);

  const parts = [];

  // Direct text node from fast-xml-parser is in '#text'.
  if (node['#text'] !== undefined) parts.push(String(node['#text']));

  for (const [key, value] of Object.entries(node)) {
    if (key === '#text' || key.startsWith('@_')) continue;
    if (key === 'DESTINATION') continue; // structural anchor, no visible text
    if (key === 'TAB' || key === 'LINEBRK' || key === 'BR') {
      const repeats = Array.isArray(value) ? value.length : 1;
      parts.push(' '.repeat(repeats));
      continue;
    }
    const children = asArray(value);
    for (const c of children) parts.push(extractText(c));
  }

  return parts.join('').replace(/[ \t]+/g, ' ').trim();
}

/**
 * Capture only LINKs whose destination-name looks like an RCNY canonical
 * address (T<digits>C<digits>(_<section>)?). Other LINKs (Inter Infobase
 * Jump pointing into the Admin Code etc.) are left to the regex pass —
 * they will produce external_curie like nyc-admin-code:24-603.
 */
function collectLinks(node, accum) {
  if (!node || typeof node !== 'object') return;
  for (const [key, value] of Object.entries(node)) {
    if (key.startsWith('@_')) continue;
    if (key === 'LINK') {
      for (const link of asArray(value)) {
        const dest = link['@_destination-name'];
        if (dest && /^T\d+(?:C\d+)?(?:_[\w.-]+)?$/.test(dest)) {
          accum.push({ targetDestination: dest, rawText: extractText(link) });
        }
      }
    } else if (typeof value === 'object') {
      for (const c of asArray(value)) collectLinks(c, accum);
    }
  }
}

function findFirstDescendant(node, tag) {
  if (!node || typeof node !== 'object') return null;
  if (node[tag]) {
    const v = asArray(node[tag])[0];
    if (v) return v;
  }
  for (const [key, value] of Object.entries(node)) {
    if (key.startsWith('@_') || key === '#text') continue;
    for (const child of asArray(value)) {
      const hit = findFirstDescendant(child, tag);
      if (hit) return hit;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Heading parsing
// ---------------------------------------------------------------------------

/**
 * Split a heading like "§ 41-01 Scope and Application." or
 * "Chapter 41: Community Right-to-Know Regulations" into number + label.
 */
function splitHeading(unitType, headingText) {
  const text = (headingText || '').trim();
  if (!text) return { number: null, label: null };

  if (unitType === 'section' || unitType === 'subsection') {
    // "§ 41-01 Scope and Application." or "§ 41-01. Scope..."
    const m = text.match(/^§\s*([\w.-]+?)\s*[.:]?\s+(.*)$/);
    if (m) return { number: m[1], label: trimTrailingPunct(m[2]) };
    const onlyNum = text.match(/^§\s*([\w.-]+)\s*$/);
    if (onlyNum) return { number: onlyNum[1], label: null };
  }

  // "Chapter 41: Community Right-to-Know Regulations"
  // "Title 15: Department of Environmental Protection"
  // "Part A: Foo"  /  "Subpart 1: Bar"
  const m = text.match(/^(?:[A-Za-z][\w-]*\s+)?([\w.-]+?)\s*[:.\-—]\s+(.*)$/);
  if (m) return { number: m[1], label: trimTrailingPunct(m[2]) };

  return { number: null, label: trimTrailingPunct(text) };
}

function trimTrailingPunct(s) {
  return (s || '').replace(/[\s.:;,]+$/, '').trim() || null;
}

// ---------------------------------------------------------------------------
// CURIE / citation derivation
// ---------------------------------------------------------------------------

/**
 * Derive the case-side join key from canonical address.
 *
 * Section forms (must match what extractLegislativeCitationsFromCaselaw.js emits):
 *   T15C041_41-01     → rcny:15-41-01      (standard section)
 *   T24C019A_19A-01   → rcny:24-19A-01     (lettered chapter)
 *   T15C019.1_19.1-01 → rcny:15-19.1-01    (decimal chapter)
 *   T62AppA_Preface   → rcny:62-AppA-Preface (chapter-level appendix's section)
 *   T28C061AppA       → rcny:28-61-AppA    (chapter's appendix as a section)
 *
 * Higher-level forms (internal use, namespaced t:/c: so they can't collide
 * with case-side section CURIEs):
 *   T15           → rcny:t:15
 *   T15C041       → rcny:c:15-41
 *   T62AppA       → rcny:c:62-AppA
 */
function deriveCanonicalId(unitType, canonicalAddress, number) {
  if (!canonicalAddress) return null;

  // Standard section: T<title>C<chap>_<sec>; chap can be \w+ (e.g., 019A) or
  // contain a dot (e.g., 019.1). We drop the chapter from the CURIE since
  // case-side cites are <title>-<sec> with no chapter component.
  const sectionMatch = canonicalAddress.match(/^T(\d+)C[A-Za-z0-9.]+_(.+)$/);
  if (sectionMatch) return `rcny:${parseInt(sectionMatch[1], 10)}-${sectionMatch[2]}`;

  // Section in a chapter-level appendix: T<title>App<X>_<sec> or T<title>App<X>-<sec>
  const appSectionMatch = canonicalAddress.match(/^T(\d+)App([A-Z]+)[_-](.+)$/);
  if (appSectionMatch) {
    return `rcny:${parseInt(appSectionMatch[1], 10)}-App${appSectionMatch[2]}-${appSectionMatch[3]}`;
  }

  // Chapter's appendix (acts as a section in our scheme): T<title>C<chap>App<X>
  const chapAppMatch = canonicalAddress.match(/^T(\d+)C[A-Za-z0-9.]+App([A-Z]+)$/);
  if (chapAppMatch) return `rcny:${parseInt(chapAppMatch[1], 10)}-App${chapAppMatch[2]}`;

  // Title 24 Health Code: T24HC_1.01 → rcny:24-1.01
  const hcMatch = canonicalAddress.match(/^T(\d+)HC_(.+)$/);
  if (hcMatch) return `rcny:${parseInt(hcMatch[1], 10)}-${hcMatch[2]}`;

  // Lettered title (e.g., Title 38-a): T38-aC001_1-01 → rcny:38a-1-01
  const dashTitleMatch = canonicalAddress.match(/^T(\d+)-([a-z])C[A-Za-z0-9.]+_(.+)$/);
  if (dashTitleMatch) {
    return `rcny:${parseInt(dashTitleMatch[1], 10)}${dashTitleMatch[2]}-${dashTitleMatch[3]}`;
  }

  // Compact section form without underscore: T50C002-01 → rcny:50-002-01
  // (rare; preserves chapter-section ordering since there's no other separator).
  const compactSectionMatch = canonicalAddress.match(/^T(\d+)C([A-Za-z0-9.]+)-(.+)$/);
  if (compactSectionMatch) {
    return `rcny:${parseInt(compactSectionMatch[1], 10)}-${compactSectionMatch[2]}-${compactSectionMatch[3]}`;
  }

  // Higher-level wrappers
  const chapterMatch = canonicalAddress.match(/^T(\d+)C([A-Za-z0-9.]+)$/);
  if (chapterMatch) return `rcny:c:${parseInt(chapterMatch[1], 10)}-${chapterMatch[2]}`;

  const appChapterMatch = canonicalAddress.match(/^T(\d+)App([A-Z]+)$/);
  if (appChapterMatch) return `rcny:c:${parseInt(appChapterMatch[1], 10)}-App${appChapterMatch[2]}`;

  const titleMatch = canonicalAddress.match(/^T(\d+)$/);
  if (titleMatch) return `rcny:t:${parseInt(titleMatch[1], 10)}`;

  return null;
}

function formatCitation(canonicalId, unitType, number, label) {
  // Section: "15 RCNY § 41-01 Scope and Application"
  if (canonicalId && canonicalId.startsWith('rcny:') && !canonicalId.startsWith('rcny:t:') && !canonicalId.startsWith('rcny:c:')) {
    const m = canonicalId.match(/^rcny:(\d+)-(.+)$/);
    if (m) return `${m[1]} RCNY § ${m[2]}${label ? ' — ' + label : ''}`;
  }
  if (unitType === 'chapter' && number) return `Chapter ${number}${label ? ': ' + label : ''}`;
  if (unitType === 'title' && number) return `Title ${number}${label ? ': ' + label : ''}`;
  if (number) return `${capitalize(unitType)} ${number}${label ? ': ' + label : ''}`;
  return label || null;
}

function buildSortKey(unitType, number) {
  if (!number) return null;
  // "41-05" → "000041-000005"; "9.1.2" → "000009.000001.000002"
  return number.toString().replace(/(\d+)/g, (s) => s.padStart(6, '0'));
}

function capitalize(s) {
  return s ? s[0].toUpperCase() + s.slice(1) : s;
}

// ---------------------------------------------------------------------------
// External citation regex (lifted from extractLegislativeCitationsFromCaselaw.js)
// ---------------------------------------------------------------------------

const EXTERNAL_CITATION_PATTERNS = [
  {
    // 15 RCNY § 41-05 (canonical form)
    pattern: /\b(\d+)\s*RCNY\s*(?:§|section|sec\.?)?\s*(\d+(?:-\d+)*(?:\.\d+)*[a-z]?)\b/gi,
    targetKind: 'reg_section',
    curie: (m) => `rcny:${m[1]}-${m[2]}`,
  },
  {
    // 19 NYCRR § 1200.1 / 9 NYCRR Part 6654.20
    pattern: /\b(\d+)\s*NYCRR\s*(?:Part|§|section|sec\.?)?\s*(\d+(?:[-.]\d+)*[a-z]?)\b/gi,
    targetKind: 'reg_section',
    curie: (m) => `nycrr:${m[1]}-${m[2]}`,
  },
  {
    // NYC Admin Code § 24-603 / Admin. Code § 24-603
    pattern: /\b(?:NYC\s+)?Admin(?:istrative)?\.?\s+Code\s*(?:§|section|sec\.?)\s*(\d+(?:-[A-Za-z0-9.]+)*)\b/gi,
    targetKind: 'statute_section',
    curie: (m) => `nyc-admin-code:${m[1]}`,
  },
  {
    // Charter § 1234 / NYC Charter § 1234
    pattern: /\b(?:NYC\s+)?Charter\s*(?:§|section|sec\.?)\s*(\d+(?:-[a-z])?(?:\.\d+)*)/gi,
    targetKind: 'statute_section',
    curie: (m) => `nyc-charter:${m[1]}`,
  },
  {
    // 42 U.S.C. § 1983
    pattern: /\b(\d+)\s*U\.?S\.?C\.?\s*(?:§|sec\.?)\s*(\d+[\w.-]*)\b/gi,
    targetKind: 'statute_section',
    curie: (m) => `usc:${m[1]}-${m[2]}`,
  },
  {
    // 29 C.F.R. § 1910.95
    pattern: /\b(\d+)\s*C\.?F\.?R\.?\s*(?:§|sec\.?)\s*(\d+[\w.-]*)\b/gi,
    targetKind: 'reg_section',
    curie: (m) => `cfr:${m[1]}-${m[2]}`,
  },
];

function extractExternalCitations(text) {
  const out = [];
  for (const { pattern, targetKind, curie } of EXTERNAL_CITATION_PATTERNS) {
    const re = new RegExp(pattern.source, pattern.flags);
    let m;
    while ((m = re.exec(text)) !== null) {
      out.push({
        raw: m[0],
        targetKind,
        curie: curie(m),
        context: contextAround(text, m.index, m[0].length),
      });
    }
  }
  return out;
}

function contextAround(text, idx, len) {
  const start = Math.max(0, idx - 80);
  const end = Math.min(text.length, idx + len + 80);
  let ctx = text.slice(start, end);
  if (start > 0) ctx = '…' + ctx;
  if (end < text.length) ctx = ctx + '…';
  return ctx;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function asArray(value) {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

async function writeJsonLine(fileHandle, obj) {
  await fileHandle.write(JSON.stringify(obj) + '\n');
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const stagingDir = args.find((a) => a.startsWith('--staging='))?.split('=')[1] || './data/staging/rcny';
  const ndjsonOutput = args.find((a) => a.startsWith('--out='))?.split('=')[1] || './data/processed/rcny-statutes.ndjson';
  const verbose = args.includes('--verbose');

  parseRcnyToNdjson(stagingDir, ndjsonOutput, { verbose })
    .then((counts) => {
      console.log('Done.');
      console.log(JSON.stringify(counts, null, 2));
    })
    .catch((err) => {
      console.error('FAILED:', err);
      process.exit(1);
    });
}
