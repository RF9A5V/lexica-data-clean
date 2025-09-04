import { canonicalCitation, padSort, stableId } from '../lib/utils.js';

// Best-effort mapper for section-like items to unit + text
// Adjust field picks as actual API shape is confirmed.
export function mapSectionToRecords({ sourceId, lawId, section, mapping }) {
  const secId = pick(section, mapping.sectionIdField) || pick(section, mapping.sectionNumField);
  const secNum = pick(section, mapping.sectionNumField) || secId;
  const heading = pick(section, mapping.sectionHeadingField);
  const textHtml = pick(section, mapping.sectionTextHtmlField);
  const textPlain = pick(section, mapping.sectionTextPlainField) || stripHtml(textHtml);
  const effStart = pick(section, mapping.effectiveStartField) || '1900-01-01';
  const effEnd = pick(section, mapping.effectiveEndField) || null;

  const lawUnitId = stableId(sourceId, lawId);
  const sectionUnitId = stableId(sourceId, lawId, 'sec', secId || secNum);

  const records = [];

  // emit law unit (top-level) for hierarchy context; client code can dedupe
  records.push({
    id: lawUnitId,
    type: 'title',
    number: lawId,
    label: lawId,
    parent_id: null,
    sort_key: padSort([lawId]),
    citation: canonicalCitation(lawId),
    canonical_id: `${sourceId}:${lawId}`,
    source_id: sourceId
  });

  // emit section unit (includes text fields so loader creates text_versions)
  records.push({
    id: sectionUnitId,
    type: 'section',
    number: secNum,
    label: heading || secNum,
    parent_id: lawUnitId,
    sort_key: padSort([lawId, 'sec', secNum || secId || '0']),
    citation: canonicalCitation(lawId, secNum),
    canonical_id: `${sourceId}:${lawId}:§${secNum}`,
    source_id: sourceId,
    effective_start: effStart,
    effective_end: effEnd,
    text_html: textHtml || textPlain,
    text: textPlain || stripHtml(textHtml)
  });

  return records;
}

// Map a generic law-tree node (LAW/ARTICLE/PART/SUBPART/SUBARTICLE/SECTION/etc.) to a unit.
// parentId must be provided by the caller based on the traversal path.
export function mapNodeToUnit({ sourceId, lawId, node, parentId, mapping }) {
  const docType = String(node.docType || node.type || '').toUpperCase();
  const unitType = mapDocType(docType);
  const docId = String(node.locationId || node.docId || node.id || node.number || node.lawSection || '').trim();
  const title = node.title || node.heading || node.name || undefined;

  const sectionNum = pick(node, mapping.sectionNumField) || node.lawSection || (docType === 'SECTION' ? docId : undefined);
  // Prefer direct text when present (full=true returns string), fallback to nested text objects
  let textHtml = pick(node, mapping.sectionTextHtmlField) || node.html;
  let textPlain = pick(node, mapping.sectionTextPlainField);
  const t = node.text;
  if (!textHtml && typeof t === 'object' && t) {
    textHtml = t.html || t.body || t.text || undefined;
  }
  if (!textPlain) {
    if (typeof t === 'string') textPlain = t; else if (typeof textHtml === 'string') textPlain = stripHtml(textHtml);
  }
  const effStart = pick(node, mapping.effectiveStartField) || '1900-01-01';
  const effEnd = pick(node, mapping.effectiveEndField) || null;

  const compDocId = (docType === 'SECTION' && sectionNum) ? cleanSectionNum(sectionNum) : docId;

  const id = unitIdFor({ sourceId, lawId, docType, docId: compDocId });
  const citation = docType === 'SECTION' ? canonicalCitation(lawId, cleanSectionNum(sectionNum || docId)) : canonicalCitation(lawId);
  const number = docType === 'SECTION' ? cleanSectionNum(sectionNum || docId) : (node.docLevelId || compDocId || undefined);
  const label = title || number;
  const sort_key = padSort([lawId, rankFor(docType), (node.sequenceNo || compDocId || '0')]);

  const unit = {
    id,
    type: unitType,
    number,
    label,
    parent_id: parentId || stableId(sourceId, lawId),
    sort_key,
    citation,
    canonical_id: docType === 'SECTION' && number ? `${sourceId}:${lawId}:§${number}` : `${sourceId}:${lawId}:${docType.toLowerCase()}:${compDocId}`,
    source_id: sourceId,
  };

  // Only attach text for SECTION nodes to avoid duplicating child content at parent levels
  if (docType === 'SECTION' && (textHtml || textPlain)) {
    unit.effective_start = effStart;
    unit.effective_end = effEnd;
    unit.text = textPlain || stripHtml(textHtml);
  }

  return unit;
}

export function unitIdFor({ sourceId, lawId, docType, docId }) {
  if (!docType || docType.toUpperCase() === 'LAW' || !docId) return stableId(sourceId, lawId);
  return stableId(sourceId, lawId, docType.toLowerCase(), String(docId).toLowerCase());
}

function rankFor(docType) {
  const order = ['LAW','TITLE','SUBTITLE','ARTICLE','SUBARTICLE','PART','SUBPART','CHAPTER','SUBCHAPTER','SECTION'];
  const ix = order.indexOf(String(docType || '').toUpperCase());
  return ix >= 0 ? String(ix + 1).padStart(2,'0') : '99';
}

function mapDocType(docType) {
  const m = {
    LAW: 'title',
    TITLE: 'title',
    SUBTITLE: 'subtitle',
    CHAPTER: 'chapter',
    SUBCHAPTER: 'subchapter',
    ARTICLE: 'article',
    SUBARTICLE: 'subarticle',
    PART: 'part',
    SUBPART: 'subpart',
    SECTION: 'section',
  };
  return m[String(docType || '').toUpperCase()] || 'other';
}

function getNested(obj, path) {
  let v = obj;
  for (const k of path) { if (v && k in v) v = v[k]; else return undefined; }
  return v;
}

function cleanSectionNum(n) {
  if (!n) return n;
  return String(n).replace(/^§\s*/, '').trim();
}

function pick(obj, paths) {
  if (!obj || !Array.isArray(paths)) return undefined;
  for (const p of paths) {
    if (typeof p === 'string' && p in obj) return obj[p];
    if (Array.isArray(p)) {
      let v = obj;
      for (const k of p) {
        if (v && k in v) v = v[k]; else { v = undefined; break; }
      }
      if (v !== undefined) return v;
    }
  }
  return undefined;
}

function stripHtml(html) {
  if (!html) return undefined;
  return String(html).replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}
