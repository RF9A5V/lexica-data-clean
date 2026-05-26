import path from 'path';
import fs from 'fs/promises';
import { fileURLToPath } from 'url';
import { appendLine, ensureDir, getIn, padSort, readJson } from '../lib/utils.js';

// Map the NYS API docType → our unit_type enum.
const DOC_TYPE_MAP = {
  CHAPTER: 'chapter',
  TITLE: 'title',
  SUBTITLE: 'subtitle',
  ARTICLE: 'article',
  SUBARTICLE: 'subarticle',
  PART: 'part',
  SUBPART: 'subpart',
  SECTION: 'section',
  RULE: 'rule',
  INDEX: 'other',
  PREAMBLE: 'other',
  PREAMBULATORY_CL: 'other',
  MISC: 'other',
  RESOLUTION: 'other',
  JOINT_RESOLUTION: 'other',
  CONTENTS: 'other',
};

// Used for sort_key rank within a law tree.
const RANK = ['LAW', 'CHAPTER', 'TITLE', 'SUBTITLE', 'ARTICLE', 'SUBARTICLE', 'PART', 'SUBPART', 'SECTION', 'RULE'];
function rankOf(docType) {
  const i = RANK.indexOf(String(docType || '').toUpperCase());
  return (i >= 0 ? i : 99).toString().padStart(2, '0');
}

let aliasConfig = null;
async function getAliasConfig() {
  if (aliasConfig) return aliasConfig;
  const here = path.dirname(fileURLToPath(import.meta.url));
  const cfgPath = path.resolve(here, '../../configs/law_aliases.json');
  const raw = await readJson(cfgPath);
  // Strip _comment if present.
  delete raw._comment;
  aliasConfig = raw;
  return aliasConfig;
}

function unitId(lawId, locationId) {
  return `nys:${lawId}:${locationId}`.toLowerCase();
}

function curiePart(num) {
  return String(num).trim().toLowerCase();
}

// Build all CURIE forms (primary + aliases) for a section/article.
//   shape = 'section' or 'article'
//   subpath = the section number ('120.05') or article number ('78')
function buildCuries(lawId, lawCfg, shape, subpath) {
  if (!lawCfg) return { primary: null, aliases: [] };
  const part = curiePart(subpath);
  const make = (kebab) => (shape === 'article' ? `nys:${kebab}-article-${part}` : `nys:${kebab}-${part}`);
  const primary = make(lawCfg.primary);
  const aliases = (lawCfg.aliases || []).map(make);
  return { primary, aliases };
}

// Walk a single law's tree (response of /laws/{lawId}?full=true), emitting NDJSON records.
async function emitLawTree(outFile, treeJson, lawCfg) {
  const root = getIn(treeJson, ['result', 'documents'], null);
  const lawInfo = getIn(treeJson, ['result', 'info'], {});
  const lawId = lawInfo.lawId || getIn(treeJson, ['result', 'documents', 'lawId'], null);
  if (!root || !lawId) {
    console.warn('  [skip] tree has no documents/lawId');
    return { units: 0, aliases: 0 };
  }

  const counts = { units: 0, aliases: 0 };

  // Emit synthetic root unit for the whole law (e.g. id "nys:pen:law").
  const lawRootId = `nys:${lawId}:law`.toLowerCase();
  const rootRec = {
    type: 'unit',
    id: lawRootId,
    unit_type: 'title',
    number: lawId,
    label: lawInfo.name || lawId,
    parent_id: null,
    sort_key: padSort([rankOf('LAW'), '0']),
    citation: lawInfo.name ? `${lawInfo.name} Law` : lawId,
    canonical_id: null,
    law_id: lawId,
    law_type: lawInfo.lawType || null,
    active_date: getIn(treeJson, ['result', 'lawVersion', 'activeDate'], null),
  };
  await appendLine(outFile, JSON.stringify(rootRec));
  counts.units++;

  // Recursive walk. Each node's parent is either lawRootId or another node.
  async function visit(node, parentId) {
    if (!node) return;
    const docType = String(node.docType || '').toUpperCase();
    const locationId = node.locationId || node.docId || node.docLevelId;
    if (!locationId) {
      const kids = getIn(node, ['documents', 'items'], []);
      for (const kid of kids) await visit(kid, parentId);
      return;
    }
    const id = unitId(lawId, locationId);
    const unitTypeRaw = DOC_TYPE_MAP[docType] || 'other';

    // Build canonical CURIE + aliases for SECTION and ARTICLE.
    let canonicalId = null;
    const aliasesToEmit = [];

    if (docType === 'SECTION') {
      const secNum = (node.lawSection || node.docLevelId || locationId || '').toString().trim();
      if (secNum && lawCfg) {
        const { primary, aliases } = buildCuries(lawId, lawCfg, 'section', secNum);
        canonicalId = primary;
        aliasesToEmit.push(...aliases);
      }
    } else if (docType === 'ARTICLE') {
      const artNum = (node.docLevelId || locationId || '').toString().replace(/^A/i, '').trim();
      if (artNum && /^[\w.-]+$/.test(artNum) && lawCfg) {
        const { primary, aliases } = buildCuries(lawId, lawCfg, 'article', artNum);
        canonicalId = primary;
        aliasesToEmit.push(...aliases);
      }
    }

    // Build a human citation string. Add " Law" only if the API name doesn't already
    // end with "Law", "Act", "Code", or "Constitution" (avoids "X Law Law").
    const baseName = (lawInfo.name || lawId).trim();
    const needsLawSuffix = !/\b(Law|Act|Code|Constitution|Rules)$/i.test(baseName);
    const lawNoun = needsLawSuffix ? `${baseName} Law` : baseName;
    let citation;
    if (docType === 'SECTION') {
      const secNum = (node.lawSection || node.docLevelId || locationId).toString().trim();
      citation = `${lawNoun} § ${secNum}`;
    } else if (docType === 'ARTICLE') {
      const artNum = (node.docLevelId || locationId).toString().replace(/^A/i, '').trim();
      citation = `${lawNoun} Art. ${artNum}`;
    } else {
      citation = `${lawNoun} ${docType.toLowerCase()} ${locationId}`;
    }

    const text = typeof node.text === 'string' ? node.text : null;
    const publishedDates = Array.isArray(node.publishedDates) ? node.publishedDates : null;
    const activeDate = node.activeDate || null;

    const rec = {
      type: 'unit',
      id,
      unit_type: unitTypeRaw,
      number: node.docLevelId || node.lawSection || locationId,
      label: node.title || node.docLevelId || locationId,
      parent_id: parentId,
      sort_key: padSort([rankOf(docType), node.sequenceNo || locationId]),
      citation,
      canonical_id: canonicalId,
      law_id: lawId,
      law_type: lawInfo.lawType || null,
      published_dates: publishedDates,
      active_date: activeDate,
      text_plain: text,
      effective_start: activeDate || '1900-01-01',
    };
    await appendLine(outFile, JSON.stringify(rec));
    counts.units++;

    for (const alias of aliasesToEmit) {
      await appendLine(outFile, JSON.stringify({ type: 'alias', alias, unit_id: id }));
      counts.aliases++;
    }

    // Recurse.
    const kids = getIn(node, ['documents', 'items'], []);
    for (const kid of kids) await visit(kid, id);
  }

  // The root response is a node itself representing the law. Its children are the real top-level docs.
  const childItems = getIn(root, ['documents', 'items'], []);
  for (const kid of childItems) await visit(kid, lawRootId);

  return counts;
}

export async function transformAllCachedLaws({ cacheDir, outFile, only = null }) {
  const cfg = await getAliasConfig();
  const lawsDir = path.join(cacheDir, 'laws');
  let entries;
  try {
    entries = await fs.readdir(lawsDir);
  } catch (e) {
    console.error(`No cached laws at ${lawsDir}`);
    return;
  }
  // Truncate / start fresh.
  await ensureDir(path.dirname(outFile));
  await fs.writeFile(outFile, '');

  const totals = { laws: 0, units: 0, aliases: 0, repealed: 0 };

  for (const ent of entries) {
    if (!ent.endsWith('.json')) continue;
    const lawId = ent.replace(/\.json$/, '');
    if (only && !only.includes(lawId)) continue;
    const lawCfg = cfg[lawId];
    if (!lawCfg) {
      console.warn(`  ${lawId}: no alias config — units will load without canonical_id`);
    }
    const tree = await readJson(path.join(lawsDir, ent));
    const c = await emitLawTree(outFile, tree, lawCfg);
    totals.laws++;
    totals.units += c.units;
    totals.aliases += c.aliases;
    console.log(`  ${lawId}: ${c.units} units, ${c.aliases} aliases`);
  }

  // Now repealed sections — emit as standalone units with is_active=false.
  // Skip those whose locationId already appeared in the tree; otherwise
  // append a new repealed unit.
  const repealedDir = path.join(cacheDir, 'repealed');
  let lawDirs = [];
  try { lawDirs = await fs.readdir(repealedDir); } catch {}
  for (const lawId of lawDirs) {
    if (only && !only.includes(lawId)) continue;
    const lawCfg = cfg[lawId];
    const files = await fs.readdir(path.join(repealedDir, lawId));
    for (const f of files) {
      if (!f.endsWith('.json')) continue;
      const json = await readJson(path.join(repealedDir, lawId, f));
      const meta = json._meta || {};
      const result = json.result || {};
      const locationId = result.locationId || meta.locationId;
      const docType = String(result.docType || 'SECTION').toUpperCase();
      if (!locationId) continue;
      const id = `${unitId(lawId, locationId)}::repealed:${meta.publishedDate || result.activeDate || 'unk'}`;

      let canonicalId = null;
      const aliasesToEmit = [];
      if (docType === 'SECTION' && lawCfg) {
        const secNum = (result.lawSection || result.docLevelId || locationId).toString().trim();
        const { primary, aliases } = buildCuries(lawId, lawCfg, 'section', secNum);
        canonicalId = primary;
        aliasesToEmit.push(...aliases);
      }

      // For repealed sections: if repealedDate < publishedDate (a
      // retroactive repeal — the text snapshot we have is from after the
      // repeal), drop effective_end to avoid violating
      // CHECK (effective_end > effective_start). The repealed_date column
      // on units captures the repeal fact regardless.
      const effStart = meta.publishedDate || result.activeDate || '1900-01-01';
      const effEnd =
        meta.repealedDate && meta.repealedDate > effStart ? meta.repealedDate : null;

      const rec = {
        type: 'unit',
        id,
        unit_type: DOC_TYPE_MAP[docType] || 'other',
        number: result.lawSection || result.docLevelId || locationId,
        label: result.title || locationId,
        parent_id: `nys:${lawId}:law`.toLowerCase(),
        sort_key: padSort([rankOf(docType), 'zz', locationId]),
        citation: `${result.lawName || lawId} Law § ${result.lawSection || locationId} (repealed)`,
        canonical_id: canonicalId,
        law_id: lawId,
        is_active: false,
        repealed_date: meta.repealedDate || null,
        active_date: result.activeDate || meta.publishedDate || null,
        published_dates: result.publishedDates || null,
        text_plain: typeof result.text === 'string' ? result.text : null,
        effective_start: effStart,
        effective_end: effEnd,
      };
      await appendLine(outFile, JSON.stringify(rec));
      totals.repealed++;
      for (const alias of aliasesToEmit) {
        await appendLine(outFile, JSON.stringify({ type: 'alias', alias, unit_id: id }));
        totals.aliases++;
      }
    }
  }

  console.log(`\ntotals: ${totals.laws} laws, ${totals.units} units, ${totals.repealed} repealed, ${totals.aliases} aliases`);
}
