// Phase 7 — opinion_footnotes + opinion_holdings + opinion_keywords
//          + opinion_negative_treatments + opinion_citations.
// Spec §D.2 row 7.
//
// All five tables key on `opinion_curie text` (post-D-6 prereq DDL applied
// 2026-05-25). No opinion-id remap needed — opinions.curie is globally
// unique cross-source and NOT NULL UNIQUE in the merged schema. No remap
// table is produced for any of the five.
//
// FK fan-out:
//   * opinion_footnotes — no analysis_run_id; only opinion_curie FK.
//   * opinion_holdings, opinion_negative_treatments, opinion_citations —
//     opinion_curie + analysis_run_id (nullable, remapped via
//     _merge_remap_analysis_runs).
//   * opinion_keywords — opinion_curie + analysis_run_id + keyword_id
//     (REQUIRED, remapped via _merge_remap_keywords).
//
// Volume is small (~119k rows total across all five), so a single
// statement per (table, source) is fine. No GIN-rebuild dance.
//
// Idempotency: phase 7 produces no remap table. Detection: if any of the
// five tables already has rows, the phase has completed (the wrapping
// transaction guarantees all-or-nothing).

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function rowCountIn(client, table) {
  const { rows: [{ c }] } = await client.query(`SELECT count(*)::bigint AS c FROM public.${table}`);
  return Number(c);
}

async function alreadyRan(client) {
  const counts = await Promise.all([
    rowCountIn(client, 'opinion_footnotes'),
    rowCountIn(client, 'opinion_holdings'),
    rowCountIn(client, 'opinion_keywords'),
    rowCountIn(client, 'opinion_negative_treatments'),
    rowCountIn(client, 'opinion_citations'),
  ]);
  return counts.every((n) => n > 0);
}

// ---------------------------------------------------------------------------
// Sub-phase 7a — opinion_footnotes
// (no analysis_run_id; opinion_curie now NOT NULL in source per D-6 DDL)
// ---------------------------------------------------------------------------
const FOOTNOTE_COLS = [
  'opinion_curie', 'footnote_index', 'marker', 'text',
  'body_offset', 'page_index', 'volume_page', 'created_at',
];

async function mergeFootnotes(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_footnotes (
      source_ref      text NOT NULL,
      opinion_curie   text NOT NULL,
      footnote_index  integer NOT NULL,
      marker          text NOT NULL,
      text            text,
      body_offset     integer,
      page_index      integer,
      volume_page     integer,
      created_at      timestamp without time zone
    )
  `);
  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref, ${FOOTNOTE_COLS.join(', ')}
        FROM opinion_footnotes
    )`;
    const n = await copyBetween(
      sourceClients[ref],
      select,
      targetClient,
      `_stage_footnotes (source_ref, ${FOOTNOTE_COLS.join(', ')})`
    );
    total += n;
  }
  log.info(`staged ${total} opinion_footnotes`);

  const { rowCount } = await targetClient.query(`
    INSERT INTO public.opinion_footnotes (${FOOTNOTE_COLS.join(', ')})
    SELECT ${FOOTNOTE_COLS.join(', ')} FROM _stage_footnotes
  `);
  log.info(`inserted ${rowCount} opinion_footnotes`);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Sub-phase 7b — opinion_holdings
// (opinion_curie + analysis_run_id remap)
// ---------------------------------------------------------------------------
const HOLDING_COLS = [
  'issue', 'holding', 'rule', 'reasoning',
  'precedential_value', 'confidence', 'created_at',
  'opinion_curie', 'status',
];

async function mergeHoldings(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_holdings (
      source_ref         text NOT NULL,
      old_analysis_run_id bigint,
      issue              text NOT NULL,
      holding            text NOT NULL,
      rule               text NOT NULL,
      reasoning          text NOT NULL,
      precedential_value text NOT NULL,
      confidence         numeric NOT NULL,
      created_at         timestamp with time zone,
      opinion_curie      text NOT NULL,
      status             text NOT NULL
    )
  `);
  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             analysis_run_id AS old_analysis_run_id,
             ${HOLDING_COLS.join(', ')}
        FROM opinion_holdings
    )`;
    const n = await copyBetween(
      sourceClients[ref],
      select,
      targetClient,
      `_stage_holdings (source_ref, old_analysis_run_id, ${HOLDING_COLS.join(', ')})`
    );
    total += n;
  }
  log.info(`staged ${total} opinion_holdings`);

  const { rowCount } = await targetClient.query(`
    INSERT INTO public.opinion_holdings (analysis_run_id, ${HOLDING_COLS.join(', ')})
    SELECT rar.new_id,
           ${HOLDING_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_holdings s
      LEFT JOIN _merge_remap_analysis_runs rar
        ON rar.source_ref = s.source_ref
       AND rar.old_id     = s.old_analysis_run_id
  `);
  log.info(`inserted ${rowCount} opinion_holdings`);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Sub-phase 7c — opinion_keywords
// (opinion_curie + analysis_run_id + keyword_id; the latter is REQUIRED)
// ---------------------------------------------------------------------------
const KEYWORD_COLS = [
  'relevance_score', 'extraction_method', 'category', 'context',
  'created_at', 'opinion_curie', 'status',
];

async function mergeOpinionKeywords(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_opinion_keywords (
      source_ref          text NOT NULL,
      old_keyword_id      integer NOT NULL,
      old_analysis_run_id bigint,
      relevance_score     numeric,
      extraction_method   text,
      category            text,
      context             jsonb NOT NULL,
      created_at          timestamp with time zone,
      opinion_curie       text NOT NULL,
      status              text NOT NULL
    )
  `);
  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             keyword_id      AS old_keyword_id,
             analysis_run_id AS old_analysis_run_id,
             ${KEYWORD_COLS.join(', ')}
        FROM opinion_keywords
    )`;
    const n = await copyBetween(
      sourceClients[ref],
      select,
      targetClient,
      `_stage_opinion_keywords (source_ref, old_keyword_id, old_analysis_run_id, ${KEYWORD_COLS.join(', ')})`
    );
    total += n;
  }
  log.info(`staged ${total} opinion_keywords`);

  // Validation: every old_keyword_id must resolve in _merge_remap_keywords.
  const { rows: [{ orph }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orph
      FROM _stage_opinion_keywords s
      LEFT JOIN _merge_remap_keywords rk
        ON rk.source_ref = s.source_ref AND rk.old_id = s.old_keyword_id
     WHERE rk.new_id IS NULL
  `);
  if (Number(orph) !== 0) {
    throw new Error(`${orph} opinion_keywords reference unknown source keyword_id`);
  }

  // D-3 collateral: where two source keyword_ids dedup'd to the same
  // canonical keyword AND an opinion was tagged with both variants, the
  // merged (opinion_curie, keyword_id) tuple would violate
  // uniq_opinion_keywords_curie_kw_active. Deduplicate at INSERT time
  // with deterministic winner ordering (highest relevance_score, then
  // oldest created_at, then lowest old keyword_id as final tie-break).
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.opinion_keywords (keyword_id, analysis_run_id, ${KEYWORD_COLS.join(', ')})
    SELECT rk.new_id,
           rar.new_id,
           ${KEYWORD_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_opinion_keywords s
      JOIN _merge_remap_keywords rk
        ON rk.source_ref = s.source_ref
       AND rk.old_id     = s.old_keyword_id
      LEFT JOIN _merge_remap_analysis_runs rar
        ON rar.source_ref = s.source_ref
       AND rar.old_id     = s.old_analysis_run_id
     ORDER BY s.relevance_score DESC NULLS LAST, s.created_at ASC, s.old_keyword_id ASC
    ON CONFLICT (opinion_curie, keyword_id) WHERE status = 'active' DO NOTHING
  `);
  const dropped = total - rowCount;
  if (dropped > 0) {
    log.warn(`opinion_keywords: dropped ${dropped} duplicates after keyword dedup (D-3 collateral)`);
  }
  log.info(`inserted ${rowCount} opinion_keywords`);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Sub-phase 7d — opinion_negative_treatments
// (opinion_curie + analysis_run_id remap)
// ---------------------------------------------------------------------------
const NEG_TREAT_COLS = [
  'opinion_curie', 'tier', 'type', 'case_name', 'citation',
  'basis', 'created_at', 'status',
];

async function mergeNegativeTreatments(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_neg_treatments (
      source_ref          text NOT NULL,
      old_analysis_run_id bigint,
      opinion_curie       text NOT NULL,
      tier                text NOT NULL,
      type                text NOT NULL,
      case_name           text,
      citation            text,
      basis               text NOT NULL,
      created_at          timestamp with time zone,
      status              text NOT NULL
    )
  `);
  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             analysis_run_id AS old_analysis_run_id,
             ${NEG_TREAT_COLS.join(', ')}
        FROM opinion_negative_treatments
    )`;
    const n = await copyBetween(
      sourceClients[ref],
      select,
      targetClient,
      `_stage_neg_treatments (source_ref, old_analysis_run_id, ${NEG_TREAT_COLS.join(', ')})`
    );
    total += n;
  }
  log.info(`staged ${total} opinion_negative_treatments`);

  // uniq_ont_dedup_active is partial-unique on the full content tuple
  // (opinion_curie, tier, type, case_name, citation, basis) WHERE
  // status='active'. Same-content cross-source duplicates would collide;
  // protect via ON CONFLICT DO NOTHING. With 18 source rows total this
  // is precautionary, not load-bearing.
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.opinion_negative_treatments (analysis_run_id, ${NEG_TREAT_COLS.join(', ')})
    SELECT rar.new_id,
           ${NEG_TREAT_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_neg_treatments s
      LEFT JOIN _merge_remap_analysis_runs rar
        ON rar.source_ref = s.source_ref
       AND rar.old_id     = s.old_analysis_run_id
    ON CONFLICT (opinion_curie, tier, type, COALESCE(case_name, ''), COALESCE(citation, ''), basis)
      WHERE status = 'active' DO NOTHING
  `);
  const dropped = total - rowCount;
  if (dropped > 0) {
    log.warn(`opinion_negative_treatments: dropped ${dropped} cross-source duplicates`);
  }
  log.info(`inserted ${rowCount} opinion_negative_treatments`);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Sub-phase 7e — opinion_citations
// (opinion_curie + analysis_run_id remap)
// ---------------------------------------------------------------------------
const OP_CITATION_COLS = [
  'cite_text', 'case_name', 'normalized_citation', 'authority_type',
  'jurisdiction', 'court_level', 'year', 'pincite',
  'citation_context', 'citation_signal', 'precedential_weight',
  'discussion_level', 'legal_proposition', 'confidence',
  'created_at', 'opinion_curie', 'status',
];

async function mergeOpinionCitations(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_opinion_citations (
      source_ref           text NOT NULL,
      old_analysis_run_id  bigint,
      cite_text            text,
      case_name            text,
      normalized_citation  text,
      authority_type       text,
      jurisdiction         text,
      court_level          text,
      year                 integer,
      pincite              text,
      citation_context     text,
      citation_signal      text,
      precedential_weight  text,
      discussion_level     text,
      legal_proposition    text,
      confidence           numeric,
      created_at           timestamp with time zone,
      opinion_curie        text NOT NULL,
      status               text NOT NULL
    )
  `);
  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             analysis_run_id AS old_analysis_run_id,
             ${OP_CITATION_COLS.join(', ')}
        FROM opinion_citations
    )`;
    const n = await copyBetween(
      sourceClients[ref],
      select,
      targetClient,
      `_stage_opinion_citations (source_ref, old_analysis_run_id, ${OP_CITATION_COLS.join(', ')})`
    );
    total += n;
  }
  log.info(`staged ${total} opinion_citations`);

  const { rowCount } = await targetClient.query(`
    INSERT INTO public.opinion_citations (analysis_run_id, ${OP_CITATION_COLS.join(', ')})
    SELECT rar.new_id,
           ${OP_CITATION_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_opinion_citations s
      LEFT JOIN _merge_remap_analysis_runs rar
        ON rar.source_ref = s.source_ref
       AND rar.old_id     = s.old_analysis_run_id
  `);
  log.info(`inserted ${rowCount} opinion_citations`);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Verify — Audit-E-equivalent per-table opinion_curie resolution check
// ---------------------------------------------------------------------------

async function verifyCurieJoins(targetClient) {
  const tables = [
    'opinion_footnotes',
    'opinion_holdings',
    'opinion_keywords',
    'opinion_negative_treatments',
    'opinion_citations',
  ];
  for (const t of tables) {
    const { rows: [{ orph }] } = await targetClient.query(`
      SELECT count(*)::bigint AS orph
        FROM public.${t} c
        LEFT JOIN public.opinions o ON o.curie = c.opinion_curie
       WHERE o.curie IS NULL
    `);
    if (Number(orph) !== 0) {
      throw new Error(`${orph} rows in ${t} reference an opinion_curie not present in opinions`);
    }
  }
}

export const phase07 = {
  id: 7,
  name: 'opinion_* children (footnotes/holdings/keywords/negative_treatments/citations)',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase07');

    if (await alreadyRan(targetClient)) {
      log.info('all five opinion_* tables already populated — skipping.');
      return { skipped: true };
    }

    for (const need of ['opinions', 'keywords', 'analysis_runs']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 7 requires _merge_remap_${need} (run earlier phase first)`);
      }
    }

    const t0 = Date.now();

    log.info('▸ 7a: opinion_footnotes');
    const fn = await mergeFootnotes(targetClient, sourceClients, log);

    log.info('▸ 7b: opinion_holdings');
    const hd = await mergeHoldings(targetClient, sourceClients, log);

    log.info('▸ 7c: opinion_keywords');
    const kw = await mergeOpinionKeywords(targetClient, sourceClients, log);

    log.info('▸ 7d: opinion_negative_treatments');
    const nt = await mergeNegativeTreatments(targetClient, sourceClients, log);

    log.info('▸ 7e: opinion_citations');
    const oc = await mergeOpinionCitations(targetClient, sourceClients, log);

    await verifyCurieJoins(targetClient);

    log.info(`phase 7 complete (${((Date.now() - t0) / 1000).toFixed(1)}s)`);

    return {
      opinion_footnotes: fn,
      opinion_holdings: hd,
      opinion_keywords: kw,
      opinion_negative_treatments: nt,
      opinion_citations: oc,
    };
  },
};
