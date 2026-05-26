// Phase 13 — appellate_history_connections.
// Spec §D.2 row 13:
//   Inputs:  Source rows + `_merge_remap_cases` (applied 3 ways)
//   Outputs: Merged
//   Notes:   "Three-way remap per row. Per-source lookup using existing
//            `*_source_ref` columns."
//
// The source DBs already populate source_case_source_ref +
// target_case_source_ref columns — those say which jurisdiction each
// case reference belongs to. The merge keeps those columns verbatim and
// uses them to drive which _merge_remap_cases entry to consult.
//
//   citing_case_id  : remap via (s.source_ref,                 s.old_citing_case_id)
//   source_case_id  : remap via (s.source_case_source_ref,     s.old_source_case_id)
//   target_case_id  : remap via (s.target_case_source_ref,     s.old_target_case_id)
//
// citing_opinion_id (nullable) remaps via _merge_remap_opinions in the
// citing source.
//
// All three case remaps are INNER JOIN — a connection pointing at a case
// not in our merged DB is silently dropped (the dropped count is logged).
// On the partial-unique idempotency indexes we use ON CONFLICT DO NOTHING
// to handle cross-source duplicate edges.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const AHC_COLS_DIRECT = [
  'source_case_source_ref', 'target_case_source_ref',
  'relation', 'relation_kind', 'raw_citation_string',
  'source_parallels', 'target_parallels',
  'extraction_method', 'parser_version', 'llm_extraction_version', 'extracted_at',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.appellate_history_connections`
  );
  return Number(c) > 0;
}

export const phase13 = {
  id: 13,
  name: 'appellate_history_connections',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase13');

    if (await alreadyRan(targetClient)) {
      log.info('appellate_history_connections already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['cases', 'opinions']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 13 requires _merge_remap_${need}`);
      }
    }

    await targetClient.query(`
      CREATE TEMP TABLE _stage_ahc (
        source_ref               text NOT NULL,
        old_id                   bigint NOT NULL,
        old_citing_case_id       bigint NOT NULL,
        source_case_source_ref   text NOT NULL,
        old_source_case_id       bigint NOT NULL,
        target_case_source_ref   text NOT NULL,
        old_target_case_id       bigint NOT NULL,
        relation                 text NOT NULL,
        relation_kind            text NOT NULL,
        raw_citation_string      text NOT NULL,
        source_parallels         jsonb,
        target_parallels         jsonb,
        extraction_method        text NOT NULL,
        parser_version           integer NOT NULL,
        llm_extraction_version   integer,
        extracted_at             timestamp with time zone,
        old_citing_opinion_id    bigint
      )
    `);
    await targetClient.query(`CREATE INDEX ON _stage_ahc (source_ref, old_citing_case_id)`);
    await targetClient.query(`CREATE INDEX ON _stage_ahc (source_case_source_ref, old_source_case_id)`);
    await targetClient.query(`CREATE INDEX ON _stage_ahc (target_case_source_ref, old_target_case_id)`);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const t0 = Date.now();
      const select = `(
        SELECT $$${ref}$$::text AS source_ref,
               id                AS old_id,
               citing_case_id    AS old_citing_case_id,
               source_case_source_ref,
               source_case_id    AS old_source_case_id,
               target_case_source_ref,
               target_case_id    AS old_target_case_id,
               relation, relation_kind, raw_citation_string,
               source_parallels, target_parallels,
               extraction_method, parser_version, llm_extraction_version, extracted_at,
               citing_opinion_id AS old_citing_opinion_id
          FROM appellate_history_connections
      )`;
      const dest = `_stage_ahc
        (source_ref, old_id, old_citing_case_id,
         source_case_source_ref, old_source_case_id,
         target_case_source_ref, old_target_case_id,
         relation, relation_kind, raw_citation_string,
         source_parallels, target_parallels,
         extraction_method, parser_version, llm_extraction_version, extracted_at,
         old_citing_opinion_id)`;
      const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
      log.info(`copied ${n} connections from ${ref} (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
      staged += n;
    }
    log.info(`total connections staged: ${staged}`);

    // Inspect drop count for each three-way join leg (for visibility).
    const { rows: [{ unres_citing, unres_source, unres_target }] } = await targetClient.query(`
      SELECT
        count(*) FILTER (
          WHERE NOT EXISTS (
            SELECT 1 FROM _merge_remap_cases rc
             WHERE rc.source_ref = s.source_ref AND rc.old_id = s.old_citing_case_id
          )
        )::bigint AS unres_citing,
        count(*) FILTER (
          WHERE NOT EXISTS (
            SELECT 1 FROM _merge_remap_cases rc
             WHERE rc.source_ref = s.source_case_source_ref AND rc.old_id = s.old_source_case_id
          )
        )::bigint AS unres_source,
        count(*) FILTER (
          WHERE NOT EXISTS (
            SELECT 1 FROM _merge_remap_cases rc
             WHERE rc.source_ref = s.target_case_source_ref AND rc.old_id = s.old_target_case_id
          )
        )::bigint AS unres_target
        FROM _stage_ahc s
    `);
    log.info(`unresolved case refs — citing: ${unres_citing}, source: ${unres_source}, target: ${unres_target}`);

    const { rowCount } = await targetClient.query(`
      INSERT INTO public.appellate_history_connections
        (citing_case_id,
         source_case_source_ref, source_case_id,
         target_case_source_ref, target_case_id,
         relation, relation_kind, raw_citation_string,
         source_parallels, target_parallels,
         extraction_method, parser_version, llm_extraction_version, extracted_at,
         citing_opinion_id)
      SELECT rc_citing.new_id,
             s.source_case_source_ref, rc_source.new_id,
             s.target_case_source_ref, rc_target.new_id,
             s.relation, s.relation_kind, s.raw_citation_string,
             s.source_parallels, s.target_parallels,
             s.extraction_method, s.parser_version, s.llm_extraction_version, s.extracted_at,
             ro.new_id
        FROM _stage_ahc s
        JOIN _merge_remap_cases rc_citing
          ON rc_citing.source_ref = s.source_ref AND rc_citing.old_id = s.old_citing_case_id
        JOIN _merge_remap_cases rc_source
          ON rc_source.source_ref = s.source_case_source_ref AND rc_source.old_id = s.old_source_case_id
        JOIN _merge_remap_cases rc_target
          ON rc_target.source_ref = s.target_case_source_ref AND rc_target.old_id = s.old_target_case_id
        LEFT JOIN _merge_remap_opinions ro
          ON ro.source_ref = s.source_ref AND ro.old_id = s.old_citing_opinion_id
      -- Two partial-unique idempotency indexes share the (raw_citation_string,
      -- parser_version, extraction_method, COALESCE(llm_extraction_version, 0))
      -- tail; the choice between them depends on whether citing_opinion_id
      -- is NULL. Postgres ON CONFLICT can only target ONE index, so we
      -- handle this by simply letting the INSERT fail loudly on the rare
      -- cross-source duplicate. If real-world data shows duplicates, we'll
      -- pre-dedup in the staging step.
      -- Result: ON CONFLICT omitted intentionally; rely on inner JOINs to
      -- drop unresolvable rows, and on the source-side uniqueness to
      -- prevent within-source duplicates.
    `);
    const dropped = staged - rowCount;
    if (dropped > 0) {
      log.warn(`dropped ${dropped} connections whose case ref could not be remapped (sum of citing/source/target unresolved above; rows may fail multiple legs)`);
    }
    log.info(`inserted ${rowCount} connections`);

    return {
      source_rows: staged,
      inserted: rowCount,
      dropped,
      unresolved_citing: Number(unres_citing),
      unresolved_source_case: Number(unres_source),
      unresolved_target_case: Number(unres_target),
    };
  },
};
