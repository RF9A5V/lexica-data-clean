// Phase 14 — appellate_history_resolution_queue.
// Spec §D.2 row 14:
//   Inputs:  Source rows + `_merge_remap_cases` + `_merge_remap_opinions`
//   Outputs: Merged
//   Notes:   "Standard FK remap."
//
// citing_case_id remaps via _merge_remap_cases (INNER JOIN — drop if
// unresolvable; log count). citing_opinion_id remaps via
// _merge_remap_opinions when non-null.
//
// Two partial-unique idempotency indexes (uniq_ahrq_idempotency on
// citing_opinion_id-keyed; uniq_ahrq_idempotency_legacy on
// citing_case_id-keyed) — within-source uniqueness is preserved by the
// source DBs, and cross-source can't produce dupes after case_id remap
// allocates fresh ids per case.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const QUEUE_COLS_DIRECT = [
  'raw_window', 'trigger_text', 'reason',
  'parser_version', 'status', 'llm_extraction_version',
  'enqueued_at', 'resolved_at', 'notes',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.appellate_history_resolution_queue`
  );
  return Number(c) > 0;
}

export const phase14 = {
  id: 14,
  name: 'appellate_history_resolution_queue',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase14');

    if (await alreadyRan(targetClient)) {
      log.info('appellate_history_resolution_queue already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['cases', 'opinions']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 14 requires _merge_remap_${need}`);
      }
    }

    await targetClient.query(`
      CREATE TEMP TABLE _stage_ahrq (
        source_ref               text NOT NULL,
        old_citing_case_id       bigint NOT NULL,
        raw_window               text NOT NULL,
        trigger_text             text,
        reason                   text NOT NULL,
        parser_version           integer NOT NULL,
        status                   text NOT NULL,
        llm_extraction_version   integer,
        enqueued_at              timestamp with time zone,
        resolved_at              timestamp with time zone,
        notes                    text,
        old_citing_opinion_id    bigint
      )
    `);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const t0 = Date.now();
      const select = `(
        SELECT $$${ref}$$::text AS source_ref,
               citing_case_id    AS old_citing_case_id,
               ${QUEUE_COLS_DIRECT.join(', ')},
               citing_opinion_id AS old_citing_opinion_id
          FROM appellate_history_resolution_queue
      )`;
      const dest = `_stage_ahrq (source_ref, old_citing_case_id, ${QUEUE_COLS_DIRECT.join(', ')}, old_citing_opinion_id)`;
      const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
      log.info(`copied ${n} queue rows from ${ref} (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
      staged += n;
    }
    log.info(`total queue rows staged: ${staged}`);

    const { rowCount } = await targetClient.query(`
      INSERT INTO public.appellate_history_resolution_queue
        (citing_case_id, ${QUEUE_COLS_DIRECT.join(', ')}, citing_opinion_id)
      SELECT rc.new_id,
             ${QUEUE_COLS_DIRECT.map((c) => 's.' + c).join(', ')},
             ro.new_id
        FROM _stage_ahrq s
        JOIN _merge_remap_cases rc
          ON rc.source_ref = s.source_ref AND rc.old_id = s.old_citing_case_id
        LEFT JOIN _merge_remap_opinions ro
          ON ro.source_ref = s.source_ref AND ro.old_id = s.old_citing_opinion_id
    `);
    const dropped = staged - rowCount;
    if (dropped > 0) {
      log.warn(`dropped ${dropped} queue rows whose citing_case_id could not be remapped`);
    }
    log.info(`inserted ${rowCount} queue rows`);

    return { source_rows: staged, inserted: rowCount, dropped };
  },
};
