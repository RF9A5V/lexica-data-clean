// Phase 12 — appellate_history_case_status.
// Spec §D.2 row 12:
//   Inputs:  Source rows + `_merge_remap_cases` + `_merge_remap_opinions`
//   Outputs: Merged
//   Notes:   "Standard FK remap."
//
// case_id is the PK + FK; remap via _merge_remap_cases. last_opinion_id
// (nullable) remaps via _merge_remap_opinions. Each merged case_id is
// guaranteed unique across sources (phase 3 allocated fresh ids), so no
// ON CONFLICT needed.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const STATUS_COLS = [
  'completeness', 'pending_queue_count', 'last_extracted_at', 'parser_version',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.appellate_history_case_status`
  );
  return Number(c) > 0;
}

export const phase12 = {
  id: 12,
  name: 'appellate_history_case_status',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase12');

    if (await alreadyRan(targetClient)) {
      log.info('appellate_history_case_status already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['cases', 'opinions']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 12 requires _merge_remap_${need}`);
      }
    }

    await targetClient.query(`
      CREATE TEMP TABLE _stage_ahcs (
        source_ref           text NOT NULL,
        old_case_id          bigint NOT NULL,
        completeness         text NOT NULL,
        pending_queue_count  integer NOT NULL,
        last_extracted_at    timestamp with time zone,
        parser_version       integer NOT NULL,
        old_last_opinion_id  bigint
      )
    `);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const select = `(
        SELECT $$${ref}$$::text AS source_ref,
               case_id AS old_case_id,
               ${STATUS_COLS.join(', ')},
               last_opinion_id AS old_last_opinion_id
          FROM appellate_history_case_status
      )`;
      const dest = `_stage_ahcs (source_ref, old_case_id, ${STATUS_COLS.join(', ')}, old_last_opinion_id)`;
      const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
      log.info(`copied ${n} case_status from ${ref}`);
      staged += n;
    }
    log.info(`total staged: ${staged}`);

    const { rowCount } = await targetClient.query(`
      INSERT INTO public.appellate_history_case_status
        (case_id, ${STATUS_COLS.join(', ')}, last_opinion_id)
      SELECT rc.new_id,
             ${STATUS_COLS.map((c) => 's.' + c).join(', ')},
             ro.new_id
        FROM _stage_ahcs s
        JOIN _merge_remap_cases rc
          ON rc.source_ref = s.source_ref AND rc.old_id = s.old_case_id
        LEFT JOIN _merge_remap_opinions ro
          ON ro.source_ref = s.source_ref AND ro.old_id = s.old_last_opinion_id
    `);
    if (rowCount !== staged) {
      throw new Error(`case_status count mismatch: staged ${staged}, inserted ${rowCount}`);
    }
    log.info(`inserted ${rowCount} case_status`);

    return { source_rows: staged, inserted: rowCount };
  },
};
