// Phase 10 — doctrine_case_classifications.
// Spec §D.2 row 10:
//   Inputs:  Source rows + `_merge_remap_cases` + `_merge_remap_doctrine_anchors`
//   Outputs: Merged
//   Notes:   "Standard FK remap."
//
// Both case_id and anchor_id are remapped. analysis_run_id remaps via
// _merge_remap_analysis_runs when not null.
//
// Phase-9 cascade: classifications whose anchor was dropped by phase 9's
// tier validation will be silently skipped via INNER JOIN. The count is
// logged and verified.
//
// As of 2026-05-25, this table is empty across all three source DBs
// (verified pre-impl). The code is written to be correct when data
// eventually appears.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const CLASS_COLS = [
  'classification', 'classification_justification',
  'citation_count_in_result_set', 'citation_distribution_z_score',
  'methodology_version', 'confidence_tier', 'reviewed_by_user_id',
  'classified_at', 'created_at', 'updated_at', 'status',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.doctrine_case_classifications`
  );
  return Number(c) > 0;
}

export const phase10 = {
  id: 10,
  name: 'doctrine_case_classifications',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase10');

    if (await alreadyRan(targetClient)) {
      log.info('doctrine_case_classifications already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['cases', 'doctrine_anchors', 'analysis_runs']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 10 requires _merge_remap_${need}`);
      }
    }

    await targetClient.query(`
      CREATE TEMP TABLE _stage_dcc (
        source_ref                     text NOT NULL,
        old_anchor_id                  integer NOT NULL,
        old_case_id                    bigint NOT NULL,
        old_analysis_run_id            bigint,
        classification                 text NOT NULL,
        classification_justification   text,
        citation_count_in_result_set   integer,
        citation_distribution_z_score  numeric,
        methodology_version            integer NOT NULL,
        confidence_tier                text NOT NULL,
        reviewed_by_user_id            text,
        classified_at                  timestamp with time zone,
        created_at                     timestamp with time zone,
        updated_at                     timestamp with time zone,
        status                         text NOT NULL
      )
    `);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const select = `(
        SELECT $$${ref}$$::text AS source_ref,
               anchor_id        AS old_anchor_id,
               case_id          AS old_case_id,
               analysis_run_id  AS old_analysis_run_id,
               ${CLASS_COLS.join(', ')}
          FROM doctrine_case_classifications
      )`;
      const dest = `_stage_dcc (source_ref, old_anchor_id, old_case_id, old_analysis_run_id, ${CLASS_COLS.join(', ')})`;
      const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
      log.info(`copied ${n} doctrine_case_classifications from ${ref}`);
      staged += n;
    }
    log.info(`total staged: ${staged}`);

    if (staged === 0) {
      log.info('source tables empty — nothing to merge');
      return { source_rows: 0, inserted: 0, dropped_anchor_unresolved: 0 };
    }

    // INNER JOIN drops rows whose anchor was skipped in phase 9.
    const { rowCount: inserted } = await targetClient.query(`
      INSERT INTO public.doctrine_case_classifications
        (anchor_id, case_id, analysis_run_id, ${CLASS_COLS.join(', ')})
      SELECT ra.new_id, rc.new_id, rar.new_id,
             ${CLASS_COLS.map((c) => 's.' + c).join(', ')}
        FROM _stage_dcc s
        JOIN _merge_remap_doctrine_anchors ra
          ON ra.source_ref = s.source_ref AND ra.old_id = s.old_anchor_id
        JOIN _merge_remap_cases rc
          ON rc.source_ref = s.source_ref AND rc.old_id = s.old_case_id
        LEFT JOIN _merge_remap_analysis_runs rar
          ON rar.source_ref = s.source_ref AND rar.old_id = s.old_analysis_run_id
    `);
    const dropped = staged - inserted;
    if (dropped > 0) {
      log.warn(`dropped ${dropped} classifications whose anchor was skipped by phase-9 tier validation`);
    }
    log.info(`inserted ${inserted} doctrine_case_classifications`);

    return {
      source_rows: staged,
      inserted,
      dropped_anchor_unresolved: dropped,
    };
  },
};
