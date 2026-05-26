// Phase 8 — case_captions.
// Spec §D.2 row 8:
//   Inputs:  Source rows + `_merge_remap_cases`
//   Outputs: Merged
//   Notes:   "Standard FK remap."
//
// No remap table needed (no other table FKs case_captions). Idempotency
// via row-count sentinel (case_captions is populated → phase has run).
//
// Within-source `uniq_case_captions_idx (case_id, caption_index)` is
// enforced by source DB. Cross-source can't collide since case_id is
// allocated fresh per cases row in phase 3, so no ON CONFLICT needed.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const CAPTION_COLS = [
  'caption_index', 'name', 'name_abbreviation', 'docket_number', 'created_at',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.case_captions`
  );
  return Number(c) > 0;
}

async function stage(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_case_captions (
      source_ref         text NOT NULL,
      old_case_id        bigint NOT NULL,
      caption_index      integer NOT NULL,
      name               text NOT NULL,
      name_abbreviation  text,
      docket_number      text,
      created_at         timestamp without time zone
    )
  `);
  await targetClient.query(`CREATE INDEX ON _stage_case_captions (source_ref, old_case_id)`);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const t0 = Date.now();
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             case_id AS old_case_id,
             ${CAPTION_COLS.join(', ')}
        FROM case_captions
    )`;
    const dest = `_stage_case_captions (source_ref, old_case_id, ${CAPTION_COLS.join(', ')})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} case_captions from ${ref} (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
    total += n;
  }
  return total;
}

async function insertCaptions(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.case_captions (case_id, ${CAPTION_COLS.join(', ')})
    SELECT rc.new_id,
           ${CAPTION_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_case_captions s
      JOIN _merge_remap_cases rc
        ON rc.source_ref = s.source_ref
       AND rc.old_id     = s.old_case_id
  `);
  return rowCount;
}

export const phase08 = {
  id: 8,
  name: 'case_captions',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase08');

    if (await alreadyRan(targetClient)) {
      log.info('case_captions already populated — skipping.');
      return { skipped: true };
    }

    if (!(await remapExists(targetClient, 'cases'))) {
      throw new Error('phase 3 (cases) must run before phase 8');
    }

    const staged = await stage(targetClient, sourceClients, log);
    log.info(`total case_captions staged: ${staged}`);

    const inserted = await insertCaptions(targetClient);
    log.info(`inserted ${inserted} case_captions`);

    if (inserted !== staged) {
      throw new Error(`case_captions count mismatch: staged ${staged}, inserted ${inserted}`);
    }

    return { source_rows: staged, inserted };
  },
};
