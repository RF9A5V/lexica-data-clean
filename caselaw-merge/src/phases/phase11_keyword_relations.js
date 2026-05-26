// Phase 11 — keyword_relations.
// Spec §D.2 row 11:
//   Inputs:  Source rows + `_merge_remap_doctrine_anchors` + `_merge_remap_keywords`
//   Outputs: Merged
//   Notes:   "Standard FK remap."
//
// anchor_id remaps via _merge_remap_doctrine_anchors (INNER JOIN —
// rows referencing phase-9-skipped anchors are dropped).
// related_keyword_id remaps via _merge_remap_keywords.
//
// uniq_keyword_relations_anchor_keyword_type (anchor_id, related_keyword_id,
// relation_type) may surface dedup collisions when two source rows
// collapse to the same triple post-remap. ON CONFLICT DO NOTHING handles
// that.
//
// As of 2026-05-25, this table is empty across all three source DBs.

import { remapExists } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const REL_COLS = [
  'relation_type', 'provenance', 'confidence_tier',
  'methodology_version', 'created_at', 'updated_at',
];

async function alreadyRan(targetClient) {
  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM public.keyword_relations`
  );
  return Number(c) > 0;
}

export const phase11 = {
  id: 11,
  name: 'keyword_relations',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase11');

    if (await alreadyRan(targetClient)) {
      log.info('keyword_relations already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['keywords', 'doctrine_anchors']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 11 requires _merge_remap_${need}`);
      }
    }

    await targetClient.query(`
      CREATE TEMP TABLE _stage_kr (
        source_ref             text NOT NULL,
        old_anchor_id          integer NOT NULL,
        old_related_keyword_id integer NOT NULL,
        relation_type          text NOT NULL,
        provenance             jsonb NOT NULL,
        confidence_tier        text NOT NULL,
        methodology_version    integer NOT NULL,
        created_at             timestamp with time zone,
        updated_at             timestamp with time zone
      )
    `);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const select = `(
        SELECT $$${ref}$$::text AS source_ref,
               anchor_id          AS old_anchor_id,
               related_keyword_id AS old_related_keyword_id,
               ${REL_COLS.join(', ')}
          FROM keyword_relations
      )`;
      const dest = `_stage_kr (source_ref, old_anchor_id, old_related_keyword_id, ${REL_COLS.join(', ')})`;
      const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
      log.info(`copied ${n} keyword_relations from ${ref}`);
      staged += n;
    }
    log.info(`total staged: ${staged}`);

    if (staged === 0) {
      log.info('source tables empty — nothing to merge');
      return { source_rows: 0, inserted: 0 };
    }

    const { rowCount: inserted } = await targetClient.query(`
      INSERT INTO public.keyword_relations
        (anchor_id, related_keyword_id, ${REL_COLS.join(', ')})
      SELECT ra.new_id, rk.new_id,
             ${REL_COLS.map((c) => 's.' + c).join(', ')}
        FROM _stage_kr s
        JOIN _merge_remap_doctrine_anchors ra
          ON ra.source_ref = s.source_ref AND ra.old_id = s.old_anchor_id
        JOIN _merge_remap_keywords rk
          ON rk.source_ref = s.source_ref AND rk.old_id = s.old_related_keyword_id
       ORDER BY ra.new_id, rk.new_id, s.relation_type
      ON CONFLICT (anchor_id, related_keyword_id, relation_type) DO NOTHING
    `);
    const dropped = staged - inserted;
    if (dropped > 0) {
      log.warn(`dropped ${dropped} keyword_relations (anchor skipped or post-remap duplicate)`);
    }
    log.info(`inserted ${inserted} keyword_relations`);

    return {
      source_rows: staged,
      inserted,
      dropped,
    };
  },
};
