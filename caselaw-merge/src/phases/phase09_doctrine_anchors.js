// Phase 9 — doctrine_anchors with D-4 collapse.
// Spec §D.2 row 9 + §B D-4.
//
// Algorithm:
//   1. Stage all source anchors with (source_ref, old_id, raw FK refs, content).
//   2. Remap doctrine_keyword_id + field_of_law_keyword_id via _merge_remap_keywords.
//   3. **Pre-flight tier validation**: D-3 dedup can flip a keyword's tier
//      (e.g., a source `tier='doctrine'` keyword may have lost to a `tier=
//      'doctrinal_test'` variant in the merged keywords table). The
//      `doctrine_anchors_validate_keyword_tiers` trigger would RAISE on any
//      such anchor. We pre-filter rows where the canonical doctrine_keyword
//      no longer has tier='doctrine', or the canonical field_of_law_keyword
//      (when non-null) no longer has tier='field_of_law'. The skipped rows
//      are logged with WARN; their source_ref+old_id pairs are surfaced for
//      curator review.
//   4. Collapse surviving rows by (canon_doc_kw, canon_fol_kw):
//      * co_occurrence_count = SUM
//      * confidence_tier     = max-precedence (expert_reviewed > llm_classified > uncurated)
//      * status              = max-precedence (promoted > pending > dismissed)
//      * description         = longest non-NULL across the group (ties → source_ref then old_id)
//      * last_reviewed_at    = MAX
//   5. Allocate fresh ids via nextval; INSERT canonical anchors.
//   6. Populate _merge_remap_doctrine_anchors mapping every surviving
//      source row → its canonical merged id.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const ANCHOR_COLS = [
  'description', 'co_occurrence_count', 'confidence_tier', 'status', 'last_reviewed_at',
];

async function stage(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_doctrine_anchors (
      source_ref                  text NOT NULL,
      old_id                      integer NOT NULL,
      old_doctrine_keyword_id     integer NOT NULL,
      old_field_of_law_keyword_id integer,
      description                 text,
      co_occurrence_count         integer NOT NULL,
      confidence_tier             text NOT NULL,
      status                      text NOT NULL,
      last_reviewed_at            timestamp with time zone,
      PRIMARY KEY (source_ref, old_id)
    )
  `);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             id                      AS old_id,
             doctrine_keyword_id     AS old_doctrine_keyword_id,
             field_of_law_keyword_id AS old_field_of_law_keyword_id,
             ${ANCHOR_COLS.join(', ')}
        FROM doctrine_anchors
    )`;
    const dest = `_stage_doctrine_anchors
      (source_ref, old_id, old_doctrine_keyword_id, old_field_of_law_keyword_id,
       ${ANCHOR_COLS.join(', ')})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} doctrine_anchors from ${ref}`);
    total += n;
  }
  return total;
}

async function remapAndValidate(targetClient, log) {
  // Remap → _remapped_anchors
  await targetClient.query(`
    CREATE TEMP TABLE _remapped_anchors AS
    SELECT s.source_ref,
           s.old_id,
           rk.new_id  AS canon_doc_kw,
           rfk.new_id AS canon_fol_kw,
           s.description, s.co_occurrence_count, s.confidence_tier, s.status, s.last_reviewed_at
      FROM _stage_doctrine_anchors s
      JOIN _merge_remap_keywords rk
        ON rk.source_ref = s.source_ref AND rk.old_id = s.old_doctrine_keyword_id
      LEFT JOIN _merge_remap_keywords rfk
        ON rfk.source_ref = s.source_ref AND rfk.old_id = s.old_field_of_law_keyword_id
  `);
  await targetClient.query(`CREATE INDEX ON _remapped_anchors (canon_doc_kw, canon_fol_kw)`);
  await targetClient.query(`CREATE INDEX ON _remapped_anchors (source_ref, old_id)`);

  // Pre-flight tier validation. The `doctrine_anchors_validate_keyword_tiers`
  // trigger requires:
  //   canon_doc_kw.tier = 'doctrine'
  //   canon_fol_kw.tier = 'field_of_law'  (when canon_fol_kw IS NOT NULL)
  await targetClient.query(`
    CREATE TEMP TABLE _validated_anchors AS
    SELECT r.*
      FROM _remapped_anchors r
      JOIN public.keywords kd ON kd.id = r.canon_doc_kw AND kd.tier = 'doctrine'
      LEFT JOIN public.keywords kf ON kf.id = r.canon_fol_kw
     WHERE r.canon_fol_kw IS NULL OR kf.tier = 'field_of_law'
  `);
  await targetClient.query(`CREATE INDEX ON _validated_anchors (canon_doc_kw, canon_fol_kw)`);

  const { rows: [{ remapped_n }] } = await targetClient.query(
    `SELECT count(*)::bigint AS remapped_n FROM _remapped_anchors`
  );
  const { rows: [{ validated_n }] } = await targetClient.query(
    `SELECT count(*)::bigint AS validated_n FROM _validated_anchors`
  );
  const skipped = Number(remapped_n) - Number(validated_n);
  if (skipped > 0) {
    log.warn(`pre-flight: skipping ${skipped} anchors whose remapped keyword(s) lost the required tier via D-3 dedup`);
    // Surface a sample for curator review.
    const { rows: samples } = await targetClient.query(`
      SELECT r.source_ref, r.old_id,
             r.canon_doc_kw,  kd.tier AS doc_tier,  kd.keyword_text  AS doc_text,
             r.canon_fol_kw,  kf.tier AS fol_tier,  kf.keyword_text  AS fol_text
        FROM _remapped_anchors r
        LEFT JOIN public.keywords kd ON kd.id = r.canon_doc_kw
        LEFT JOIN public.keywords kf ON kf.id = r.canon_fol_kw
        LEFT JOIN _validated_anchors v ON v.source_ref = r.source_ref AND v.old_id = r.old_id
       WHERE v.old_id IS NULL
       ORDER BY r.source_ref, r.old_id
       LIMIT 10
    `);
    for (const s of samples) {
      log.warn(
        `  skipped: ${s.source_ref}/anchor#${s.old_id}  doc[id=${s.canon_doc_kw} tier=${s.doc_tier} "${s.doc_text}"]` +
          `  fol[id=${s.canon_fol_kw} tier=${s.fol_tier} "${s.fol_text}"]`
      );
    }
  }
  return { remapped: Number(remapped_n), validated: Number(validated_n), skipped };
}

async function collapseAndAllocate(targetClient) {
  // GROUP BY (canon_doc_kw, canon_fol_kw) with D-4 precedence rules, then
  // allocate new ids. Use CASE-rank for max-precedence on tier/status.
  await targetClient.query(`
    CREATE TEMP TABLE _canonical_anchors AS
    SELECT
      canon_doc_kw,
      canon_fol_kw,
      SUM(co_occurrence_count)::integer AS total_count,
      CASE MAX(CASE confidence_tier
                 WHEN 'expert_reviewed' THEN 3
                 WHEN 'llm_classified'  THEN 2
                 WHEN 'uncurated'       THEN 1
               END)
        WHEN 3 THEN 'expert_reviewed'
        WHEN 2 THEN 'llm_classified'
        WHEN 1 THEN 'uncurated'
      END AS confidence_tier,
      CASE MAX(CASE status
                 WHEN 'promoted'  THEN 3
                 WHEN 'pending'   THEN 2
                 WHEN 'dismissed' THEN 1
               END)
        WHEN 3 THEN 'promoted'
        WHEN 2 THEN 'pending'
        WHEN 1 THEN 'dismissed'
      END AS status,
      (ARRAY_AGG(description ORDER BY length(description) DESC NULLS LAST, source_ref ASC, old_id ASC))[1] AS description,
      MAX(last_reviewed_at) AS last_reviewed_at
    FROM _validated_anchors
    GROUP BY canon_doc_kw, canon_fol_kw
  `);

  await targetClient.query(`
    ALTER TABLE _canonical_anchors ADD COLUMN new_id integer
  `);
  await targetClient.query(`
    UPDATE _canonical_anchors SET new_id = nextval('public.doctrine_anchors_id_seq')::integer
  `);

  await targetClient.query(`CREATE INDEX ON _canonical_anchors (canon_doc_kw, canon_fol_kw)`);
  await targetClient.query(`CREATE INDEX ON _canonical_anchors (new_id)`);

  const { rows: [{ c }] } = await targetClient.query(
    `SELECT count(*)::bigint AS c FROM _canonical_anchors`
  );
  return Number(c);
}

async function insertAnchors(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.doctrine_anchors
      (id, doctrine_keyword_id, field_of_law_keyword_id,
       description, co_occurrence_count, confidence_tier, status, last_reviewed_at)
    SELECT new_id, canon_doc_kw, canon_fol_kw,
           description, total_count, confidence_tier, status, last_reviewed_at
      FROM _canonical_anchors
  `);
  return rowCount;
}

async function populateRemap(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO _merge_remap_doctrine_anchors (source_ref, old_id, new_id)
    SELECT v.source_ref, v.old_id, ca.new_id
      FROM _validated_anchors v
      JOIN _canonical_anchors ca
        ON ca.canon_doc_kw = v.canon_doc_kw
       AND ((ca.canon_fol_kw IS NULL  AND v.canon_fol_kw IS NULL)
         OR (ca.canon_fol_kw IS NOT NULL AND v.canon_fol_kw IS NOT NULL
             AND ca.canon_fol_kw = v.canon_fol_kw))
  `);
  return rowCount;
}

export const phase09 = {
  id: 9,
  name: 'doctrine_anchors',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase09');

    if (await remapExists(targetClient, 'doctrine_anchors')) {
      const count = await remapRowCount(targetClient, 'doctrine_anchors');
      if (count > 0) {
        log.info(`_merge_remap_doctrine_anchors already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_doctrine_anchors exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_doctrine_anchors`);
    }

    if (!(await remapExists(targetClient, 'keywords'))) {
      throw new Error('phase 9 requires _merge_remap_keywords (run phase 2 first)');
    }

    await createRemapTable(targetClient, 'doctrine_anchors', {
      oldIdType: 'integer',
      newIdType: 'integer',
    });

    const staged = await stage(targetClient, sourceClients, log);
    log.info(`total doctrine_anchors staged: ${staged}`);

    const { remapped, validated, skipped } = await remapAndValidate(targetClient, log);
    log.info(`remapped=${remapped} validated=${validated} skipped=${skipped}`);

    if (remapped !== staged) {
      throw new Error(
        `keyword remap dropped ${staged - remapped} anchors — every source anchor's doctrine_keyword_id must resolve`
      );
    }

    const canonical = await collapseAndAllocate(targetClient);
    log.info(`canonical anchors after D-4 collapse: ${canonical}`);

    const inserted = await insertAnchors(targetClient);
    log.info(`inserted ${inserted} doctrine_anchors`);

    const remap = await populateRemap(targetClient);
    log.info(`_merge_remap_doctrine_anchors rows: ${remap}`);

    if (remap !== validated) {
      throw new Error(`remap count mismatch: validated=${validated}, remap=${remap}`);
    }

    return {
      source_rows: staged,
      skipped_by_tier_validation: skipped,
      canonical_rows: canonical,
      inserted,
      remap_rows: remap,
    };
  },
};
