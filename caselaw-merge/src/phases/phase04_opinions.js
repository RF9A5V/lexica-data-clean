// Phase 4 — Opinions.
// Spec §D.2 row 4:
//   Inputs:  Source DBs' `opinions` + `_merge_remap_cases`
//   Outputs: Merged `opinions`;
//            `_merge_remap_opinions (source_ref, old_id, new_id)` — used only
//            by `opinion_footnotes` post-D-6 +
//            `appellate_history_case_status.last_opinion_id` +
//            `appellate_history_resolution_queue.citing_opinion_id`.
//   Notes:   "Fresh `id`s. CURIE carried. `case_id` looked up via
//            `_merge_remap_cases`."
//
// Same shape as phase 3 (cases) but with an additional case_id remap via
// _merge_remap_cases. Source-side `case_id` joins to the cases remap to
// recover the merged case_id; new opinion id is paired back to old via
// `opinions.curie` (globally unique cross-source — B0 Audit A).
//
// Bulk-load optimization:
//   The two GIN indexes on opinions (idx_opinions_text +
//   idx_opinions_text_search_vector) and the BEFORE-INSERT
//   text_search_vector trigger together make per-row INSERT on 968k
//   opinions prohibitively slow (>10 min before we cancelled the first
//   attempt). The standard fix: drop the GIN indexes + disable the
//   trigger, compute text_search_vector inline in the SELECT, then
//   recreate the indexes in a single shot post-INSERT (parallelisable
//   by Postgres). All wrapped in the phase transaction so a failure
//   leaves the schema untouched.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

// Columns shared by source.opinions and target.opinions. case_id is
// remapped separately (carried as old_case_id in staging); id +
// text_search_vector are skipped (autogen / trigger-computed); source_ref
// is the new column we stamp.
const OPINION_COLUMNS = [
  'opinion_type',
  'author',
  'text',
  'created_at',
  'is_valueless',
  'valueless_reason',
  'curie',
  'opinion_index',
  'page_breaks',
];

async function createStaging(targetClient) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_opinions (
      source_ref         text     NOT NULL,
      old_id             integer  NOT NULL,
      old_case_id        bigint   NOT NULL,
      opinion_type       text,
      author             text,
      text               text,
      created_at         timestamp without time zone,
      is_valueless       boolean  NOT NULL,
      valueless_reason   text,
      curie              text     NOT NULL,
      opinion_index      integer,
      page_breaks        jsonb,
      PRIMARY KEY (source_ref, old_id)
    )
  `);
  await targetClient.query(`CREATE INDEX ON _stage_opinions (curie)`);
  await targetClient.query(`CREATE INDEX ON _stage_opinions (source_ref, old_case_id)`);
}

async function copySourceIntoStaging(sourceClient, targetClient, sourceRef) {
  const cols = OPINION_COLUMNS.join(', ');
  const select = `(
    SELECT $$${sourceRef}$$::text AS source_ref,
           id      AS old_id,
           case_id AS old_case_id,
           ${cols}
      FROM opinions
     ORDER BY id
  )`;
  const dest = `_stage_opinions (source_ref, old_id, old_case_id, ${cols})`;
  return copyBetween(sourceClient, select, targetClient, dest);
}

async function dropExpensiveIndexes(targetClient, log) {
  log.info('dropping GIN indexes + disabling tsvector trigger for bulk load');
  await targetClient.query(`DROP INDEX IF EXISTS public.idx_opinions_text`);
  await targetClient.query(`DROP INDEX IF EXISTS public.idx_opinions_text_search_vector`);
  await targetClient.query(`
    ALTER TABLE public.opinions DISABLE TRIGGER trigger_update_opinion_text_search_vector
  `);
}

async function recreateExpensiveIndexes(targetClient, log) {
  log.info('recreating GIN indexes + re-enabling tsvector trigger');
  await targetClient.query(`
    ALTER TABLE public.opinions ENABLE TRIGGER trigger_update_opinion_text_search_vector
  `);
  await targetClient.query(`
    CREATE INDEX idx_opinions_text
      ON public.opinions
      USING gin (to_tsvector('english'::regconfig, text))
  `);
  await targetClient.query(`
    CREATE INDEX idx_opinions_text_search_vector
      ON public.opinions
      USING gin (text_search_vector)
  `);
}

async function insertOpinionsAndPopulateRemap(targetClient) {
  const cols = OPINION_COLUMNS.join(', ');
  // INSERT … SELECT joins staging to _merge_remap_cases on (source_ref,
  // old_case_id) to recover the merged case_id. text_search_vector is
  // computed inline (trigger disabled above) — same expression as the
  // trigger but the planner can pipeline it with the INSERT instead of
  // calling a plpgsql function per row.
  // RETURNING (id, curie, source_ref) pairs back to staging by (curie,
  // source_ref) to populate the remap. curie alone would suffice (global
  // uniqueness per B0 Audit A); the composite key makes intent explicit.
  const { rowCount } = await targetClient.query(`
    WITH ins AS (
      INSERT INTO public.opinions (
        case_id, ${cols}, text_search_vector, source_ref
      )
      SELECT rc.new_id AS case_id,
             ${OPINION_COLUMNS.map((c) => 's.' + c).join(', ')},
             to_tsvector('english'::regconfig, COALESCE(s.text, '')) AS text_search_vector,
             s.source_ref
        FROM _stage_opinions s
        JOIN _merge_remap_cases rc
          ON rc.source_ref = s.source_ref
         AND rc.old_id     = s.old_case_id
      RETURNING id, curie, source_ref
    )
    INSERT INTO _merge_remap_opinions (source_ref, old_id, new_id)
    SELECT s.source_ref, s.old_id, ins.id
      FROM ins
      JOIN _stage_opinions s
        ON s.curie       = ins.curie
       AND s.source_ref  = ins.source_ref
  `);
  return rowCount;
}

async function verify(targetClient, expectedTotal) {
  const { rows: [{ remap_count }] } = await targetClient.query(
    `SELECT count(*)::bigint AS remap_count FROM _merge_remap_opinions`
  );
  if (Number(remap_count) !== expectedTotal) {
    throw new Error(
      `remap mismatch: staged ${expectedTotal} opinions, wrote ${remap_count} remap rows`
    );
  }
  const { rows: [{ opinions_count }] } = await targetClient.query(
    `SELECT count(*)::bigint AS opinions_count FROM public.opinions`
  );
  if (Number(opinions_count) !== expectedTotal) {
    throw new Error(
      `opinions mismatch: staged ${expectedTotal} opinions, public.opinions has ${opinions_count}`
    );
  }
  const { rows: [{ orphans }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orphans
      FROM _merge_remap_opinions r
      LEFT JOIN public.opinions o ON o.id = r.new_id
     WHERE o.id IS NULL
  `);
  if (Number(orphans) !== 0) {
    throw new Error(`${orphans} remap rows point at missing opinion ids`);
  }
}

export const phase04 = {
  id: 4,
  name: 'Opinions',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase04');

    if (await remapExists(targetClient, 'opinions')) {
      const count = await remapRowCount(targetClient, 'opinions');
      if (count > 0) {
        log.info(`_merge_remap_opinions already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_opinions exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_opinions`);
    }

    // Sanity: phase 3 must have run.
    if (!(await remapExists(targetClient, 'cases'))) {
      throw new Error('phase 3 (cases) must run before phase 4 — _merge_remap_cases missing');
    }

    await createRemapTable(targetClient, 'opinions', {
      oldIdType: 'integer',
      newIdType: 'integer',
    });

    await createStaging(targetClient);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const t0 = Date.now();
      const n = await copySourceIntoStaging(sourceClients[ref], targetClient, ref);
      const dt = ((Date.now() - t0) / 1000).toFixed(1);
      log.info(`copied ${n} opinions from ${ref} (${dt}s)`);
      staged += n;
    }
    log.info(`total opinions staged: ${staged}`);

    await targetClient.query(`ANALYZE _stage_opinions`);

    await dropExpensiveIndexes(targetClient, log);

    const tInsert = Date.now();
    const remapped = await insertOpinionsAndPopulateRemap(targetClient);
    log.info(`inserted ${remapped} opinions + remap rows (${((Date.now() - tInsert)/1000).toFixed(1)}s)`);

    const tIdx = Date.now();
    await recreateExpensiveIndexes(targetClient, log);
    log.info(`GIN indexes rebuilt (${((Date.now() - tIdx)/1000).toFixed(1)}s)`);

    await verify(targetClient, staged);

    return {
      source_rows: staged,
      opinion_rows: staged,
      remap_rows: remapped,
    };
  },
};
