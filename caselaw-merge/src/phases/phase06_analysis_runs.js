// Phase 6 — analysis_runs.
// Spec §D.2 row 6:
//   Inputs:  Source `analysis_runs` + `source_ref` stamp
//   Outputs: Merged `analysis_runs`;
//            `_merge_remap_analysis_runs (source_ref, old_id, new_id)`
//   Notes:   "Small table; flat COPY with new IDs. Must precede phase 7
//            (opinion_* tables FK into it)."
//
// Volume per source: a handful of rows each (6 total at the time of B0
// audit). No natural pairing key — analysis_runs has no curie or other
// business-unique column, and source DBs all start their `id` at 1 so
// collisions are guaranteed. Uses the phase-5 nextval-allocation pattern.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const ANALYSIS_RUN_COLUMNS = [
  'env',
  'pipeline',
  'model',
  'prompt_version',
  'schema_version',
  'git_sha',
  'started_at',
  'finished_at',
  'input_tokens',
  'output_tokens',
  'cost_usd',
  'notes',
];

async function stage(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_analysis_runs (
      source_ref      text NOT NULL,
      old_id          bigint NOT NULL,
      env             text NOT NULL,
      pipeline        text NOT NULL,
      model           text NOT NULL,
      prompt_version  text NOT NULL,
      schema_version  text,
      git_sha         text,
      started_at      timestamp with time zone NOT NULL,
      finished_at     timestamp with time zone,
      input_tokens    bigint,
      output_tokens   bigint,
      cost_usd        numeric,
      notes           text,
      PRIMARY KEY (source_ref, old_id)
    )
  `);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const cols = ANALYSIS_RUN_COLUMNS.join(', ');
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             id AS old_id,
             ${cols}
        FROM analysis_runs
       ORDER BY id
    )`;
    const dest = `_stage_analysis_runs (source_ref, old_id, ${cols})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} analysis_runs from ${ref}`);
    total += n;
  }
  return total;
}

async function allocateAndInsert(targetClient, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_analysis_runs_allocated AS
    SELECT s.*,
           nextval('public.analysis_runs_id_seq')::bigint AS new_id
      FROM _stage_analysis_runs s
  `);

  const cols = ANALYSIS_RUN_COLUMNS.join(', ');
  const { rowCount: inserted } = await targetClient.query(`
    INSERT INTO public.analysis_runs (id, ${cols}, source_ref)
    SELECT new_id,
           ${ANALYSIS_RUN_COLUMNS.map((c) => 'a.' + c).join(', ')},
           a.source_ref
      FROM _stage_analysis_runs_allocated a
  `);

  const { rowCount: remap } = await targetClient.query(`
    INSERT INTO _merge_remap_analysis_runs (source_ref, old_id, new_id)
    SELECT source_ref, old_id, new_id FROM _stage_analysis_runs_allocated
  `);

  log.info(`inserted ${inserted} analysis_runs + ${remap} remap rows`);
  return { inserted, remap };
}

async function verify(targetClient, expectedTotal) {
  const { rows: [{ runs }] } = await targetClient.query(
    `SELECT count(*)::bigint AS runs FROM public.analysis_runs`
  );
  if (Number(runs) !== expectedTotal) {
    throw new Error(`analysis_runs count mismatch: staged ${expectedTotal}, found ${runs}`);
  }
  const { rows: [{ orph }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orph
      FROM _merge_remap_analysis_runs r
      LEFT JOIN public.analysis_runs a ON a.id = r.new_id
     WHERE a.id IS NULL
  `);
  if (Number(orph) !== 0) {
    throw new Error(`${orph} analysis_runs remap rows point at missing ids`);
  }
}

export const phase06 = {
  id: 6,
  name: 'analysis_runs',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase06');

    if (await remapExists(targetClient, 'analysis_runs')) {
      const count = await remapRowCount(targetClient, 'analysis_runs');
      if (count > 0) {
        log.info(`_merge_remap_analysis_runs already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_analysis_runs exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_analysis_runs`);
    }

    await createRemapTable(targetClient, 'analysis_runs', {
      oldIdType: 'bigint',
      newIdType: 'bigint',
    });

    const staged = await stage(targetClient, sourceClients, log);
    log.info(`total analysis_runs staged: ${staged}`);

    const { inserted, remap } = await allocateAndInsert(targetClient, log);

    await verify(targetClient, staged);

    return { source_rows: staged, inserted, remap_rows: remap };
  },
};
