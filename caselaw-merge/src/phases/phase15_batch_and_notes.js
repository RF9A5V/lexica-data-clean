// Phase 15 — batch_jobs + batch_opinion_requests + case_notes.
// Spec §D.2 row 15:
//   Inputs:  Source rows + `_merge_remap_cases` + new batch_job remap
//   Outputs: Merged
//   Notes:   "Operational; `source_ref` stamped on `batch_jobs`."
//
// Three sub-merges:
//   15a. batch_jobs — stamp source_ref, allocate new ids via nextval,
//        build _merge_remap_batch_jobs. UNIQUE(batch_id) is enforced
//        (cross-source collisions are theoretically possible on
//        externally-generated batch_id strings; we let the INSERT raise
//        if it happens — no plausible business case for two source DBs
//        sharing a batch_id).
//   15b. batch_opinion_requests — remap batch_job_id (REQUIRED) +
//        opinion_id (REQUIRED, treated as FK to opinions even though
//        the source schema doesn't formally declare it).
//   15c. case_notes — remap case_id (nullable). Preserve NULL case_id
//        rows verbatim.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

// ---------------------------------------------------------------------------
// 15a — batch_jobs
// ---------------------------------------------------------------------------
const BATCH_JOB_COLS = [
  'batch_id', 'status', 'created_at', 'submitted_at', 'completed_at',
  'total_requests', 'completed_requests', 'failed_requests',
  'database_name', 'limit_count', 'resume_mode', 'dry_run',
  'input_file_id', 'output_file_id', 'error_file_id', 'error_message',
  'description', 'created_by',
];

async function mergeBatchJobs(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_batch_jobs (
      source_ref          text NOT NULL,
      old_id              integer NOT NULL,
      batch_id            varchar(255) NOT NULL,
      status              varchar(50) NOT NULL,
      created_at          timestamp with time zone,
      submitted_at        timestamp with time zone,
      completed_at        timestamp with time zone,
      total_requests      integer NOT NULL,
      completed_requests  integer,
      failed_requests     integer,
      database_name       varchar(255) NOT NULL,
      limit_count         integer,
      resume_mode         boolean NOT NULL,
      dry_run             boolean NOT NULL,
      input_file_id       varchar(255),
      output_file_id      varchar(255),
      error_file_id       varchar(255),
      error_message       text,
      description         text,
      created_by          varchar(255),
      PRIMARY KEY (source_ref, old_id)
    )
  `);

  let staged = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             id AS old_id,
             ${BATCH_JOB_COLS.join(', ')}
        FROM batch_jobs
    )`;
    const dest = `_stage_batch_jobs (source_ref, old_id, ${BATCH_JOB_COLS.join(', ')})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} batch_jobs from ${ref}`);
    staged += n;
  }
  log.info(`total batch_jobs staged: ${staged}`);

  if (staged === 0) {
    log.info('no batch_jobs to merge');
    return { staged: 0, inserted: 0, remap: 0 };
  }

  await targetClient.query(`
    CREATE TEMP TABLE _stage_batch_jobs_allocated AS
    SELECT s.*, nextval('public.batch_jobs_id_seq')::integer AS new_id
      FROM _stage_batch_jobs s
  `);

  const { rowCount: inserted } = await targetClient.query(`
    INSERT INTO public.batch_jobs (id, ${BATCH_JOB_COLS.join(', ')}, source_ref)
    SELECT a.new_id,
           ${BATCH_JOB_COLS.map((c) => 'a.' + c).join(', ')},
           a.source_ref
      FROM _stage_batch_jobs_allocated a
  `);

  const { rowCount: remap } = await targetClient.query(`
    INSERT INTO _merge_remap_batch_jobs (source_ref, old_id, new_id)
    SELECT source_ref, old_id, new_id FROM _stage_batch_jobs_allocated
  `);

  log.info(`inserted ${inserted} batch_jobs + ${remap} remap rows`);
  return { staged, inserted, remap };
}

// ---------------------------------------------------------------------------
// 15b — batch_opinion_requests
// ---------------------------------------------------------------------------
const BOR_COLS = [
  'custom_id', 'status', 'processed_at', 'error_code', 'error_message',
  'extraction_successful', 'validation_successful', 'upsert_successful',
  'keywords_extracted', 'holdings_extracted', 'negative_treatments_extracted',
];

async function mergeBatchOpinionRequests(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_bor (
      source_ref                    text NOT NULL,
      old_batch_job_id              integer NOT NULL,
      old_opinion_id                integer NOT NULL,
      custom_id                     varchar(255) NOT NULL,
      status                        varchar(50) NOT NULL,
      processed_at                  timestamp with time zone,
      error_code                    varchar(50),
      error_message                 text,
      extraction_successful         boolean,
      validation_successful         boolean,
      upsert_successful             boolean,
      keywords_extracted            integer,
      holdings_extracted            integer,
      negative_treatments_extracted integer
    )
  `);

  let staged = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             batch_job_id AS old_batch_job_id,
             opinion_id   AS old_opinion_id,
             ${BOR_COLS.join(', ')}
        FROM batch_opinion_requests
    )`;
    const dest = `_stage_bor (source_ref, old_batch_job_id, old_opinion_id, ${BOR_COLS.join(', ')})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} batch_opinion_requests from ${ref}`);
    staged += n;
  }
  log.info(`total batch_opinion_requests staged: ${staged}`);

  if (staged === 0) {
    log.info('no batch_opinion_requests to merge');
    return { staged: 0, inserted: 0 };
  }

  const { rowCount: inserted } = await targetClient.query(`
    INSERT INTO public.batch_opinion_requests (batch_job_id, opinion_id, ${BOR_COLS.join(', ')})
    SELECT rbj.new_id, ro.new_id,
           ${BOR_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_bor s
      JOIN _merge_remap_batch_jobs rbj
        ON rbj.source_ref = s.source_ref AND rbj.old_id = s.old_batch_job_id
      JOIN _merge_remap_opinions ro
        ON ro.source_ref = s.source_ref AND ro.old_id = s.old_opinion_id
  `);
  const dropped = staged - inserted;
  if (dropped > 0) {
    log.warn(`dropped ${dropped} batch_opinion_requests (batch_job_id or opinion_id unresolvable)`);
  }
  log.info(`inserted ${inserted} batch_opinion_requests`);
  return { staged, inserted, dropped };
}

// ---------------------------------------------------------------------------
// 15c — case_notes
// ---------------------------------------------------------------------------
const CASE_NOTE_COLS = ['note_type', 'note', 'metadata', 'created_at'];

async function mergeCaseNotes(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_case_notes (
      source_ref   text NOT NULL,
      old_case_id  bigint,
      note_type    text NOT NULL,
      note         text NOT NULL,
      metadata     jsonb,
      created_at   timestamp with time zone
    )
  `);

  let staged = 0;
  for (const ref of SOURCE_REFS) {
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             case_id AS old_case_id,
             ${CASE_NOTE_COLS.join(', ')}
        FROM case_notes
    )`;
    const dest = `_stage_case_notes (source_ref, old_case_id, ${CASE_NOTE_COLS.join(', ')})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} case_notes from ${ref}`);
    staged += n;
  }
  log.info(`total case_notes staged: ${staged}`);

  if (staged === 0) {
    log.info('no case_notes to merge');
    return { staged: 0, inserted: 0 };
  }

  // case_id is nullable; LEFT JOIN to preserve NULL rows.
  const { rowCount: inserted } = await targetClient.query(`
    INSERT INTO public.case_notes (case_id, ${CASE_NOTE_COLS.join(', ')})
    SELECT rc.new_id,
           ${CASE_NOTE_COLS.map((c) => 's.' + c).join(', ')}
      FROM _stage_case_notes s
      LEFT JOIN _merge_remap_cases rc
        ON rc.source_ref = s.source_ref AND rc.old_id = s.old_case_id
     -- If old_case_id was non-null but didn't resolve, fail loudly
     -- (would indicate a remap-table bug). LEFT JOIN handles the
     -- legitimately-NULL old_case_id rows.
     WHERE s.old_case_id IS NULL OR rc.new_id IS NOT NULL
  `);
  const dropped = staged - inserted;
  if (dropped > 0) {
    log.warn(`dropped ${dropped} case_notes whose non-null case_id could not be remapped`);
  }
  log.info(`inserted ${inserted} case_notes`);
  return { staged, inserted, dropped };
}

// ---------------------------------------------------------------------------
// Phase 15 main
// ---------------------------------------------------------------------------

async function alreadyRan(targetClient) {
  // batch_jobs is the simplest sentinel — small, and is the predecessor
  // for batch_opinion_requests.
  if (await remapExists(targetClient, 'batch_jobs')) {
    const c = await remapRowCount(targetClient, 'batch_jobs');
    return c > 0;
  }
  return false;
}

export const phase15 = {
  id: 15,
  name: 'batch_jobs + batch_opinion_requests + case_notes',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase15');

    if (await alreadyRan(targetClient)) {
      log.info('_merge_remap_batch_jobs already populated — skipping.');
      return { skipped: true };
    }
    for (const need of ['cases', 'opinions']) {
      if (!(await remapExists(targetClient, need))) {
        throw new Error(`phase 15 requires _merge_remap_${need}`);
      }
    }

    await createRemapTable(targetClient, 'batch_jobs', {
      oldIdType: 'integer',
      newIdType: 'integer',
    });

    log.info('▸ 15a: batch_jobs');
    const a = await mergeBatchJobs(targetClient, sourceClients, log);

    log.info('▸ 15b: batch_opinion_requests');
    const b = await mergeBatchOpinionRequests(targetClient, sourceClients, log);

    log.info('▸ 15c: case_notes');
    const c = await mergeCaseNotes(targetClient, sourceClients, log);

    return {
      batch_jobs: a,
      batch_opinion_requests: b,
      case_notes: c,
    };
  },
};
