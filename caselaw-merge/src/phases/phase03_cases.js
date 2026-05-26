// Phase 3 — Cases.
// Spec §D.2 row 3:
//   Inputs:  Source DBs' `cases` + `source_ref` stamp
//   Outputs: Merged `cases`; `_merge_remap_cases (source_ref, old_id, new_id)`
//   Notes:   "Fresh `id`s allocated by serial. CURIE carried verbatim.
//            `original_id` preserved as-is for traceability."
//
// Algorithm:
//   1. CREATE TEMP TABLE _stage_cases with the same column set as cases
//      (minus id + name_search_vector which the trigger recomputes) plus
//      source_ref + old_id.
//   2. For each source DB: COPY rows out of source.cases and into _stage_cases
//      via a piped COPY (pg-copy-streams). ~1-2 minutes total for 940k rows.
//   3. Bulk INSERT _stage_cases → public.cases in a single statement, using
//      a CTE that joins back via (source_ref, curie) to materialize the
//      (old_id → new_id) pairing into _merge_remap_cases. CURIE is
//      globally unique cross-source per B0 Audit A.
//   4. Validate: remap row count == staged row count.
//
// Triggers fired on INSERT INTO cases:
//   * cases_name_search_update BEFORE INSERT — recomputes name_search_vector
//     from name + name_abbreviation; pays per-row but matches spec D-7.
//   * cases_updated_at_trigger BEFORE UPDATE — only on UPDATE; no-op here.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

// Columns shared by both source and target. Order matters: both the
// source SELECT and the target COPY column list use this same order.
const CASE_COLUMNS = [
  'name',
  'name_abbreviation',
  'decision_date',
  'docket_number',
  'first_page',
  'last_page',
  'file_name',
  'court_name',
  'court_name_abbreviation',
  'court_id',
  'jurisdiction_name',
  'jurisdiction_abbreviation',
  'jurisdiction_id',
  'created_at',
  'updated_at',
  'citation_count',
  'original_id',
  'source_url',
  'court_department',
  'curie',
  'curie_locked_at',
  'display_slug',
];

async function createStaging(targetClient) {
  // Mirror cases column types but skip id + name_search_vector (regenerated
  // by trigger). Carry source_ref + old_id for the post-COPY remap pairing.
  await targetClient.query(`
    CREATE TEMP TABLE _stage_cases (
      source_ref                 text    NOT NULL,
      old_id                     bigint  NOT NULL,
      name                       text    NOT NULL,
      name_abbreviation          text,
      decision_date              text,
      docket_number              text,
      first_page                 text,
      last_page                  text,
      file_name                  text,
      court_name                 text,
      court_name_abbreviation    text,
      court_id                   integer,
      jurisdiction_name          text,
      jurisdiction_abbreviation  text,
      jurisdiction_id            integer,
      created_at                 timestamp without time zone,
      updated_at                 timestamp without time zone,
      citation_count             integer,
      original_id                bigint,
      source_url                 text,
      court_department           smallint,
      curie                      text NOT NULL,
      curie_locked_at            timestamp with time zone,
      display_slug               text,
      PRIMARY KEY (source_ref, old_id)
    )
  `);
  await targetClient.query(`CREATE INDEX ON _stage_cases (curie)`);
}

async function copySourceIntoStaging(sourceClient, targetClient, sourceRef) {
  // Inject source_ref as a literal in the SELECT so the target COPY column
  // list lines up. ORDER BY id is for deterministic ordering, not correctness.
  const cols = CASE_COLUMNS.join(', ');
  const select = `(
    SELECT $$${sourceRef}$$::text AS source_ref,
           id AS old_id,
           ${cols}
      FROM cases
     ORDER BY id
  )`;
  const dest = `_stage_cases (source_ref, old_id, ${cols})`;
  return copyBetween(sourceClient, select, targetClient, dest);
}

async function insertCasesAndPopulateRemap(targetClient) {
  const cols = CASE_COLUMNS.join(', ');
  // Single statement: INSERT into cases RETURNING new id + curie + source_ref,
  // then JOIN back to staging by (curie, source_ref) to map old_id → new_id.
  // CURIE uniqueness is global (B0 Audit A) so the curie alone would suffice,
  // but source_ref makes the JOIN explicit and self-documenting.
  const { rowCount } = await targetClient.query(`
    WITH ins AS (
      INSERT INTO public.cases (${cols}, source_ref)
      SELECT ${cols}, source_ref FROM _stage_cases
      RETURNING id, curie, source_ref
    )
    INSERT INTO _merge_remap_cases (source_ref, old_id, new_id)
    SELECT s.source_ref, s.old_id, ins.id
      FROM ins
      JOIN _stage_cases s
        ON s.curie = ins.curie
       AND s.source_ref = ins.source_ref
  `);
  return rowCount;
}

async function verify(targetClient, expectedTotal) {
  const { rows: [{ remap_count }] } = await targetClient.query(
    `SELECT count(*)::bigint AS remap_count FROM _merge_remap_cases`
  );
  if (Number(remap_count) !== expectedTotal) {
    throw new Error(
      `remap mismatch: staged ${expectedTotal} cases, wrote ${remap_count} remap rows`
    );
  }
  const { rows: [{ cases_count }] } = await targetClient.query(
    `SELECT count(*)::bigint AS cases_count FROM public.cases`
  );
  if (Number(cases_count) !== expectedTotal) {
    throw new Error(
      `cases mismatch: staged ${expectedTotal} cases, public.cases has ${cases_count}`
    );
  }
  const { rows: [{ orphans }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orphans
      FROM _merge_remap_cases r
      LEFT JOIN public.cases c ON c.id = r.new_id
     WHERE c.id IS NULL
  `);
  if (Number(orphans) !== 0) {
    throw new Error(`${orphans} remap rows point at missing case ids`);
  }
}

export const phase03 = {
  id: 3,
  name: 'Cases',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase03');

    if (await remapExists(targetClient, 'cases')) {
      const count = await remapRowCount(targetClient, 'cases');
      if (count > 0) {
        log.info(`_merge_remap_cases already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_cases exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_cases`);
    }

    await createRemapTable(targetClient, 'cases', {
      oldIdType: 'bigint',
      newIdType: 'bigint',
    });

    await createStaging(targetClient);

    let staged = 0;
    for (const ref of SOURCE_REFS) {
      const t0 = Date.now();
      const n = await copySourceIntoStaging(sourceClients[ref], targetClient, ref);
      const dt = ((Date.now() - t0) / 1000).toFixed(1);
      log.info(`copied ${n} cases from ${ref} (${dt}s)`);
      staged += n;
    }
    log.info(`total cases staged: ${staged}`);

    await targetClient.query(`ANALYZE _stage_cases`);

    const t0 = Date.now();
    const remapped = await insertCasesAndPopulateRemap(targetClient);
    log.info(`inserted ${remapped} cases + remap rows (${((Date.now() - t0)/1000).toFixed(1)}s)`);

    await verify(targetClient, staged);

    return {
      source_rows: staged,
      case_rows: staged,
      remap_rows: remapped,
    };
  },
};
