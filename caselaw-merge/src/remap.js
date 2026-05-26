// Per spec §D.2 + §D.3: each phase that allocates new integer IDs writes a
// `_merge_remap_<table>` table mapping `(source_ref, old_id) → new_id`. These
// tables double as the resume marker — main.js consults them to decide
// whether a phase has already run.
//
// Tables created:
//   _merge_remap_keywords         (phase 2)
//   _merge_remap_cases            (phase 3)
//   _merge_remap_opinions         (phase 4) — used only by footnotes +
//                                              appellate_history_case_status/
//                                              resolution_queue
//   _merge_remap_citations        (phase 5)
//   _merge_remap_analysis_runs    (phase 6)
//   _merge_remap_doctrine_anchors (phase 9)
//   _merge_remap_batch_jobs       (phase 15)
//
// Phase 16 drops all `_merge_remap_*` tables once final validation passes.

const REMAP_TABLES = {
  keywords: '_merge_remap_keywords',
  cases: '_merge_remap_cases',
  opinions: '_merge_remap_opinions',
  citations: '_merge_remap_citations',
  analysis_runs: '_merge_remap_analysis_runs',
  doctrine_anchors: '_merge_remap_doctrine_anchors',
  batch_jobs: '_merge_remap_batch_jobs',
};

export function remapTableName(logical) {
  const t = REMAP_TABLES[logical];
  if (!t) throw new Error(`Unknown logical remap table: ${logical}`);
  return t;
}

export async function createRemapTable(client, logical, { oldIdType = 'bigint', newIdType = 'bigint' } = {}) {
  const table = remapTableName(logical);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${table} (
      source_ref text NOT NULL,
      old_id     ${oldIdType} NOT NULL,
      new_id     ${newIdType} NOT NULL,
      PRIMARY KEY (source_ref, old_id)
    )
  `);
  await client.query(
    `CREATE INDEX IF NOT EXISTS ${table}_new_id_idx ON ${table} (new_id)`
  );
}

export async function remapExists(client, logical) {
  const { rows } = await client.query(
    `SELECT to_regclass('public.' || $1) AS oid`,
    [remapTableName(logical)]
  );
  return rows[0].oid !== null;
}

export async function remapRowCount(client, logical) {
  if (!(await remapExists(client, logical))) return 0;
  const { rows } = await client.query(`SELECT count(*)::bigint AS c FROM ${remapTableName(logical)}`);
  return rows[0].c;
}

export async function dropAllRemapTables(client) {
  for (const t of Object.values(REMAP_TABLES)) {
    await client.query(`DROP TABLE IF EXISTS ${t}`);
  }
}

export async function lookupNewId(client, logical, sourceRef, oldId) {
  const { rows } = await client.query(
    `SELECT new_id FROM ${remapTableName(logical)} WHERE source_ref = $1 AND old_id = $2`,
    [sourceRef, oldId]
  );
  return rows[0]?.new_id ?? null;
}
