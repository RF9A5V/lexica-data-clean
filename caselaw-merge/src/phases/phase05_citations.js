// Phase 5 — Citations + citation_corrections.
// Spec §D.2 row 5:
//   Inputs:  Source `citations` + `citation_corrections` + `_merge_remap_cases`
//   Outputs: Merged tables; `_merge_remap_citations`
//   Notes:   "Standard FK remap on `case_id`."
//
// Wrinkle: `citations.curie` is ~26% NULL in practice (~329k of 1.27M rows
// across the three sources). B0 §A.2's "0 NULLs" was counting distinct
// non-null curies, not raw rows. So we can't pair old↔new via curie like
// phases 3 and 4. Instead this phase pre-allocates new IDs via
// `nextval('citations_id_seq')` into the staging table, then INSERTs with
// explicit IDs — pairing is materialized before the INSERT runs.
//
// `citation_corrections` has no remap table (no other table FKs it).
// Both citation_id and case_id columns are remapped via _merge_remap_*
// and the LEFT JOINs are validated to ensure non-null old_ids find their
// match.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { copyBetween } from '../copyPipe.js';

const CITATION_COLUMNS = [
  'citation_type',
  'cite',
  'created_at',
  'curie',
  'normalized_form',
];

const CORRECTION_COLUMNS = [
  'observed_cite',
  'observed_reporter',
  'corrected_cite',
  'expected_reporter',
  'source_id',
  'file_volume',
  'reason',
  'confidence',
  'applied',
  'applied_at',
  'created_at',
];

// ---------------------------------------------------------------------------
// Sub-phase A — citations
// ---------------------------------------------------------------------------

async function stageCitations(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_citations (
      source_ref       text NOT NULL,
      old_id           integer NOT NULL,
      old_case_id      bigint  NOT NULL,
      citation_type    public.citation_type_enum,
      cite             text NOT NULL,
      created_at       timestamp without time zone,
      curie            text,
      normalized_form  text,
      PRIMARY KEY (source_ref, old_id)
    )
  `);
  await targetClient.query(`CREATE INDEX ON _stage_citations (source_ref, old_case_id)`);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const t0 = Date.now();
    const cols = CITATION_COLUMNS.join(', ');
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             id      AS old_id,
             case_id AS old_case_id,
             ${cols}
        FROM citations
       ORDER BY id
    )`;
    const dest = `_stage_citations (source_ref, old_id, old_case_id, ${cols})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} citations from ${ref} (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
    total += n;
  }
  await targetClient.query(`ANALYZE _stage_citations`);
  return total;
}

async function allocateAndInsertCitations(targetClient, log) {
  // 1. Pre-allocate new IDs via nextval, materialised alongside staging.
  //    nextval is volatile so it fires per row; CTAS pins the pairing.
  const tAlloc = Date.now();
  await targetClient.query(`
    CREATE TEMP TABLE _stage_citations_allocated AS
    SELECT s.*,
           nextval('public.citations_id_seq')::integer AS new_id
      FROM _stage_citations s
  `);
  await targetClient.query(`CREATE INDEX ON _stage_citations_allocated (source_ref, old_case_id)`);
  await targetClient.query(`CREATE INDEX ON _stage_citations_allocated (new_id)`);
  await targetClient.query(`ANALYZE _stage_citations_allocated`);
  log.info(`allocated new ids (${((Date.now() - tAlloc) / 1000).toFixed(1)}s)`);

  // 2. INSERT citations with explicit ids, case_id remapped via
  //    _merge_remap_cases.
  const tIns = Date.now();
  const { rowCount: cit } = await targetClient.query(`
    INSERT INTO public.citations (id, case_id, citation_type, cite, created_at, curie, normalized_form)
    SELECT a.new_id,
           rc.new_id,
           a.citation_type,
           a.cite,
           a.created_at,
           a.curie,
           a.normalized_form
      FROM _stage_citations_allocated a
      JOIN _merge_remap_cases rc
        ON rc.source_ref = a.source_ref
       AND rc.old_id     = a.old_case_id
  `);
  log.info(`inserted ${cit} citations (${((Date.now() - tIns) / 1000).toFixed(1)}s)`);

  // 3. Populate _merge_remap_citations directly from the allocated table.
  const { rowCount: rem } = await targetClient.query(`
    INSERT INTO _merge_remap_citations (source_ref, old_id, new_id)
    SELECT source_ref, old_id, new_id FROM _stage_citations_allocated
  `);
  log.info(`_merge_remap_citations rows: ${rem}`);

  return { citations: cit, remap: rem };
}

// ---------------------------------------------------------------------------
// Sub-phase B — citation_corrections
// ---------------------------------------------------------------------------

async function stageCorrections(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_corrections (
      source_ref         text NOT NULL,
      old_id             bigint NOT NULL,
      old_citation_id    integer,
      old_case_id        bigint,
      observed_cite      text NOT NULL,
      observed_reporter  text,
      corrected_cite     text NOT NULL,
      expected_reporter  text,
      source_id          text,
      file_volume        integer,
      reason             text,
      confidence         text,
      applied            boolean,
      applied_at         timestamp with time zone,
      created_at         timestamp with time zone,
      PRIMARY KEY (source_ref, old_id)
    )
  `);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const t0 = Date.now();
    const cols = CORRECTION_COLUMNS.join(', ');
    const select = `(
      SELECT $$${ref}$$::text AS source_ref,
             id          AS old_id,
             citation_id AS old_citation_id,
             case_id     AS old_case_id,
             ${cols}
        FROM citation_corrections
       ORDER BY id
    )`;
    const dest = `_stage_corrections (source_ref, old_id, old_citation_id, old_case_id, ${cols})`;
    const n = await copyBetween(sourceClients[ref], select, targetClient, dest);
    log.info(`copied ${n} citation_corrections from ${ref} (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
    total += n;
  }
  await targetClient.query(`ANALYZE _stage_corrections`);
  return total;
}

async function validateCorrectionsCanRemap(targetClient) {
  const { rows: [{ orphan_cit }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orphan_cit
      FROM _stage_corrections s
      LEFT JOIN _merge_remap_citations r
        ON r.source_ref = s.source_ref AND r.old_id = s.old_citation_id
     WHERE s.old_citation_id IS NOT NULL AND r.new_id IS NULL
  `);
  if (Number(orphan_cit) !== 0) {
    throw new Error(`${orphan_cit} citation_corrections rows reference unknown source citation_id`);
  }
  const { rows: [{ orphan_case }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orphan_case
      FROM _stage_corrections s
      LEFT JOIN _merge_remap_cases r
        ON r.source_ref = s.source_ref AND r.old_id = s.old_case_id
     WHERE s.old_case_id IS NOT NULL AND r.new_id IS NULL
  `);
  if (Number(orphan_case) !== 0) {
    throw new Error(`${orphan_case} citation_corrections rows reference unknown source case_id`);
  }
}

async function insertCorrections(targetClient) {
  const cols = CORRECTION_COLUMNS.join(', ');
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.citation_corrections (citation_id, case_id, ${cols})
    SELECT rcit.new_id,
           rcase.new_id,
           ${CORRECTION_COLUMNS.map((c) => 's.' + c).join(', ')}
      FROM _stage_corrections s
      LEFT JOIN _merge_remap_citations rcit
        ON rcit.source_ref = s.source_ref
       AND rcit.old_id     = s.old_citation_id
      LEFT JOIN _merge_remap_cases rcase
        ON rcase.source_ref = s.source_ref
       AND rcase.old_id     = s.old_case_id
  `);
  return rowCount;
}

// ---------------------------------------------------------------------------
// Verify + main
// ---------------------------------------------------------------------------

async function verify(targetClient, citationsStaged, correctionsStaged) {
  const { rows: [{ cit }] } = await targetClient.query(
    `SELECT count(*)::bigint AS cit FROM public.citations`
  );
  if (Number(cit) !== citationsStaged) {
    throw new Error(`citations count mismatch: staged ${citationsStaged}, found ${cit}`);
  }
  const { rows: [{ remap }] } = await targetClient.query(
    `SELECT count(*)::bigint AS remap FROM _merge_remap_citations`
  );
  if (Number(remap) !== citationsStaged) {
    throw new Error(`_merge_remap_citations count mismatch: ${remap} vs ${citationsStaged}`);
  }
  const { rows: [{ corr }] } = await targetClient.query(
    `SELECT count(*)::bigint AS corr FROM public.citation_corrections`
  );
  if (Number(corr) !== correctionsStaged) {
    throw new Error(`citation_corrections count mismatch: staged ${correctionsStaged}, found ${corr}`);
  }
  // Every remap.new_id should resolve to a citations row.
  const { rows: [{ orph }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orph
      FROM _merge_remap_citations r
      LEFT JOIN public.citations c ON c.id = r.new_id
     WHERE c.id IS NULL
  `);
  if (Number(orph) !== 0) {
    throw new Error(`${orph} citation remap rows point at missing citations.id`);
  }
}

export const phase05 = {
  id: 5,
  name: 'Citations + citation_corrections',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase05');

    if (await remapExists(targetClient, 'citations')) {
      const count = await remapRowCount(targetClient, 'citations');
      if (count > 0) {
        log.info(`_merge_remap_citations already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_citations exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_citations`);
    }

    if (!(await remapExists(targetClient, 'cases'))) {
      throw new Error('phase 3 (cases) must run before phase 5 — _merge_remap_cases missing');
    }

    await createRemapTable(targetClient, 'citations', {
      oldIdType: 'integer',
      newIdType: 'integer',
    });

    // Sub-phase A — citations
    log.info('▸ sub-phase A: citations');
    const citationsStaged = await stageCitations(targetClient, sourceClients, log);
    log.info(`total citations staged: ${citationsStaged}`);
    const { citations, remap } = await allocateAndInsertCitations(targetClient, log);

    // Sub-phase B — citation_corrections
    log.info('▸ sub-phase B: citation_corrections');
    const correctionsStaged = await stageCorrections(targetClient, sourceClients, log);
    log.info(`total citation_corrections staged: ${correctionsStaged}`);
    await validateCorrectionsCanRemap(targetClient);
    const corrections = await insertCorrections(targetClient);
    log.info(`inserted ${corrections} citation_corrections`);

    await verify(targetClient, citationsStaged, correctionsStaged);

    return {
      citations_source_rows: citationsStaged,
      citations_inserted: citations,
      citation_remap_rows: remap,
      corrections_source_rows: correctionsStaged,
      corrections_inserted: corrections,
    };
  },
};
