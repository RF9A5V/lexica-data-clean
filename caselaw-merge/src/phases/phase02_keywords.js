// Phase 2 — Keywords (with dedup).
// Spec §D.2 row 2 + §B D-3.
//
// Algorithm (D-3 — "most-frequent-tier wins"):
//   1. Read all (id, keyword_text, tier, frequency) rows from every source DB.
//   2. Group by lower(keyword_text). For each group:
//      * winning tier = the tier with the highest SUM(frequency); ties broken
//        by alphabetical tier order (deterministic).
//      * canonical text form = the variant within the group with the highest
//        individual frequency; ties broken by alphabetical text order.
//      * canonical frequency = SUM(frequency) across the whole group, irrespective
//        of tier (a curator can split later).
//   3. INSERT one row per group into `keywords`.
//   4. INSERT one row per source-side row into `_merge_remap_keywords`
//      mapping (source_ref, old_id) → new canonical id.
//   5. For every (group, losing_tier) pair, INSERT a row into
//      `keyword_dedup_conflicts` so a curator can review. ~83 expected per
//      B0 Audit B.
//
// Phase 9 hazard: doctrine_anchors_validate_keyword_tiers trigger requires
// doctrine_keyword_id to point at a row with tier='doctrine'. If a curator
// conflict ends up flipping a keyword's tier away from 'doctrine', phase 9
// inserts will RAISE EXCEPTION for the anchors that referenced it. Out of
// scope here — flag for phase 9 / curator review. See B1 scaffolding memory.

import { createRemapTable, remapExists, remapRowCount } from '../remap.js';
import { SOURCE_REFS } from '../config.js';
import { bulkInsert } from '../bulk.js';

const STAGING_COLUMNS = ['source_ref', 'old_id', 'keyword_text', 'keyword_text_lower', 'tier', 'frequency'];

async function readKeywordsFromSource(sourceClient) {
  const { rows } = await sourceClient.query(`
    SELECT id, keyword_text, tier, frequency
      FROM keywords
  `);
  return rows;
}

async function buildStaging(targetClient, sourceClients, log) {
  await targetClient.query(`
    CREATE TEMP TABLE _stage_keywords (
      source_ref         text    NOT NULL,
      old_id             integer NOT NULL,
      keyword_text       text    NOT NULL,
      keyword_text_lower text    NOT NULL,
      tier               text    NOT NULL,
      frequency          integer NOT NULL,
      PRIMARY KEY (source_ref, old_id)
    )
  `);
  await targetClient.query(`CREATE INDEX ON _stage_keywords (keyword_text_lower)`);

  let total = 0;
  for (const ref of SOURCE_REFS) {
    const sourceRows = await readKeywordsFromSource(sourceClients[ref]);
    const staged = sourceRows.map((r) => ({
      source_ref: ref,
      old_id: r.id,
      keyword_text: r.keyword_text,
      keyword_text_lower: r.keyword_text.toLowerCase(),
      tier: r.tier,
      frequency: r.frequency,
    }));
    await bulkInsert(targetClient, '_stage_keywords', STAGING_COLUMNS, staged);
    total += staged.length;
    log.info(`staged ${staged.length} keywords from ${ref}`);
  }
  await targetClient.query(`ANALYZE _stage_keywords`);
  return total;
}

async function computeCanonical(targetClient) {
  // Materialize the per-group winner so subsequent steps can join cheaply.
  await targetClient.query(`
    CREATE TEMP TABLE _canonical_keywords (
      keyword_text_lower text    NOT NULL PRIMARY KEY,
      canonical_text     text    NOT NULL,
      winning_tier       text    NOT NULL,
      total_frequency    integer NOT NULL
    )
  `);

  await targetClient.query(`
    INSERT INTO _canonical_keywords (keyword_text_lower, canonical_text, winning_tier, total_frequency)
    WITH per_tier AS (
      SELECT keyword_text_lower, tier,
             SUM(frequency)::integer AS tier_freq
        FROM _stage_keywords
       GROUP BY keyword_text_lower, tier
    ),
    winners AS (
      SELECT keyword_text_lower, tier AS winning_tier,
             row_number() OVER (
               PARTITION BY keyword_text_lower
               ORDER BY tier_freq DESC, tier ASC
             ) AS rn
        FROM per_tier
    ),
    canon_text AS (
      SELECT DISTINCT ON (keyword_text_lower)
             keyword_text_lower, keyword_text
        FROM _stage_keywords
       ORDER BY keyword_text_lower, frequency DESC, keyword_text ASC
    ),
    totals AS (
      SELECT keyword_text_lower, SUM(frequency)::integer AS total_freq
        FROM _stage_keywords
       GROUP BY keyword_text_lower
    )
    SELECT w.keyword_text_lower, c.keyword_text, w.winning_tier, t.total_freq
      FROM winners w
      JOIN canon_text c USING (keyword_text_lower)
      JOIN totals    t USING (keyword_text_lower)
     WHERE w.rn = 1
  `);
  await targetClient.query(`ANALYZE _canonical_keywords`);
}

async function insertCanonicalKeywords(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.keywords (keyword_text, tier, frequency)
    SELECT canonical_text, winning_tier, total_frequency
      FROM _canonical_keywords
  `);
  return rowCount;
}

async function populateRemap(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO _merge_remap_keywords (source_ref, old_id, new_id)
    SELECT s.source_ref, s.old_id, k.id
      FROM _stage_keywords     s
      JOIN _canonical_keywords c ON c.keyword_text_lower = s.keyword_text_lower
      JOIN public.keywords     k ON k.keyword_text       = c.canonical_text
  `);
  return rowCount;
}

async function logConflicts(targetClient) {
  const { rowCount } = await targetClient.query(`
    INSERT INTO public.keyword_dedup_conflicts (
      keyword_text, winning_tier, losing_tier,
      winning_count, losing_count,
      sources_winning, sources_losing
    )
    WITH per_tier AS (
      SELECT keyword_text_lower, tier,
             SUM(frequency)::integer AS tier_freq,
             array_agg(DISTINCT source_ref ORDER BY source_ref) AS srcs
        FROM _stage_keywords
       GROUP BY keyword_text_lower, tier
    )
    SELECT c.canonical_text,
           c.winning_tier,
           l.tier,
           w.tier_freq,
           l.tier_freq,
           w.srcs,
           l.srcs
      FROM _canonical_keywords c
      JOIN per_tier w ON w.keyword_text_lower = c.keyword_text_lower
                     AND w.tier               = c.winning_tier
      JOIN per_tier l ON l.keyword_text_lower = c.keyword_text_lower
                     AND l.tier              <> c.winning_tier
  `);
  return rowCount;
}

async function verifyIntegrity(targetClient, sourceTotal) {
  // Every source row must have a remap entry.
  const { rows: [{ remap_count }] } = await targetClient.query(
    `SELECT count(*)::bigint AS remap_count FROM _merge_remap_keywords`
  );
  if (Number(remap_count) !== sourceTotal) {
    throw new Error(
      `remap mismatch: staged ${sourceTotal} source rows, wrote ${remap_count} remap rows`
    );
  }

  // No remap row may reference a missing canonical id.
  const { rows: [{ orphans }] } = await targetClient.query(`
    SELECT count(*)::bigint AS orphans
      FROM _merge_remap_keywords r
      LEFT JOIN public.keywords k ON k.id = r.new_id
     WHERE k.id IS NULL
  `);
  if (Number(orphans) !== 0) {
    throw new Error(`${orphans} remap rows point at missing keyword ids`);
  }
}

export const phase02 = {
  id: 2,
  name: 'Keywords (with dedup)',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase02');

    if (await remapExists(targetClient, 'keywords')) {
      const count = await remapRowCount(targetClient, 'keywords');
      if (count > 0) {
        log.info(`_merge_remap_keywords already populated (${count} rows) — skipping.`);
        return { skipped: true, remap_rows: count };
      }
      log.warn('_merge_remap_keywords exists but empty — rerunning the phase.');
      await targetClient.query(`DROP TABLE _merge_remap_keywords`);
    }

    await createRemapTable(targetClient, 'keywords', {
      oldIdType: 'integer',
      newIdType: 'integer',
    });

    const sourceTotal = await buildStaging(targetClient, sourceClients, log);
    log.info(`total source keywords staged: ${sourceTotal}`);

    await computeCanonical(targetClient);

    const canonical = await insertCanonicalKeywords(targetClient);
    log.info(`canonical keywords inserted: ${canonical}`);

    const remap = await populateRemap(targetClient);
    log.info(`_merge_remap_keywords rows: ${remap}`);

    const conflicts = await logConflicts(targetClient);
    log.info(`keyword_dedup_conflicts rows: ${conflicts}`);

    await verifyIntegrity(targetClient, sourceTotal);

    return {
      source_rows: sourceTotal,
      canonical_rows: canonical,
      remap_rows: remap,
      conflict_rows: conflicts,
    };
  },
};
