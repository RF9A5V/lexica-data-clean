// Phase 16 — final integrity validation.
// Spec §D.2 row 16 + §D.4.
//
// The view (v_case_preferred_citation) was created in phase 1 / 06_views.sql,
// so this phase is validation-only. On success, drops every `_merge_remap_*`
// table.
//
// Validation suite (each check raises on failure unless marked WARN):
//   A. Per-table row counts + per-source breakdown (informational).
//   B. Audit-E re-run on merged DB — every FK / curie join must have 0 orphans.
//   C. CURIE round-trip sample — pick N random merged cases, verify their
//      curie matches the source case at (source_ref, remap.old_id).
//   D. opinion_citations.opinion_curie resolution rate (1000-row sample,
//      expect 100%).
//   E. Doctrine-anchor sanity: merged count ≤ sum(per-source-remap) AND
//      ≥ max(per-source-remap rows from any single source).
//   F. keyword_dedup_conflicts pending count (informational — curator review).
//   G. v_case_preferred_citation returns rows.
//
// On all-PASS: DROP every `_merge_remap_*` table. Subsequent re-runs will
// still validate (read-only); the drop is the only side effect and is
// idempotent (DROP IF EXISTS).

import { SOURCE_REFS } from '../config.js';
import { dropAllRemapTables, remapExists, remapRowCount } from '../remap.js';

const CHECK = '✓';
const FAIL = '✗';

async function summarizeCounts(client, log) {
  const tables = [
    'cases', 'case_captions', 'case_notes',
    'opinions', 'opinion_footnotes', 'opinion_holdings',
    'opinion_keywords', 'opinion_negative_treatments', 'opinion_citations',
    'citations', 'citation_corrections',
    'keywords', 'keyword_dedup_conflicts', 'keyword_relations',
    'doctrine_anchors', 'doctrine_case_classifications',
    'appellate_history_case_status', 'appellate_history_connections', 'appellate_history_resolution_queue',
    'analysis_runs', 'batch_jobs', 'batch_opinion_requests',
  ];
  log.info('────────────── Merged DB row counts ──────────────');
  for (const t of tables) {
    const { rows: [{ c }] } = await client.query(`SELECT count(*)::bigint AS c FROM public.${t}`);
    log.info(`  ${t.padEnd(40)} ${String(c).padStart(10)}`);
  }
  log.info('──────────────────────────────────────────────────');
}

async function auditE(client, log) {
  // Re-run Audit-E equivalent on the merged DB. Every orphan check must be 0.
  const checks = [
    // case_captions.case_id → cases
    { name: 'case_captions.case_id → cases',
      sql: `SELECT count(*) AS c FROM case_captions x LEFT JOIN cases y ON y.id = x.case_id WHERE y.id IS NULL` },
    // case_notes.case_id (nullable) → cases when non-null
    { name: 'case_notes.case_id → cases (where non-null)',
      sql: `SELECT count(*) AS c FROM case_notes x WHERE x.case_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM cases y WHERE y.id = x.case_id)` },
    // opinions.case_id → cases
    { name: 'opinions.case_id → cases',
      sql: `SELECT count(*) AS c FROM opinions x LEFT JOIN cases y ON y.id = x.case_id WHERE y.id IS NULL` },
    // opinion_footnotes.opinion_curie → opinions
    { name: 'opinion_footnotes.opinion_curie → opinions',
      sql: `SELECT count(*) AS c FROM opinion_footnotes x LEFT JOIN opinions y ON y.curie = x.opinion_curie WHERE y.curie IS NULL` },
    { name: 'opinion_holdings.opinion_curie → opinions',
      sql: `SELECT count(*) AS c FROM opinion_holdings x LEFT JOIN opinions y ON y.curie = x.opinion_curie WHERE y.curie IS NULL` },
    { name: 'opinion_keywords.opinion_curie → opinions',
      sql: `SELECT count(*) AS c FROM opinion_keywords x LEFT JOIN opinions y ON y.curie = x.opinion_curie WHERE y.curie IS NULL` },
    { name: 'opinion_keywords.keyword_id → keywords',
      sql: `SELECT count(*) AS c FROM opinion_keywords x LEFT JOIN keywords y ON y.id = x.keyword_id WHERE y.id IS NULL` },
    { name: 'opinion_negative_treatments.opinion_curie → opinions',
      sql: `SELECT count(*) AS c FROM opinion_negative_treatments x LEFT JOIN opinions y ON y.curie = x.opinion_curie WHERE y.curie IS NULL` },
    { name: 'opinion_citations.opinion_curie → opinions',
      sql: `SELECT count(*) AS c FROM opinion_citations x LEFT JOIN opinions y ON y.curie = x.opinion_curie WHERE y.curie IS NULL` },
    { name: 'citations.case_id → cases',
      sql: `SELECT count(*) AS c FROM citations x LEFT JOIN cases y ON y.id = x.case_id WHERE y.id IS NULL` },
    { name: 'citation_corrections.case_id → cases (where non-null)',
      sql: `SELECT count(*) AS c FROM citation_corrections x WHERE x.case_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM cases y WHERE y.id = x.case_id)` },
    { name: 'citation_corrections.citation_id → citations (where non-null)',
      sql: `SELECT count(*) AS c FROM citation_corrections x WHERE x.citation_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM citations y WHERE y.id = x.citation_id)` },
    { name: 'doctrine_anchors.doctrine_keyword_id → keywords (tier=doctrine)',
      sql: `SELECT count(*) AS c FROM doctrine_anchors x JOIN keywords y ON y.id = x.doctrine_keyword_id WHERE y.tier <> 'doctrine'` },
    { name: 'doctrine_anchors.field_of_law_keyword_id → keywords (tier=field_of_law)',
      sql: `SELECT count(*) AS c FROM doctrine_anchors x JOIN keywords y ON y.id = x.field_of_law_keyword_id WHERE x.field_of_law_keyword_id IS NOT NULL AND y.tier <> 'field_of_law'` },
    { name: 'appellate_history_case_status.case_id → cases',
      sql: `SELECT count(*) AS c FROM appellate_history_case_status x LEFT JOIN cases y ON y.id = x.case_id WHERE y.id IS NULL` },
    { name: 'appellate_history_connections.citing_case_id → cases',
      sql: `SELECT count(*) AS c FROM appellate_history_connections x LEFT JOIN cases y ON y.id = x.citing_case_id WHERE y.id IS NULL` },
    { name: 'appellate_history_connections.source_case_id → cases at source_case_source_ref',
      sql: `SELECT count(*) AS c FROM appellate_history_connections x LEFT JOIN cases y ON y.id = x.source_case_id AND y.source_ref = x.source_case_source_ref WHERE y.id IS NULL` },
    { name: 'appellate_history_connections.target_case_id → cases at target_case_source_ref',
      sql: `SELECT count(*) AS c FROM appellate_history_connections x LEFT JOIN cases y ON y.id = x.target_case_id AND y.source_ref = x.target_case_source_ref WHERE y.id IS NULL` },
    { name: 'appellate_history_resolution_queue.citing_case_id → cases',
      sql: `SELECT count(*) AS c FROM appellate_history_resolution_queue x LEFT JOIN cases y ON y.id = x.citing_case_id WHERE y.id IS NULL` },
    { name: 'batch_opinion_requests.batch_job_id → batch_jobs',
      sql: `SELECT count(*) AS c FROM batch_opinion_requests x LEFT JOIN batch_jobs y ON y.id = x.batch_job_id WHERE y.id IS NULL` },
  ];
  const failures = [];
  log.info('────────────── Audit E (orphans must all = 0) ──────────────');
  for (const { name, sql } of checks) {
    const { rows: [{ c }] } = await client.query(sql);
    const n = Number(c);
    if (n === 0) {
      log.info(`  ${CHECK}  ${name}: 0`);
    } else {
      log.error(`  ${FAIL}  ${name}: ${n}`);
      failures.push({ name, orphans: n });
    }
  }
  log.info('────────────────────────────────────────────────────────────');
  return failures;
}

async function curieRoundtrip(client, sourceClients, log, sampleSize = 200) {
  if (!(await remapExists(client, 'cases'))) {
    log.info('skipping CURIE round-trip (remap tables already dropped)');
    return { skipped: true };
  }
  // Sample N remap rows. For each, fetch merged.cases.curie + source.cases.curie.
  // They must match.
  const { rows: samples } = await client.query(`
    SELECT source_ref, old_id, new_id
      FROM _merge_remap_cases
     ORDER BY random()
     LIMIT $1
  `, [sampleSize]);

  let mismatches = 0;
  // Group by source for batched lookup.
  const bySource = { ny_supreme: [], ny_appellate: [], ny_trial: [] };
  for (const s of samples) bySource[s.source_ref].push(s);

  for (const ref of SOURCE_REFS) {
    if (bySource[ref].length === 0) continue;
    const oldIds = bySource[ref].map((s) => s.old_id);
    const { rows: sourceRows } = await sourceClients[ref].query(
      `SELECT id, curie FROM cases WHERE id = ANY($1::bigint[])`,
      [oldIds]
    );
    const sourceByOld = new Map(sourceRows.map((r) => [Number(r.id), r.curie]));
    for (const s of bySource[ref]) {
      const { rows: [m] } = await client.query(
        `SELECT curie FROM cases WHERE id = $1`,
        [s.new_id]
      );
      const sourceCurie = sourceByOld.get(Number(s.old_id));
      if (m.curie !== sourceCurie) {
        log.error(`CURIE mismatch: ${ref}/${s.old_id} → merged.id=${s.new_id} curie=${m.curie} vs source=${sourceCurie}`);
        mismatches += 1;
      }
    }
  }
  log.info(`CURIE round-trip: ${sampleSize} samples, ${mismatches} mismatches`);
  return { sampleSize, mismatches };
}

async function opinionCitationResolution(client, log, sampleSize = 1000) {
  // §D.4 query: 1000-row sample, expect 0 unresolved.
  const { rows: [{ unres }] } = await client.query(`
    SELECT count(*) FILTER (WHERE o.curie IS NULL)::bigint AS unres
      FROM (
        SELECT opinion_curie FROM opinion_citations
         ORDER BY random()
         LIMIT $1
      ) oc
      LEFT JOIN opinions o ON o.curie = oc.opinion_curie
  `, [sampleSize]);
  log.info(`opinion_citations resolution: ${sampleSize}-row sample, ${Number(unres)} unresolved`);
  return Number(unres);
}

async function doctrineAnchorSanity(client, log) {
  // §A.4 deferred check: merged anchor count must be ≤ sum of per-source anchors
  // (D-4 collapses; never adds) and ≥ max(per-source) (always preserves the
  // largest source's anchors at minimum).
  if (!(await remapExists(client, 'doctrine_anchors'))) {
    log.info('skipping doctrine_anchor sanity (remap tables dropped)');
    return { skipped: true };
  }
  const { rows: [{ merged_count, source_sum, source_max }] } = await client.query(`
    SELECT
      (SELECT count(*) FROM doctrine_anchors)::int AS merged_count,
      (SELECT count(*) FROM _merge_remap_doctrine_anchors)::int AS source_sum,
      (SELECT max(c)::int FROM (SELECT source_ref, count(*) AS c FROM _merge_remap_doctrine_anchors GROUP BY 1) t) AS source_max
  `);
  const ok = merged_count <= source_sum && merged_count >= source_max;
  log.info(
    `doctrine_anchors sanity: merged=${merged_count} ` +
      `source_sum=${source_sum} source_max=${source_max} ` +
      (ok ? CHECK : FAIL) +
      (ok ? '' : ' VIOLATION')
  );
  return { merged_count, source_sum, source_max, pass: ok };
}

async function keywordConflictsReport(client, log) {
  const { rows: [{ total, pending }] } = await client.query(`
    SELECT count(*)::int AS total,
           count(*) FILTER (WHERE curator_decision IS NULL)::int AS pending
      FROM keyword_dedup_conflicts
  `);
  log.info(`keyword_dedup_conflicts: ${total} total, ${pending} pending curator review`);
  return { total, pending };
}

async function viewSmokeTest(client, log) {
  const { rows: [{ c }] } = await client.query(
    `SELECT count(*)::bigint AS c FROM v_case_preferred_citation`
  );
  log.info(`v_case_preferred_citation: ${Number(c)} rows`);
  return Number(c);
}

async function remapTableSummary(client, log) {
  const tables = [
    'keywords', 'cases', 'opinions', 'citations',
    'analysis_runs', 'doctrine_anchors', 'batch_jobs',
  ];
  log.info('────────────── Remap table state ──────────────');
  for (const t of tables) {
    if (await remapExists(client, t)) {
      const c = await remapRowCount(client, t);
      log.info(`  _merge_remap_${t.padEnd(20)} ${String(c).padStart(10)}`);
    } else {
      log.info(`  _merge_remap_${t.padEnd(20)} (dropped)`);
    }
  }
  log.info('───────────────────────────────────────────────');
}

export const phase16 = {
  id: 16,
  name: 'View + final integrity validation',
  async run({ logger, sourceClients, targetClient }) {
    const log = logger.child('phase16');

    await summarizeCounts(targetClient, log);
    await remapTableSummary(targetClient, log);

    const eFailures = await auditE(targetClient, log);
    if (eFailures.length > 0) {
      throw new Error(`Audit E failed: ${eFailures.length} orphan check(s) > 0`);
    }

    const rt = await curieRoundtrip(targetClient, sourceClients, log);
    if (!rt.skipped && rt.mismatches > 0) {
      throw new Error(`CURIE round-trip: ${rt.mismatches} mismatches in ${rt.sampleSize}-sample`);
    }

    const unres = await opinionCitationResolution(targetClient, log);
    if (unres > 0) {
      throw new Error(`opinion_citations resolution: ${unres} unresolved in 1000-sample`);
    }

    const ds = await doctrineAnchorSanity(targetClient, log);
    if (!ds.skipped && !ds.pass) {
      throw new Error(`doctrine_anchors sanity violated`);
    }

    const kc = await keywordConflictsReport(targetClient, log);
    const view = await viewSmokeTest(targetClient, log);

    // All checks passed. Drop remap tables.
    log.info('All validation passed — dropping _merge_remap_* tables');
    await dropAllRemapTables(targetClient);
    log.info('phase 16 complete');

    return {
      audit_e_failures: 0,
      curie_roundtrip: rt,
      opinion_citations_unresolved: unres,
      doctrine_anchor_sanity: ds,
      keyword_dedup_conflicts: kc,
      v_case_preferred_citation_rows: view,
    };
  },
};
