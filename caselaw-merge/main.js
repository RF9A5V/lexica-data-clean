#!/usr/bin/env node
/**
 * Track B / B1 — caselaw-merge orchestrator.
 *
 * Usage:
 *   node main.js --phase=1
 *   node main.js --phase=1 --force         # drop+recreate scratch DB
 *   node main.js --from=1 --to=4           # run a contiguous range
 *   node main.js --only=2,3,4              # run a specific subset
 *   node main.js                           # run all phases sequentially
 *
 * Per spec §D.3, each phase is bracketed by
 *   BEGIN; pg_advisory_xact_lock(<phase_id>); ... COMMIT;
 * and the existence of the phase's `_merge_remap_*` table is the resume
 * marker (no remap table = phase has not been run).
 *
 * Phase 1 (schema bootstrap) is a special case — it has no remap table; its
 * idempotency marker is the presence of `public.cases` in the target DB.
 */

import { parseCliArgs, config } from './src/config.js';
import { logger } from './src/logger.js';
import {
  getAdminPool,
  getTargetPool,
  getSourcePool,
  closeAllPools,
  withTargetTx,
} from './src/db.js';
import { acquirePhaseLock } from './src/advisoryLock.js';
import { PHASES, phaseById } from './src/phases/index.js';
import { SOURCE_REFS } from './src/config.js';

function pickPhases(args) {
  if (args.phase != null) return [phaseById(args.phase)];
  if (args.only) return args.only.map(phaseById);
  if (args.from != null || args.to != null) {
    const lo = args.from ?? 1;
    const hi = args.to   ?? PHASES.length;
    return PHASES.filter((p) => p.id >= lo && p.id <= hi);
  }
  return PHASES;
}

async function getSourceClients() {
  const out = {};
  for (const ref of SOURCE_REFS) {
    out[ref] = await getSourcePool(ref).connect();
  }
  return out;
}

async function releaseSourceClients(clients) {
  for (const c of Object.values(clients)) {
    c.release();
  }
}

async function runPhase(phase, args) {
  const log = logger.child(`phase${String(phase.id).padStart(2, '0')}`);
  log.info(`▶ ${phase.name}`);

  if (phase.id === 1) {
    // Phase 1 runs outside the standard advisory-lock/transaction wrapper
    // because it provisions the target database itself (CREATE DATABASE
    // cannot run inside a transaction).
    return phase.run({ logger, args, config });
  }

  // Phases 2–16: lock + transaction in the target DB.
  const sourceClients = await getSourceClients();
  try {
    return await withTargetTx(async (targetClient) => {
      await acquirePhaseLock(targetClient, phase.id);
      return phase.run({ logger, args, config, sourceClients, targetClient });
    });
  } finally {
    await releaseSourceClients(sourceClients);
  }
}

async function main() {
  let args;
  try {
    args = parseCliArgs();
  } catch (err) {
    logger.error(err.message);
    process.exit(2);
  }

  // Friendly summary up front.
  logger.info('caselaw-merge starting', {
    target_db: config.targetDbName,
    target_url: config.target.replace(/:[^:@/]+@/, ':****@'),
    sources: Object.fromEntries(
      Object.entries(config.source).map(([k, v]) => [k, v.replace(/:[^:@/]+@/, ':****@')])
    ),
    dry_run: args.dryRun,
    force: args.force,
  });

  const phases = pickPhases(args);
  logger.info(`Will run ${phases.length} phase(s): ${phases.map((p) => p.id).join(', ')}`);

  if (args.dryRun) {
    logger.warn('--dry-run set: phases will not be executed.');
    return;
  }

  for (const phase of phases) {
    try {
      const t0 = Date.now();
      const result = await runPhase(phase, args);
      const dt = ((Date.now() - t0) / 1000).toFixed(1);
      logger.info(`✓ phase ${phase.id} (${phase.name}) — ${dt}s`, result ?? {});
    } catch (err) {
      logger.error(`✗ phase ${phase.id} (${phase.name}): ${err.message}`);
      if (err.stack) logger.error(err.stack);
      process.exitCode = 1;
      break;
    }
  }
}

main()
  .catch((err) => {
    logger.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await closeAllPools().catch(() => {});
    // Help any lingering pg client handles drain so node exits cleanly.
    setTimeout(() => process.exit(process.exitCode ?? 0), 50).unref();
  });

// Silence the `getAdminPool` import-but-unused warning in lint tools without
// pulling it into runtime; phase 1 already reaches for it directly.
void getAdminPool;
void getTargetPool;
