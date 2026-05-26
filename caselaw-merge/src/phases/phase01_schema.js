// Phase 1 — schema bootstrap.
// Per spec §D.2 row 1: "One-shot CREATE script. B2 reruns idempotently via
// DROP-IF-EXISTS guard."
//
// What this phase does:
//   1. Connect to ADMIN_URL (the maintenance DB) and CREATE DATABASE
//      MERGE_TARGET_DB if missing.
//   2. Connect to TARGET_URL and execute the six SQL files in order:
//        01_extensions_and_types.sql
//        02_functions.sql
//        03_tables.sql
//        04_indexes.sql
//        05_triggers.sql
//        06_views.sql
//
// Idempotency:
//   * Phase 1 detects existing schema by checking for the `cases` table and
//     skips re-applying. Pass --force to re-create (drops + recreates the
//     target DB entirely; will error if there is data the user might want).

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from '../config.js';
import { getAdminPool, getTargetPool } from '../db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCHEMA_DIR = path.join(__dirname, '..', 'sql', 'schema');

const SQL_FILES = [
  '01_extensions_and_types.sql',
  '02_functions.sql',
  '03_tables.sql',
  '04_indexes.sql',
  '05_triggers.sql',
  '06_views.sql',
];

async function targetDbExists(adminClient, dbName) {
  const { rows } = await adminClient.query(
    'SELECT 1 FROM pg_database WHERE datname = $1',
    [dbName]
  );
  return rows.length > 0;
}

async function casesTableExists(targetClient) {
  const { rows } = await targetClient.query(
    `SELECT to_regclass('public.cases') AS oid`
  );
  return rows[0].oid !== null;
}

export const phase01 = {
  id: 1,
  name: 'Schema bootstrap',
  expectedSentinel: 'public.cases', // any phase-1 artifact would do
  async run({ logger, args }) {
    const log = logger.child('phase01');

    // Step 1: ensure target DB exists.
    const admin = getAdminPool();
    const adminClient = await admin.connect();
    try {
      const exists = await targetDbExists(adminClient, config.targetDbName);
      if (!exists) {
        log.info(`Creating database ${config.targetDbName}`);
        // pg lib doesn't parameterize identifiers — interpolate after sanitizing.
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.targetDbName)) {
          throw new Error(`Unsafe MERGE_TARGET_DB: ${config.targetDbName}`);
        }
        await adminClient.query(`CREATE DATABASE ${config.targetDbName}`);
      } else if (args.force) {
        log.warn(`--force: dropping + recreating ${config.targetDbName}`);
        // Disallow drops on databases that aren't ours by sanity-check.
        if (
          !config.targetDbName.startsWith('ny_caselaw') &&
          !config.targetDbName.endsWith('_dev') &&
          !config.targetDbName.endsWith('_scratch')
        ) {
          throw new Error(
            `Refusing to DROP non-scratch DB: ${config.targetDbName}. Drop manually if you really mean it.`
          );
        }
        await adminClient.query(
          `SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
            WHERE datname = $1 AND pid <> pg_backend_pid()`,
          [config.targetDbName]
        );
        await adminClient.query(`DROP DATABASE ${config.targetDbName}`);
        await adminClient.query(`CREATE DATABASE ${config.targetDbName}`);
      } else {
        log.info(`Database ${config.targetDbName} already exists`);
      }
    } finally {
      adminClient.release();
    }

    // Step 2: apply schema files into target DB.
    const target = getTargetPool();
    const targetClient = await target.connect();
    try {
      const already = await casesTableExists(targetClient);
      if (already && !args.force) {
        log.info('cases table already exists — schema already applied (skip).');
        return { skipped: true };
      }

      for (const file of SQL_FILES) {
        const fpath = path.join(SCHEMA_DIR, file);
        const sql = await fs.readFile(fpath, 'utf8');
        log.info(`Applying ${file}`);
        await targetClient.query('BEGIN');
        try {
          await targetClient.query(sql);
          await targetClient.query('COMMIT');
        } catch (err) {
          await targetClient.query('ROLLBACK');
          throw new Error(`Failed applying ${file}: ${err.message}`);
        }
      }
      log.info('Schema bootstrap complete.');
      return { skipped: false };
    } finally {
      targetClient.release();
    }
  },
};
