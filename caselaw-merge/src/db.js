import pg from 'pg';
import { config, SOURCE_REFS } from './config.js';

// Coerce int8 (OID 20) to JS Number on read.
// node-postgres returns int8 as string by default; downstream Number.isInteger
// + arithmetic silently breaks. See feedback_pg_bigint_coerce — 60k cases
// ingested with 0 opinions on 2026-05-04 traced to this exact bug.
//
// Safe here: max row counts we encounter (~940k cases, ~3M extracted_citations
// in adjacent contexts) are well under Number.MAX_SAFE_INTEGER (2^53).
pg.types.setTypeParser(20, (val) => (val === null ? null : Number(val)));

const { Pool } = pg;

const pools = new Map();

function makePool(url) {
  return new Pool({
    connectionString: url,
    max: 4,
    idleTimeoutMillis: 30_000,
    statement_timeout: 0,
  });
}

export function getSourcePool(sourceRef) {
  if (!SOURCE_REFS.includes(sourceRef)) {
    throw new Error(`Unknown sourceRef: ${sourceRef}`);
  }
  const key = `source:${sourceRef}`;
  if (!pools.has(key)) pools.set(key, makePool(config.source[sourceRef]));
  return pools.get(key);
}

export function getTargetPool() {
  if (!pools.has('target')) pools.set('target', makePool(config.target));
  return pools.get('target');
}

export function getAdminPool() {
  if (!pools.has('admin')) pools.set('admin', makePool(config.admin));
  return pools.get('admin');
}

export async function closeAllPools() {
  for (const pool of pools.values()) {
    await pool.end();
  }
  pools.clear();
}

export async function withTargetTx(fn) {
  const pool = getTargetPool();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
