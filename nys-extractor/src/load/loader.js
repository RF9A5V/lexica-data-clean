import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import crypto from 'crypto';
import pg from 'pg';

const { Pool } = pg;

const CODE_META = {
  code_key: 'nys',
  name: 'New York State Consolidated and Unconsolidated Laws',
  publisher: 'New York State Legislature (via NYSenate Open Legislation API)',
  source_uri: 'https://legislation.nysenate.gov/api/3/laws',
};

export async function loadNdjsonToDatabase(ndjsonPath, dbConfig, { truncate = true, verbose = false } = {}) {
  await ensureDatabaseExists(dbConfig, verbose);
  const pool = new Pool({ ...dbConfig, max: 5, idleTimeoutMillis: 30_000 });

  const counts = { units: 0, textVersions: 0, aliases: 0 };
  try {
    await applySchema(pool, verbose);
    if (truncate) await truncateAll(pool, verbose);

    const buffers = await streamNdjson(ndjsonPath, verbose);
    if (verbose) console.log(`  parsed: ${buffers.units.length} units, ${buffers.aliases.length} aliases`);

    counts.units = await insertUnits(pool, buffers.units, verbose);
    counts.textVersions = await insertTextVersions(pool, buffers.units, verbose);
    counts.aliases = await insertAliases(pool, buffers.aliases, verbose);
    await seedCodeMeta(pool, verbose);
    await populateUnitSearch(pool, verbose);
    await pool.query('ANALYZE');
  } finally {
    await pool.end();
  }
  return counts;
}

async function ensureDatabaseExists(cfg, verbose) {
  const admin = new Pool({ ...cfg, database: 'postgres', max: 1 });
  try {
    const r = await admin.query('SELECT 1 FROM pg_database WHERE datname = $1', [cfg.database]);
    if (r.rows.length === 0) {
      const safe = cfg.database.replace(/[^a-zA-Z0-9_]/g, '');
      if (verbose) console.log(`  creating database ${safe}`);
      await admin.query(`CREATE DATABASE "${safe}"`);
    }
  } finally {
    await admin.end();
  }
}

async function applySchema(pool, verbose) {
  if (verbose) console.log('  applying schema');
  const here = path.dirname(fileURLToPath(import.meta.url));
  const sql = await fs.readFile(path.resolve(here, '../../sql/schema.sql'), 'utf8');
  await pool.query(sql);
}

async function truncateAll(pool, verbose) {
  if (verbose) console.log('  truncating tables');
  await pool.query(`
    TRUNCATE TABLE
      unit_search, citations, unit_text_versions, unit_aliases, units, code_meta
    RESTART IDENTITY CASCADE;
  `);
}

async function streamNdjson(ndjsonPath, verbose) {
  const buffers = { units: [], aliases: [] };
  const rl = createInterface({
    input: createReadStream(ndjsonPath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  let n = 0;
  for await (const line of rl) {
    n++;
    if (!line.trim()) continue;
    let rec;
    try { rec = JSON.parse(line); } catch { console.warn(`  line ${n}: malformed JSON`); continue; }
    if (rec.type === 'unit') buffers.units.push(rec);
    else if (rec.type === 'alias') buffers.aliases.push(rec);
    else if (verbose) console.warn(`  line ${n}: unknown type ${rec.type}`);
  }
  return buffers;
}

async function insertUnits(pool, units, verbose) {
  if (units.length === 0) return 0;
  if (verbose) console.log(`  inserting ${units.length} units`);

  // Topological sort: depth 0 = no parent_id; depth N+1 = parent at depth N.
  const byId = new Map(units.map((u) => [u.id, u]));
  const depth = new Map();
  const compute = (u) => {
    if (depth.has(u.id)) return depth.get(u.id);
    if (!u.parent_id || !byId.has(u.parent_id)) { depth.set(u.id, 0); return 0; }
    const d = compute(byId.get(u.parent_id)) + 1;
    depth.set(u.id, d);
    return d;
  };
  for (const u of units) compute(u);
  const sorted = units.slice().sort((a, b) => depth.get(a.id) - depth.get(b.id));

  const cols = [
    'id', 'unit_type', 'number', 'label', 'parent_id', 'sort_key', 'citation',
    'canonical_id', 'source_id', 'law_id', 'law_type', 'is_active',
    'repealed_date', 'published_dates', 'active_date',
  ];
  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < sorted.length; i += batchSize) {
    const batch = sorted.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const u of batch) {
      values.push(`(${cols.map(() => `$${p++}`).join(', ')})`);
      params.push(
        u.id,
        u.unit_type || 'other',
        u.number ?? null,
        u.label ?? null,
        byId.has(u.parent_id) ? u.parent_id : null,
        u.sort_key ?? null,
        u.citation ?? null,
        u.canonical_id ?? null,
        'nys',
        u.law_id ?? null,
        u.law_type ?? null,
        u.is_active === false ? false : true,
        u.repealed_date ?? null,
        u.published_dates ?? null,
        u.active_date ?? null,
      );
    }
    const sql = `INSERT INTO units (${cols.join(', ')}) VALUES ${values.join(', ')} ON CONFLICT (id) DO NOTHING`;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }
  return inserted;
}

async function insertTextVersions(pool, units, verbose) {
  const withText = units.filter((u) => u.text_plain && u.text_plain.length > 0);
  if (withText.length === 0) return 0;
  if (verbose) console.log(`  inserting ${withText.length} text versions`);

  const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
  const batchSize = 400;
  let inserted = 0;
  for (let i = 0; i < withText.length; i += batchSize) {
    const batch = withText.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const u of batch) {
      values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
      params.push(
        u.id,
        u.effective_start || '1900-01-01',
        u.effective_end || null,
        u.text_plain,
        sha(u.text_plain),
      );
    }
    const sql = `
      INSERT INTO unit_text_versions (unit_id, effective_start, effective_end, text_plain, checksum)
      VALUES ${values.join(', ')}
    `;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }
  return inserted;
}

async function insertAliases(pool, aliases, verbose) {
  if (aliases.length === 0) return 0;
  if (verbose) console.log(`  inserting ${aliases.length} aliases`);
  const batchSize = 1000;
  let inserted = 0;
  for (let i = 0; i < aliases.length; i += batchSize) {
    const batch = aliases.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const a of batch) {
      values.push(`($${p++}, $${p++})`);
      params.push(a.alias, a.unit_id);
    }
    const sql = `INSERT INTO unit_aliases (alias, unit_id) VALUES ${values.join(', ')} ON CONFLICT (alias, unit_id) DO NOTHING`;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }
  return inserted;
}

async function seedCodeMeta(pool, verbose) {
  if (verbose) console.log('  seeding code_meta');
  await pool.query(`
    INSERT INTO code_meta (id, code_key, name, publisher, source_uri, current_edition_date)
    VALUES (1, $1, $2, $3, $4, CURRENT_DATE)
    ON CONFLICT (id) DO UPDATE SET
      code_key = EXCLUDED.code_key,
      name = EXCLUDED.name,
      publisher = EXCLUDED.publisher,
      source_uri = EXCLUDED.source_uri,
      current_edition_date = EXCLUDED.current_edition_date
  `, [CODE_META.code_key, CODE_META.name, CODE_META.publisher, CODE_META.source_uri]);
}

async function populateUnitSearch(pool, verbose) {
  if (verbose) console.log('  populating unit_search');
  await pool.query(`
    INSERT INTO unit_search (unit_id, heading, text_plain, tsv)
    SELECT
      u.id,
      u.label,
      v.text_plain,
      to_tsvector('english', COALESCE(u.label, '') || ' ' || COALESCE(v.text_plain, ''))
    FROM units u
    LEFT JOIN LATERAL (
      SELECT text_plain
      FROM unit_text_versions tv
      WHERE tv.unit_id = u.id
      ORDER BY effective_start DESC
      LIMIT 1
    ) v ON true
    ON CONFLICT (unit_id) DO UPDATE SET
      heading = EXCLUDED.heading,
      text_plain = EXCLUDED.text_plain,
      tsv = EXCLUDED.tsv,
      updated_at = NOW()
  `);
}
