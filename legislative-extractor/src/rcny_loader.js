/**
 * RCNY loader — consumes NDJSON from rcny_parser.js and populates
 * rcny_legislative Postgres.
 *
 * Idempotent: truncates all tables on each run by default. Pass --append
 * to skip the truncate (no on-conflict semantics — designed for full reloads).
 *
 * Pipeline:
 *   1. Ensure DB exists
 *   2. Apply schema (additive: canonical_address column, indexes)
 *   3. TRUNCATE (unless --append)
 *   4. Stream NDJSON → buffers
 *   5. Bulk insert units → text_versions → internal_links → external_citations
 *   6. Seed code_meta
 *   7. Populate unit_search (tsvector)
 *   8. ANALYZE
 */

import dotenv from 'dotenv';
import fs from 'fs/promises';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import pg from 'pg';

dotenv.config();

const { Pool } = pg;

const DEFAULT_DB = {
  host: process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PGPORT || '5432', 10),
  database: process.env.PGDATABASE || 'rcny_legislative',
  user: process.env.PGUSER || 'dev',
  password: process.env.PGPASSWORD || 'dev',
};

const CODE_META = {
  code_key: 'rcny',
  name: 'Rules of the City of New York',
  publisher: 'American Legal Publishing',
  source_uri: 'http://files.amlegal.com/pdffiles/NewYorkCity/Rules/XML.zip',
};

const SOURCE_ID = 'rcny';

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

export async function loadRcnyToDatabase(ndjsonPath, dbConfig = DEFAULT_DB, options = {}) {
  const { truncate = true, verbose = false } = options;

  await ensureDatabaseExists(dbConfig, verbose);

  const pool = new Pool({ ...dbConfig, max: 5, idleTimeoutMillis: 30_000 });
  const counts = {
    units: 0,
    textVersions: 0,
    internalLinks: 0,
    internalLinksResolved: 0,
    externalCitations: 0,
  };

  try {
    await applySchema(pool, verbose);
    if (truncate) await truncateAll(pool, verbose);

    const buffers = await streamNdjson(ndjsonPath, verbose);
    if (verbose) {
      console.log(`  parsed: ${buffers.units.length} units, ${buffers.internalLinks.length} internal_links, ${buffers.externalCitations.length} external_citations`);
    }

    counts.units = await insertUnitsTopologically(pool, buffers.units, verbose);
    counts.textVersions = await insertTextVersions(pool, buffers.units, verbose);

    const linkResults = await insertInternalLinks(pool, buffers.internalLinks, verbose);
    counts.internalLinks = linkResults.inserted;
    counts.internalLinksResolved = linkResults.resolved;

    counts.externalCitations = await insertExternalCitations(pool, buffers.externalCitations, verbose);

    await seedCodeMeta(pool, CODE_META, verbose);
    await populateUnitSearch(pool, verbose);

    await pool.query('ANALYZE');
  } finally {
    await pool.end();
  }

  return counts;
}

// ---------------------------------------------------------------------------
// Schema management
// ---------------------------------------------------------------------------

async function ensureDatabaseExists(dbConfig, verbose) {
  const adminPool = new Pool({ ...dbConfig, database: 'postgres', max: 1 });
  try {
    const exists = await adminPool.query('SELECT 1 FROM pg_database WHERE datname = $1', [dbConfig.database]);
    if (exists.rows.length === 0) {
      const safeName = dbConfig.database.replace(/[^a-zA-Z0-9_]/g, '');
      if (verbose) console.log(`  creating database ${safeName}`);
      await adminPool.query(`CREATE DATABASE "${safeName}"`);
    }
  } finally {
    await adminPool.end();
  }
}

async function applySchema(pool, verbose) {
  if (verbose) console.log('  applying schema');
  await pool.query(`
    DO $$ BEGIN
      CREATE TYPE unit_type AS ENUM (
        'title','subtitle','chapter','subchapter','article','subarticle',
        'part','subpart','section','subsection','paragraph','subparagraph',
        'clause','subclause','item','subitem','appendix','normal_level',
        'heading_level','other'
      );
    EXCEPTION WHEN duplicate_object THEN NULL; END $$;

    DO $$ BEGIN
      CREATE TYPE citation_target_kind AS ENUM ('statute_section','reg_section','case','unknown');
    EXCEPTION WHEN duplicate_object THEN NULL; END $$;

    DO $$ BEGIN
      CREATE TYPE change_action AS ENUM ('add','amend','repeal','renumber','reserved');
    EXCEPTION WHEN duplicate_object THEN NULL; END $$;

    CREATE TABLE IF NOT EXISTS code_meta (
      id SMALLINT PRIMARY KEY DEFAULT 1,
      code_key TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      publisher TEXT,
      source_uri TEXT,
      current_edition_date DATE
    );

    CREATE TABLE IF NOT EXISTS units (
      id TEXT PRIMARY KEY,
      unit_type unit_type NOT NULL,
      number TEXT,
      label TEXT,
      parent_id TEXT REFERENCES units(id) ON DELETE CASCADE,
      sort_key TEXT,
      citation TEXT,
      canonical_id TEXT,
      source_id TEXT NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Additive: canonical_address (e.g., "T15C041_41-01") for internal-link resolution.
    ALTER TABLE units ADD COLUMN IF NOT EXISTS canonical_address TEXT;

    CREATE TABLE IF NOT EXISTS unit_text_versions (
      id BIGSERIAL PRIMARY KEY,
      unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
      effective_start DATE NOT NULL,
      effective_end DATE,
      source_doc_date DATE,
      published_at TIMESTAMPTZ DEFAULT NOW(),
      text_html TEXT,
      text_plain TEXT,
      checksum TEXT,
      CONSTRAINT chk_effective_range CHECK (effective_end IS NULL OR effective_end > effective_start)
    );

    CREATE TABLE IF NOT EXISTS citations (
      id BIGSERIAL PRIMARY KEY,
      source_unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
      source_text_version_id BIGINT REFERENCES unit_text_versions(id) ON DELETE SET NULL,
      raw_citation TEXT NOT NULL,
      target_kind citation_target_kind NOT NULL,
      target_unit_id TEXT REFERENCES units(id) ON DELETE SET NULL,
      external_curie TEXT,
      context_snippet TEXT,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS change_events (
      id BIGSERIAL PRIMARY KEY,
      unit_id TEXT REFERENCES units(id),
      action change_action NOT NULL,
      notice_date DATE NOT NULL,
      notice_uri TEXT,
      summary TEXT,
      old_citation TEXT,
      new_citation TEXT,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS unit_search (
      unit_id TEXT PRIMARY KEY REFERENCES units(id) ON DELETE CASCADE,
      heading TEXT,
      text_plain TEXT,
      tsv tsvector,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS units_parent_idx ON units(parent_id);
    CREATE INDEX IF NOT EXISTS units_source_idx ON units(source_id);
    CREATE INDEX IF NOT EXISTS units_canonical_id_idx ON units(canonical_id) WHERE canonical_id IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS units_canonical_address_unique
      ON units(source_id, canonical_address) WHERE canonical_address IS NOT NULL;
    CREATE INDEX IF NOT EXISTS utv_unit_idx ON unit_text_versions(unit_id, effective_start, effective_end);
    CREATE INDEX IF NOT EXISTS citations_src_idx ON citations(source_unit_id);
    CREATE INDEX IF NOT EXISTS citations_target_idx ON citations(target_unit_id) WHERE target_unit_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS citations_external_curie_idx ON citations(external_curie) WHERE external_curie IS NOT NULL;
    CREATE INDEX IF NOT EXISTS unit_search_tsv_idx ON unit_search USING GIN(tsv);

    -- Drop the old over-specified path uniqueness if present (replaced by canonical_address uniqueness).
    DROP INDEX IF EXISTS units_unique_path;
  `);
}

async function truncateAll(pool, verbose) {
  if (verbose) console.log('  truncating tables');
  // Truncate in dependency order; CASCADE handles FKs.
  await pool.query(`
    TRUNCATE TABLE
      unit_search,
      change_events,
      citations,
      unit_text_versions,
      units,
      code_meta
    RESTART IDENTITY CASCADE;
  `);
}

// ---------------------------------------------------------------------------
// NDJSON streaming
// ---------------------------------------------------------------------------

async function streamNdjson(ndjsonPath, verbose) {
  const buffers = { units: [], internalLinks: [], externalCitations: [] };

  const rl = createInterface({
    input: createReadStream(ndjsonPath, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  let lineNum = 0;
  for await (const line of rl) {
    lineNum += 1;
    if (!line.trim()) continue;
    let rec;
    try {
      rec = JSON.parse(line);
    } catch (err) {
      console.warn(`  line ${lineNum}: malformed JSON, skipping`);
      continue;
    }
    if (rec.type === 'unit') buffers.units.push(rec);
    else if (rec.type === 'internal_link') buffers.internalLinks.push(rec);
    else if (rec.type === 'citation') buffers.externalCitations.push(rec);
    else if (verbose) console.warn(`  line ${lineNum}: unknown record type "${rec.type}"`);
  }
  return buffers;
}

// ---------------------------------------------------------------------------
// Inserts
// ---------------------------------------------------------------------------

/**
 * Insert units in topological order (parents before children) so the
 * self-referential FK on parent_id is satisfied. The parser emits in
 * tree-walk order, but we sort defensively in case files are processed out
 * of order or a parent appears after a child.
 */
async function insertUnitsTopologically(pool, units, verbose) {
  if (units.length === 0) return 0;
  if (verbose) console.log(`  inserting ${units.length} units`);

  // BFS layering: depth 0 = no parent_record_id; depth N+1 = parent at depth N.
  const byId = new Map(units.map((u) => [u.record_id, u]));
  const depth = new Map();
  function depthOf(u) {
    if (depth.has(u.record_id)) return depth.get(u.record_id);
    if (!u.parent_record_id || !byId.has(u.parent_record_id)) {
      depth.set(u.record_id, 0);
      return 0;
    }
    const d = depthOf(byId.get(u.parent_record_id)) + 1;
    depth.set(u.record_id, d);
    return d;
  }
  for (const u of units) depthOf(u);
  const sorted = units.slice().sort((a, b) => depth.get(a.record_id) - depth.get(b.record_id));

  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < sorted.length; i += batchSize) {
    const batch = sorted.slice(i, i + batchSize);
    const cols = ['id', 'unit_type', 'number', 'label', 'parent_id', 'sort_key', 'citation', 'canonical_id', 'canonical_address', 'source_id'];
    const values = [];
    const params = [];
    let p = 1;
    for (const u of batch) {
      values.push(`(${cols.map(() => `$${p++}`).join(', ')})`);
      params.push(
        u.record_id,
        u.unit_type,
        u.number ?? null,
        u.label ?? null,
        // Drop dangling parent_id refs (parent missing from input).
        byId.has(u.parent_record_id) ? u.parent_record_id : null,
        u.sort_key || null,
        u.citation ?? null,
        u.canonical_id ?? null,
        u.canonical_address ?? null,
        u.source_id || SOURCE_ID,
      );
    }
    const sql = `INSERT INTO units (${cols.join(', ')}) VALUES ${values.join(', ')}`;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }
  return inserted;
}

async function insertTextVersions(pool, units, verbose) {
  const withText = units.filter((u) => u.text_plain);
  if (withText.length === 0) return 0;
  if (verbose) console.log(`  inserting ${withText.length} text versions`);

  const crypto = await import('crypto');
  const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');

  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < withText.length; i += batchSize) {
    const batch = withText.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const u of batch) {
      values.push(`($${p++}, $${p++}, $${p++}, $${p++}, $${p++})`);
      params.push(u.record_id, '1900-01-01', null, u.text_plain, sha(u.text_plain));
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

/**
 * Insert internal links into citations, resolving target_unit_id by
 * JOIN on canonical_address (set during unit insert).
 */
async function insertInternalLinks(pool, links, verbose) {
  if (links.length === 0) return { inserted: 0, resolved: 0 };
  if (verbose) console.log(`  inserting ${links.length} internal links`);

  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < links.length; i += batchSize) {
    const batch = links.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const link of batch) {
      // Resolve target via subselect on canonical_address.
      values.push(`(
        $${p++}, $${p++}, 'reg_section'::citation_target_kind,
        (SELECT id FROM units WHERE source_id = $${p++} AND canonical_address = $${p++} LIMIT 1),
        $${p++}, $${p++}
      )`);
      params.push(
        link.source_record_id,
        link.raw_text || link.target_destination,
        SOURCE_ID,
        link.target_destination,
        // External curie hint (lets us match this row from cross-DB joins too).
        canonicalAddressToCurie(link.target_destination),
        null, // no context snippet for inline links
      );
    }
    const sql = `
      INSERT INTO citations (source_unit_id, raw_citation, target_kind, target_unit_id, external_curie, context_snippet)
      VALUES ${values.join(', ')}
    `;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }

  const resolvedCount = await pool.query(
    `SELECT COUNT(*)::int AS n FROM citations WHERE target_unit_id IS NOT NULL AND target_kind = 'reg_section'`,
  );
  return { inserted, resolved: resolvedCount.rows[0].n };
}

async function insertExternalCitations(pool, citations, verbose) {
  if (citations.length === 0) return 0;
  if (verbose) console.log(`  inserting ${citations.length} external citations`);

  const batchSize = 500;
  let inserted = 0;
  for (let i = 0; i < citations.length; i += batchSize) {
    const batch = citations.slice(i, i + batchSize);
    const values = [];
    const params = [];
    let p = 1;
    for (const c of batch) {
      values.push(`($${p++}, $${p++}, $${p++}::citation_target_kind, $${p++}, $${p++})`);
      params.push(
        c.source_record_id,
        c.raw_citation,
        c.target_kind,
        c.external_curie ?? null,
        c.context_snippet ?? null,
      );
    }
    const sql = `
      INSERT INTO citations (source_unit_id, raw_citation, target_kind, external_curie, context_snippet)
      VALUES ${values.join(', ')}
    `;
    const r = await pool.query(sql, params);
    inserted += r.rowCount;
  }
  return inserted;
}

async function seedCodeMeta(pool, meta, verbose) {
  if (verbose) console.log('  seeding code_meta');
  await pool.query(
    `
    INSERT INTO code_meta (id, code_key, name, publisher, source_uri, current_edition_date)
    VALUES (1, $1, $2, $3, $4, NULL)
    ON CONFLICT (id) DO UPDATE SET
      code_key = EXCLUDED.code_key,
      name = EXCLUDED.name,
      publisher = EXCLUDED.publisher,
      source_uri = EXCLUDED.source_uri
    `,
    [meta.code_key, meta.name, meta.publisher, meta.source_uri],
  );
}

async function populateUnitSearch(pool, verbose) {
  if (verbose) console.log('  populating unit_search (tsvector)');
  await pool.query(`
    INSERT INTO unit_search (unit_id, heading, text_plain, tsv, updated_at)
    SELECT
      u.id,
      u.label,
      v.text_plain,
      to_tsvector('english',
        COALESCE(u.label, '') || ' ' ||
        COALESCE(u.citation, '') || ' ' ||
        COALESCE(v.text_plain, '')
      ),
      NOW()
    FROM units u
    LEFT JOIN unit_text_versions v ON v.unit_id = u.id
    ON CONFLICT (unit_id) DO UPDATE SET
      heading = EXCLUDED.heading,
      text_plain = EXCLUDED.text_plain,
      tsv = EXCLUDED.tsv,
      updated_at = EXCLUDED.updated_at
  `);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Mirror of rcny_parser.js deriveCanonicalId: turn a canonical_address
 * (T15C041_41-01) into the case-side join CURIE (rcny:15-41-01).
 */
function canonicalAddressToCurie(addr) {
  if (!addr) return null;
  const sec = addr.match(/^T(\d+)C(\d+)_(.+)$/);
  if (sec) return `rcny:${parseInt(sec[1], 10)}-${sec[3]}`;
  const ch = addr.match(/^T(\d+)C(\d+)$/);
  if (ch) return `rcny:c:${parseInt(ch[1], 10)}-${parseInt(ch[2], 10)}`;
  const t = addr.match(/^T(\d+)$/);
  if (t) return `rcny:t:${parseInt(t[1], 10)}`;
  return null;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const ndjsonPath = args.find((a) => a.startsWith('--in='))?.split('=')[1] || './data/processed/rcny-statutes.ndjson';
  const append = args.includes('--append');
  const verbose = args.includes('--verbose') || args.includes('-v');

  const dbConfig = { ...DEFAULT_DB };
  for (const a of args) {
    if (a.startsWith('--db=')) dbConfig.database = a.split('=')[1];
    if (a.startsWith('--host=')) dbConfig.host = a.split('=')[1];
    if (a.startsWith('--port=')) dbConfig.port = parseInt(a.split('=')[1], 10);
    if (a.startsWith('--user=')) dbConfig.user = a.split('=')[1];
    if (a.startsWith('--password=')) dbConfig.password = a.split('=')[1];
  }

  const t0 = Date.now();
  loadRcnyToDatabase(ndjsonPath, dbConfig, { truncate: !append, verbose })
    .then((counts) => {
      const sec = ((Date.now() - t0) / 1000).toFixed(1);
      console.log(`\nLoaded in ${sec}s:`);
      console.log(JSON.stringify(counts, null, 2));
    })
    .catch((err) => {
      console.error('LOAD FAILED:', err);
      process.exit(1);
    });
}
