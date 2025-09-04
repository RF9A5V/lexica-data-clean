/**
 * Statute database loader
 * Loads NDJSON legislative data into per-source PostgreSQL databases
 */

import fs from 'fs/promises';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import pkg from 'pg';
const { Pool } = pkg;

/**
 * Load NDJSON data into legislative database
 */
export async function loadNdjsonToDatabase(source, ndjsonFile, dbConfig, options = {}) {
  const { verbose = false, dryRun = false } = options;
  const results = { insertedUnits: 0, insertedVersions: 0 };

  if (dryRun) {
    console.log(`  Would load NDJSON data from ${ndjsonFile} into database`);
    return results;
  }

  // Ensure database exists first
  await ensureDatabaseExists(dbConfig, { verbose });

  // Create database connection pool
  const pool = new Pool({
    ...dbConfig,
    max: 10,
    idleTimeoutMillis: 30000
  });

  try {
    // Ensure database schema exists
    await createSchema(pool, { verbose });

    // Process NDJSON file
    const fileStream = createReadStream(ndjsonFile, { encoding: 'utf-8' });
    const rl = createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    const units = [];
    const textVersions = [];
    const citations = [];

    for await (const line of rl) {
      if (!line.trim()) continue;

      try {
        const item = JSON.parse(line);

        // Validate item data
        if (!item.id || !item.type) {
          console.warn(`  Skipping invalid item: missing id or type`);
          continue;
        }

        // Handle citations separately
        if (item.type === 'citation') {
          citations.push({
            id: item.id,
            raw_citation: item.rawText,
            target_kind: item.targetKind === 'regulatory_code' ? 'reg_section' : 
                        item.targetKind === 'statute_code' ? 'statute_section' : 'unknown',
            external_curie: item.curie,
            context_snippet: item.context,
            source_unit_id: item.source_unit_id || null,
            source_text_version_id: null
          });
          continue;
        }

        // Handle structural units
        units.push(item);

        // Create text version if text content exists
        if (item.text) {
          textVersions.push({
            unit_id: item.id,
            text_plain: item.text,
            text_html: null,
            effective_start: item.effective_start || '1900-01-01',
            effective_end: item.effective_end || null
          });
        }

      } catch (error) {
        console.warn(`  Error parsing JSON line: ${error.message}`);
      }
    }

    // Insert units in batches
    if (units.length > 0) {
      results.insertedUnits = await insertUnits(pool, units, { verbose });
    }

    // Insert text versions
    if (textVersions.length > 0) {
      results.insertedVersions = await insertTextVersions(pool, textVersions, { verbose });
    }

    // Insert citations
    if (citations.length > 0) {
      results.insertedCitations = await insertCitations(pool, citations, { verbose });
    }

    if (verbose) {
      console.log(`  Loaded ${results.insertedUnits} units, ${results.insertedVersions} text versions, and ${results.insertedCitations || 0} citations`);
    }

  } finally {
    await pool.end();
  }

  return results;
}

/**
 * Create legislative database schema
 */
async function createSchema(pool, options = {}) {
  const { verbose = false } = options;

  const schemaSql = `
    -- Unit types enum
    DO $$ BEGIN
      CREATE TYPE unit_type AS ENUM ('title','subtitle','chapter','subchapter','article','subarticle','part','subpart','section','subsection','paragraph','subparagraph','clause','subclause','item','subitem','appendix','normal_level','heading_level','other');
    EXCEPTION
      WHEN duplicate_object THEN null;
    END $$;

    -- Citation target kinds
    DO $$ BEGIN
      CREATE TYPE citation_target_kind AS ENUM ('statute_section','reg_section','case','unknown');
    EXCEPTION
      WHEN duplicate_object THEN null;
    END $$;

    -- Change action types
    DO $$ BEGIN
      CREATE TYPE change_action AS ENUM ('add','amend','repeal','renumber','reserved');
    EXCEPTION
      WHEN duplicate_object THEN null;
    END $$;

    -- Code metadata table
    CREATE TABLE IF NOT EXISTS code_meta (
      id SMALLINT PRIMARY KEY DEFAULT 1,
      code_key TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      publisher TEXT,
      source_uri TEXT,
      current_edition_date DATE
    );

    -- Units hierarchy table
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

    -- Unit text versions
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

    -- Citations table
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

    -- Change events table
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

    -- Search optimization table
    CREATE TABLE IF NOT EXISTS unit_search (
      unit_id TEXT PRIMARY KEY REFERENCES units(id) ON DELETE CASCADE,
      heading TEXT,
      text_plain TEXT,
      tsv tsvector,
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS units_parent_idx ON units(parent_id);
    CREATE INDEX IF NOT EXISTS units_source_idx ON units(source_id);
    CREATE UNIQUE INDEX IF NOT EXISTS units_unique_path ON units(unit_type, number, parent_id, source_id);
    CREATE INDEX IF NOT EXISTS utv_unit_idx ON unit_text_versions(unit_id, effective_start, effective_end);
    CREATE INDEX IF NOT EXISTS citations_src_idx ON citations(source_unit_id);
    CREATE INDEX IF NOT EXISTS change_events_unit_idx ON change_events(unit_id);
    CREATE INDEX IF NOT EXISTS unit_search_tsv_idx ON unit_search USING GIN(tsv);

    -- Compatibility views for co-collection integration
    CREATE OR REPLACE VIEW compat_units AS
    SELECT
      id,
      parent_id,
      unit_type,
      number,
      label,
      citation,
      sort_key,
      canonical_id,
      is_active
    FROM units;

    CREATE OR REPLACE VIEW compat_current_text AS
    SELECT u.id AS unit_id, v.text_plain, v.text_html
    FROM units u
    JOIN LATERAL (
      SELECT *
      FROM unit_text_versions t
      WHERE t.unit_id = u.id
        AND t.effective_start <= CURRENT_DATE
        AND (t.effective_end IS NULL OR t.effective_end > CURRENT_DATE)
      ORDER BY t.effective_start DESC
      LIMIT 1
    ) v ON true;
  `;

  await pool.query(schemaSql);

  if (verbose) {
    console.log('  Created legislative database schema');
  }
}

/**
 * Insert units into database
 */
async function insertUnits(pool, units, options = {}) {
  const { verbose = false } = options;
  const batchSize = 1000;
  let inserted = 0;

  for (let i = 0; i < units.length; i += batchSize) {
    const batch = units.slice(i, i + batchSize);

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const unit of batch) {
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        unit.id,
        unit.type,
        unit.number || null,
        unit.label || null,
        unit.parent_id || null,
        unit.sort_key || null,
        unit.citation || null,
        unit.canonical_id || null,
        unit.source_id
      );
    }

    const sql = `
      INSERT INTO units (id, unit_type, number, label, parent_id, sort_key, citation, canonical_id, source_id)
      VALUES ${values.join(', ')}
      ON CONFLICT (id) DO NOTHING
    `;

    const result = await pool.query(sql, params);
    inserted += result.rowCount;

    if (verbose) {
      console.log(`  Inserted batch of ${batch.length} units`);
    }
  }

  return inserted;
}

/**
 * Insert text versions into database
 */
async function insertTextVersions(pool, textVersions, options = {}) {
  const { verbose = false } = options;
  const batchSize = 500;
  let inserted = 0;

  for (let i = 0; i < textVersions.length; i += batchSize) {
    const batch = textVersions.slice(i, i + batchSize);

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const version of batch) {
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        version.unit_id,
        version.effective_start,
        version.effective_end,
        version.text_html,
        version.text_plain,
        generateChecksum(version.text_plain)
      );
    }

    const sql = `
      INSERT INTO unit_text_versions (unit_id, effective_start, effective_end, text_html, text_plain, checksum)
      VALUES ${values.join(', ')}
    `;

    const result = await pool.query(sql, params);
    inserted += result.rowCount;

    if (verbose) {
      console.log(`  Inserted batch of ${batch.length} text versions`);
    }
  }

  return inserted;
}

/**
 * Generate checksum for text content
 */
/**
 * Insert citations into database
 */
async function insertCitations(pool, citations, options = {}) {
  const { verbose = false } = options;
  const batchSize = 500;
  let inserted = 0;

  for (let i = 0; i < citations.length; i += batchSize) {
    const batch = citations.slice(i, i + batchSize);

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const citation of batch) {
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        citation.raw_citation,
        citation.target_kind,
        citation.external_curie,
        citation.context_snippet,
        citation.source_unit_id,
        citation.source_text_version_id
      );
    }

    const sql = `
      INSERT INTO citations (raw_citation, target_kind, external_curie, context_snippet, source_unit_id, source_text_version_id)
      VALUES ${values.join(', ')}
    `;

    const result = await pool.query(sql, params);
    inserted += result.rowCount;

    if (verbose) {
      console.log(`  Inserted batch of ${batch.length} citations`);
    }
  }

  return inserted;
}

/**
 * Ensure database exists, create if it doesn't
 */
async function ensureDatabaseExists(dbConfig, options = {}) {
  const { verbose = false } = options;
  
  // Connect to postgres database to check/create target database
  const adminPool = new Pool({
    ...dbConfig,
    database: 'postgres', // Connect to default postgres database
    max: 1
  });

  try {
    // Check if database exists
    const checkResult = await adminPool.query(
      'SELECT 1 FROM pg_database WHERE datname = $1',
      [dbConfig.database]
    );

    if (checkResult.rows.length === 0) {
      if (verbose) {
        console.log(`  Creating database: ${dbConfig.database}`);
      }
      
      // Create database (cannot use parameterized query for database name)
      const dbName = dbConfig.database.replace(/[^a-zA-Z0-9_]/g, '');
      await adminPool.query(`CREATE DATABASE "${dbName}"`);
      
      if (verbose) {
        console.log(`  ✅ Database ${dbConfig.database} created successfully`);
      }
    } else if (verbose) {
      console.log(`  Database ${dbConfig.database} already exists`);
    }
  } finally {
    await adminPool.end();
  }
}

/**
 * Generate checksum for text content
 */
async function generateChecksum(text) {
  const crypto = await import('crypto');
  return crypto.createHash('sha256').update(text || '').digest('hex');
}
