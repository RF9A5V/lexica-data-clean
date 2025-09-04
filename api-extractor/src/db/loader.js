/**
 * NYSenate API Extractor Database Loader
 * Loads NDJSON legislative data into PostgreSQL database
 * Adapted from legislative-extractor for NYSenate API data
 */

import fs from 'fs/promises';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import pkg from 'pg';
const { Pool } = pkg;
// Removed reconstitution testing for plain text storage

/**
 * Load NDJSON data into legislative database
 */
export async function loadNdjsonToDatabase(ndjsonFile, dbConfig, options = {}) {
  const { verbose = false, dryRun = false, skipSchemaCreation = false, cacheDir = 'data/cache' } = options;
  const results = { insertedUnits: 0, insertedVersions: 0, insertedCitations: 0, reconstitutionFallbacks: 0 };

  if (dryRun) {
    console.log(`  Would load NDJSON data from ${ndjsonFile} into database`);
    return results;
  }

  // Skip reconstitution testing - store plain text directly

  // Ensure database exists first
  await ensureDatabaseExists(dbConfig, { verbose });

  // Create database connection pool
  const pool = new Pool({
    ...dbConfig,
    max: 10,
    idleTimeoutMillis: 30000
  });

  try {
    // Ensure database schema exists (only if not skipping)
    if (!skipSchemaCreation) {
      await createSchema(pool, { verbose });
    }

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
            source_unit_id: item.source_unit_id || null,
            raw_citation: item.rawText || item.raw_citation,
            target_kind: mapTargetKind(item.targetKind),
            external_curie: item.curie,
            context: item.context
          });
          continue;
        }

        // Handle structural units
        units.push({
          id: item.id,
          unit_type: mapUnitType(item.type),
          number: item.number || null,
          label: item.label || item.heading || null,
          parent_id: item.parent_id || null,
          sort_key: item.sort_key || null,
          citation: item.citation || null,
          canonical_id: item.canonical_id || item.id,
          source_id: 'nysenate',
          law_id: item.law_id || extractLawId(item.id)
        });

        // Create text version if text content exists
        if (item.text || item.text_html) {
          textVersions.push({
            unit_id: item.id,
            text_plain: item.text || null,
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
      console.log(`  Loaded ${results.insertedUnits} units, ${results.insertedVersions} text versions, and ${results.insertedCitations} citations`);
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

  const schemaPath = new URL('../../sql/schema.sql', import.meta.url);
  const schemaSql = await fs.readFile(schemaPath, 'utf-8');

  await pool.query(schemaSql);

  if (verbose) {
    console.log('  Created NYSenate legislative database schema');
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

    // Remove duplicates within the batch to avoid "cannot affect row a second time" error
    const uniqueBatch = [];
    const seenIds = new Set();
    
    for (const unit of batch) {
      if (!seenIds.has(unit.id)) {
        seenIds.add(unit.id);
        uniqueBatch.push(unit);
      }
    }

    if (uniqueBatch.length === 0) continue;

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const unit of uniqueBatch) {
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        unit.id,
        unit.unit_type,
        unit.number,
        unit.label,
        unit.parent_id,
        unit.sort_key,
        unit.citation,
        unit.canonical_id,
        unit.source_id,
        unit.law_id
      );
    }

    const sql = `
      INSERT INTO units (id, unit_type, number, label, parent_id, sort_key, citation, canonical_id, source_id, law_id)
      VALUES ${values.join(', ')}
      ON CONFLICT (id) DO UPDATE SET
        unit_type = EXCLUDED.unit_type,
        number = EXCLUDED.number,
        label = EXCLUDED.label,
        parent_id = EXCLUDED.parent_id,
        sort_key = EXCLUDED.sort_key,
        citation = EXCLUDED.citation,
        canonical_id = EXCLUDED.canonical_id,
        law_id = EXCLUDED.law_id,
        updated_at = NOW()
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

    // Remove duplicates within the batch to avoid "cannot affect row a second time" error
    const uniqueBatch = [];
    const seenKeys = new Set();
    
    for (const version of batch) {
      const key = `${version.unit_id}:${version.effective_start}`;
      if (!seenKeys.has(key)) {
        seenKeys.add(key);
        uniqueBatch.push(version);
      }
    }

    if (uniqueBatch.length === 0) continue;

    const values = [];
    const params = [];
    let paramIndex = 1;

    for (const version of uniqueBatch) {
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        version.unit_id,
        version.effective_start,
        version.effective_end,
        version.text_html,
        version.text_plain,
        await generateChecksum(version.text_plain)
      );
    }

    const sql = `
      INSERT INTO unit_text_versions (unit_id, effective_start, effective_end, text_html, text_plain, checksum)
      VALUES ${values.join(', ')}
      ON CONFLICT (unit_id, effective_start) DO UPDATE SET
        effective_end = EXCLUDED.effective_end,
        text_html = EXCLUDED.text_html,
        text_plain = EXCLUDED.text_plain,
        checksum = EXCLUDED.checksum
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
      values.push(`($${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++}, $${paramIndex++})`);
      params.push(
        citation.source_unit_id,
        citation.raw_citation,
        citation.target_kind,
        citation.external_curie,
        citation.context
      );
    }

    const sql = `
      INSERT INTO citations (source_unit_id, raw_citation, target_kind, external_curie, context)
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

/**
 * Map NYSenate unit types to database enum values
 */
function mapUnitType(type) {
  const typeMap = {
    'law': 'title',
    'article': 'article',
    'section': 'section',
    'subsection': 'subsection',
    'paragraph': 'paragraph',
    'subparagraph': 'subparagraph',
    'subdivision': 'subdivision',
    'clause': 'clause',
    'item': 'item'
  };
  
  return typeMap[type] || 'other';
}

/**
 * Map citation target kinds to database enum values
 */
function mapTargetKind(kind) {
  const kindMap = {
    'statute_code': 'statute_code',
    'regulatory_code': 'regulatory_code',
    'case': 'case',
    'external': 'external'
  };
  
  return kindMap[kind] || 'external';
}

/**
 * Extract law ID from unit ID
 */
function extractLawId(unitId) {
  if (!unitId) return null;
  
  // Extract law ID from patterns like "nysenate:abp:section:1"
  const parts = unitId.split(':');
  if (parts.length >= 2 && parts[0] === 'nysenate') {
    return parts[1].toUpperCase();
  }
  
  return null;
}

/**
 * Extract law ID from NDJSON filename
 */
function extractLawIdFromFilename(ndjsonFile) {
  // Extract from patterns like "nysenate.ABC.ndjson"
  const filename = ndjsonFile.split('/').pop();
  const match = filename.match(/nysenate\.([A-Z]+)\.ndjson$/i);
  return match ? match[1].toUpperCase() : null;
}
