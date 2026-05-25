/**
 * Database operations for case law data
 */

import fs from 'fs/promises';
import { createReadStream } from 'fs';
import readline from 'readline';
import path from 'path';
import pg from 'pg';
import dotenv from 'dotenv';
import { getJurisdiction, getCliArgs, DATABASE_CONFIG, PATHS, EXTRACTION_CONFIG } from './config.js';
// Awkward cross-component import (co-data → co-collection) needed for
// opinion-curie generation. The merged DB enforces opinions.curie NOT NULL,
// so direct INSERTs under --target=ny_caselaw must populate it inline rather
// than deferring to a separate backfillOpinionCuries pass. Legacy ingestion
// is unaffected (opinions.curie nullable in legacy schema).
import { generateOpinionCurie } from '../../../co-collection/src/utils/curieGeneration.js';

// Load environment variables
dotenv.config();

const { Pool } = pg;

/**
 * Create database connection pool.
 *
 * Track B / B3 §1: under --target=ny_caselaw, the per-source DATABASE_CONFIG
 * is bypassed in favor of MERGE_TARGET_URL (the merged ny_caselaw DB). Caller
 * passes `{ target: 'ny_caselaw' }` as the second arg to opt in.
 */
function createPool(config = DATABASE_CONFIG, opts = {}) {
  if (opts.target === 'ny_caselaw') {
    if (!process.env.MERGE_TARGET_URL) {
      throw new Error('createPool: --target=ny_caselaw requires MERGE_TARGET_URL env var.');
    }
    return new Pool({
      connectionString: process.env.MERGE_TARGET_URL,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }
  return new Pool({
    host: config.host,
    port: config.port,
    database: config.database,
    user: config.user,
    password: config.password,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  });
}

/**
 * Create database schema.
 *
 * Under --target=ny_caselaw the merged DB already has its schema (built by
 * `co-data/caselaw-merge/src/phases/phase03_cases.js` with source_ref etc.).
 * Skip the legacy CREATE-TABLE path so we don't accidentally try to apply
 * the legacy schema (which lacks source_ref) on top of it.
 */
async function createSchema(pool, options = {}) {
  const { verbose = false, target = null } = options;

  if (target === 'ny_caselaw') {
    if (verbose) console.log('Skipping createSchema — merged ny_caselaw DB already has its schema.');
    return;
  }

  if (verbose) console.log('Creating database schema...');
  
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Create cases table
    await client.query(`
      CREATE TABLE IF NOT EXISTS cases (
        id BIGINT PRIMARY KEY,
        name TEXT NOT NULL,
        name_abbreviation TEXT,
        decision_date TEXT,
        docket_number TEXT,
        first_page TEXT,
        last_page TEXT,
        file_name TEXT,
        
        -- Court information
        court_name TEXT,
        court_name_abbreviation TEXT,
        court_id INTEGER,
        
        -- Jurisdiction information
        jurisdiction_name TEXT,
        jurisdiction_abbreviation TEXT,
        jurisdiction_id INTEGER,
        
        -- Original case.law ID for cross-database citation resolution
        original_id BIGINT,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create citations table
    await client.query(`
      CREATE TABLE IF NOT EXISTS citations (
        id SERIAL PRIMARY KEY,
        case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        citation_type TEXT,
        cite TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create case_citations (cites_to) table with cited_case_ids column
    await client.query(`
      CREATE TABLE IF NOT EXISTS case_citations (
        id SERIAL PRIMARY KEY,
        case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        cited_case TEXT NOT NULL,
        category TEXT,
        reporter TEXT,
        opinion_index INTEGER,
        pin_cites JSONB,
        cited_case_ids BIGINT[], -- Array of original case IDs referenced by this citation
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create opinions table
    await client.query(`
      CREATE TABLE IF NOT EXISTS opinions (
        id SERIAL PRIMARY KEY,
        case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
        opinion_type TEXT,
        author TEXT,
        text TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create metadata table
    await client.query(`
      CREATE TABLE IF NOT EXISTS metadata (
        id BIGSERIAL PRIMARY KEY,
        type TEXT NOT NULL,
        value TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Create indexes for better performance
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_cases_decision_date ON cases(decision_date);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_cases_court_id ON cases(court_id);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_cases_jurisdiction_id ON cases(jurisdiction_id);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_citations_case_id ON citations(case_id);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_case_citations_case_id ON case_citations(case_id);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_opinions_case_id ON opinions(case_id);
    `);
    
    // Indexes for original_id columns
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_cases_original_id ON cases(original_id);
    `);
    
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_case_citations_cited_case_ids ON case_citations USING GIN(cited_case_ids);
    `);
    
    // Unique constraint/index for metadata type+value
    await client.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_type_value_unique ON metadata(type, value);
    `);
    
    await client.query('COMMIT');
    
    if (verbose) console.log('✅ Database schema created successfully');
    
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Insert metadata key/value pairs into metadata table with uniqueness on (type, value).
 */
async function insertMetadata(pool, metadata, options = {}) {
  const { verbose = false } = options;
  const entries = Object.entries(metadata || {});
  if (!entries.length) return 0;
  const client = await pool.connect();
  let inserted = 0;
  try {
    await client.query('BEGIN');
    for (const [type, rawValue] of entries) {
      const value = typeof rawValue === 'string' ? rawValue : String(rawValue);
      const res = await client.query(
        'INSERT INTO metadata (type, value) VALUES ($1, $2) ON CONFLICT (type, value) DO NOTHING',
        [type, value]
      );
      inserted += res.rowCount || 0;
    }
    await client.query('COMMIT');
    if (verbose) console.log(`  Metadata upsert: ${inserted}/${entries.length} inserted`);
    return inserted;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}

/**
 * Build the canonical official citation from filesystem provenance.
 *
 * The ZIP archive's folder organization (source_id → reporter edition) and
 * filename (volume number) are reliable; CAP's free-text `citations[].cite`
 * string is not (e.g. 2004 cases in ny3d archives sometimes labeled "N.Y.").
 * When all inputs are present we reconstruct `<vol> <reporter> <first_page>`
 * and use it in place of the CAP-provided official cite.
 *
 * Returns null when any input is missing — caller falls back to the CAP cite.
 */
function buildOfficialCite({ reporter, fileVolume, firstPage }) {
  if (!reporter) return null;
  if (!Number.isFinite(fileVolume)) return null;
  const pageNum = parseInt(String(firstPage ?? ''), 10);
  if (!Number.isFinite(pageNum)) return null;
  return `${fileVolume} ${reporter} ${pageNum}`;
}

/**
 * Insert a batch of cases into the database
 */
async function insertCases(pool, cases, options = {}) {
  const {
    verbose = false,
    batchSize = EXTRACTION_CONFIG.batchSize,
    expectedReporter = null,
    sourceId = null,
    target = null,
    sourceRef = null,
  } = options;
  const merged = target === 'ny_caselaw';
  if (merged && !sourceRef) {
    throw new Error('insertCases: --target=ny_caselaw requires options.sourceRef to stamp on cases + opinions.');
  }
  let officialCitesRebuilt = 0;
  let officialCitesFallback = 0;
  
  const client = await pool.connect();
  let totalInserted = 0;
  let totalSkipped = 0;
  
  try {
    for (let i = 0; i < cases.length; i += batchSize) {
      const batch = cases.slice(i, i + batchSize);
      
      await client.query('BEGIN');
      
      try {
        for (const caseData of batch) {
          // Check if case already exists
          const existingCase = await client.query(
            'SELECT id FROM cases WHERE id = $1',
            [caseData.id]
          );
          
          if (existingCase.rows.length > 0) {
            totalSkipped++;
            continue;
          }
          
          // Insert case with original_id. Stamp source_ref under --target=ny_caselaw
          // (merged DB requires source_ref NOT NULL on cases).
          if (merged) {
            await client.query(`
              INSERT INTO cases (
                id, name, name_abbreviation, decision_date, docket_number,
                first_page, last_page, file_name, court_name, court_name_abbreviation,
                court_id, jurisdiction_name, jurisdiction_abbreviation, jurisdiction_id,
                original_id, source_ref
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            `, [
              caseData.id,
              caseData.name,
              caseData.name_abbreviation,
              caseData.decision_date,
              caseData.docket_number,
              caseData.first_page,
              caseData.last_page,
              caseData.file_name,
              caseData.court_name,
              caseData.court_name_abbreviation,
              caseData.court_id,
              caseData.jurisdiction_name,
              caseData.jurisdiction_abbreviation,
              caseData.jurisdiction_id,
              caseData.id,
              sourceRef,
            ]);
          } else {
            await client.query(`
              INSERT INTO cases (
                id, name, name_abbreviation, decision_date, docket_number,
                first_page, last_page, file_name, court_name, court_name_abbreviation,
                court_id, jurisdiction_name, jurisdiction_abbreviation, jurisdiction_id,
                original_id
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            `, [
              caseData.id,
              caseData.name,
              caseData.name_abbreviation,
              caseData.decision_date,
              caseData.docket_number,
              caseData.first_page,
              caseData.last_page,
              caseData.file_name,
              caseData.court_name,
              caseData.court_name_abbreviation,
              caseData.court_id,
              caseData.jurisdiction_name,
              caseData.jurisdiction_abbreviation,
              caseData.jurisdiction_id,
              caseData.id // Store the original case.law ID
            ]);
          }
          
          // Build the authoritative official citation from archive provenance
          // when available, rather than trusting CAP's free-text cite string.
          const fileVolume = caseData._file_volume;
          const reporterForCase = expectedReporter;
          const reconstructedOfficial = buildOfficialCite({
            reporter: reporterForCase,
            fileVolume,
            firstPage: caseData.first_page,
          });

          const incomingCitations = Array.isArray(caseData.citations) ? caseData.citations : [];
          const hasOfficial = incomingCitations.some(c => c.type === 'official');
          let officialOverwritten = false;

          for (const citation of incomingCitations) {
            let citeToInsert = citation.cite;
            if (citation.type === 'official' && reconstructedOfficial && !officialOverwritten) {
              citeToInsert = reconstructedOfficial;
              officialOverwritten = true;
              officialCitesRebuilt++;
            }
            await client.query(`
              INSERT INTO citations (case_id, citation_type, cite)
              VALUES ($1, $2, $3)
            `, [caseData.id, citation.type, citeToInsert]);
          }

          // No official citation from CAP: synthesize one when possible so the
          // loader never produces a case without an official cite.
          if (!hasOfficial && reconstructedOfficial) {
            await client.query(`
              INSERT INTO citations (case_id, citation_type, cite)
              VALUES ($1, 'official', $2)
            `, [caseData.id, reconstructedOfficial]);
            officialCitesRebuilt++;
          } else if (!reconstructedOfficial && hasOfficial) {
            officialCitesFallback++;
          }
          
          // Insert case citations (cites_to) with cited_case_ids. The
          // case_citations table is a legacy construct that doesn't exist in
          // the merged DB (merge phase derives cross-case relations
          // differently). Skip silently under --target=ny_caselaw.
          if (!merged && caseData.cites_to && caseData.cites_to.length > 0) {
            for (const citedCase of caseData.cites_to) {
              await client.query(`
                INSERT INTO case_citations (case_id, cited_case, category, reporter, opinion_index, pin_cites, cited_case_ids)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
              `, [
                caseData.id,
                citedCase.cite,
                citedCase.category,
                citedCase.reporter,
                citedCase.opinion_index,
                JSON.stringify(citedCase.pin_cites || []),
                citedCase.case_ids || [] // Store the array of cited case IDs
              ]);
            }
          }
          
          // Insert opinions. Under merged, also stamp source_ref + populate
          // opinion_index (array position) + opinion curie (op:<caseId>#... —
          // the merged schema has both NOT NULL on curie and a unique index on
          // (case_id, opinion_index)). Legacy ingestion deferred curies to
          // backfillOpinionCuries.js; merged can't because of the NOT NULL.
          if (caseData.opinions && caseData.opinions.length > 0) {
            if (merged) {
              for (let opIdx = 0; opIdx < caseData.opinions.length; opIdx++) {
                const opinion = caseData.opinions[opIdx];
                const curie = generateOpinionCurie(Number(caseData.id), opIdx, {
                  type: opinion.type,
                  author: opinion.author,
                });
                if (!curie) {
                  throw new Error(
                    `Failed to generate opinion curie for case ${caseData.id} opinion #${opIdx} (type=${opinion.type}, author=${opinion.author}). ` +
                    `Under --target=ny_caselaw all opinions need a deterministic curie at INSERT time.`,
                  );
                }
                await client.query(`
                  INSERT INTO opinions (case_id, opinion_type, author, text, source_ref, opinion_index, curie)
                  VALUES ($1, $2, $3, $4, $5, $6, $7)
                `, [caseData.id, opinion.type, opinion.author, opinion.text, sourceRef, opIdx, curie]);
              }
            } else {
              for (const opinion of caseData.opinions) {
                await client.query(`
                  INSERT INTO opinions (case_id, opinion_type, author, text)
                  VALUES ($1, $2, $3, $4)
                `, [caseData.id, opinion.type, opinion.author, opinion.text]);
              }
            }
          }
          
          totalInserted++;
        }
        
        await client.query('COMMIT');
        
        if (verbose) {
          console.log(`  Inserted batch ${Math.floor(i / batchSize) + 1} (${totalInserted} cases total)`);
        }
        
      } catch (error) {
        await client.query('ROLLBACK');
        throw new Error(`Failed to insert batch starting at index ${i}: ${error.message}`);
      }
    }
    
  } finally {
    client.release();
  }

  if (verbose && (officialCitesRebuilt > 0 || officialCitesFallback > 0)) {
    console.log(`  Official citations rebuilt from archive provenance: ${officialCitesRebuilt}` +
      (officialCitesFallback > 0 ? ` (fell back to CAP cite for ${officialCitesFallback})` : ''));
  }

  return {
    inserted: totalInserted,
    skipped: totalSkipped,
    officialCitesRebuilt,
    officialCitesFallback,
  };
}

/**
 * Load cases from JSON file and insert into database
 */
async function loadCasesFromFile(pool, filePath, options = {}) {
  const { verbose = false, batchSize = EXTRACTION_CONFIG.batchSize } = options;
  
  if (verbose) console.log(`Loading cases from: ${path.basename(filePath)}`);
  
  // Peek at the first non-whitespace character to detect format
  let ndjson = true;
  const fh = await fs.open(filePath, 'r');
  try {
    const { buffer, bytesRead } = await fh.read(Buffer.alloc(1024), 0, 1024, 0);
    const startStr = buffer.slice(0, bytesRead).toString('utf8');
    const match = startStr.match(/[^\s]/);
    if (match && match[0] === '[') ndjson = false;
  } finally {
    await fh.close();
  }
  
  if (!ndjson) {
    // Backward-compatible: array JSON file
    const content = await fs.readFile(filePath, 'utf8');
    const cases = JSON.parse(content);
    if (!Array.isArray(cases)) {
      throw new Error('JSON file must contain an array of cases');
    }
    if (verbose) console.log(`Found ${cases.length} cases to load (array JSON)`);
    return await insertCases(pool, cases, options);
  }
  
  // NDJSON streaming path
  if (verbose) console.log('Detected NDJSON format; streaming records...');
  const rl = readline.createInterface({
    input: createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity
  });
  
  let batch = [];
  let totalInserted = 0;
  let totalSkipped = 0;
  let totalRebuilt = 0;
  let totalFallback = 0;
  let lineNo = 0;

  const runBatch = async (b) => {
    const result = await insertCases(pool, b, options);
    totalInserted += result.inserted;
    totalSkipped += result.skipped;
    totalRebuilt += result.officialCitesRebuilt || 0;
    totalFallback += result.officialCitesFallback || 0;
  };

  for await (const line of rl) {
    lineNo++;
    const trimmed = line.trim();
    if (!trimmed) continue;
    let obj;
    try {
      obj = JSON.parse(trimmed);
    } catch (e) {
      if (verbose) console.error(`  Skipping invalid JSON on line ${lineNo}: ${e.message}`);
      continue;
    }
    batch.push(obj);
    if (batch.length >= batchSize) {
      await runBatch(batch);
      batch = [];
      if (verbose) console.log(`  Inserted ${totalInserted} so far...`);
    }
  }

  if (batch.length > 0) {
    await runBatch(batch);
  }

  if (verbose) console.log(`Loaded NDJSON: inserted=${totalInserted}, skipped=${totalSkipped}, official_rebuilt=${totalRebuilt}, official_fallback=${totalFallback}`);
  return {
    inserted: totalInserted,
    skipped: totalSkipped,
    officialCitesRebuilt: totalRebuilt,
    officialCitesFallback: totalFallback,
  };
}

/**
 * Get database statistics.
 *
 * Under merged target, the case_citations table doesn't exist (legacy-only),
 * so that stat is omitted. Caller passes `{ target: 'ny_caselaw' }` to opt
 * into the merged stat shape.
 */
async function getDatabaseStats(pool, options = {}) {
  const { target = null } = options;
  const merged = target === 'ny_caselaw';
  const client = await pool.connect();

  try {
    const stats = {};

    // Count cases
    const casesResult = await client.query('SELECT COUNT(*) as count FROM cases');
    stats.cases = parseInt(casesResult.rows[0].count);

    // Count citations
    const citationsResult = await client.query('SELECT COUNT(*) as count FROM citations');
    stats.citations = parseInt(citationsResult.rows[0].count);

    // Count case citations (legacy table — merged DB lacks it)
    if (!merged) {
      const caseCitationsResult = await client.query('SELECT COUNT(*) as count FROM case_citations');
      stats.case_citations = parseInt(caseCitationsResult.rows[0].count);
    }

    // Count opinions
    const opinionsResult = await client.query('SELECT COUNT(*) as count FROM opinions');
    stats.opinions = parseInt(opinionsResult.rows[0].count);
    
    // Get date range
    const dateRangeResult = await client.query(`
      SELECT 
        MIN(decision_date) as earliest_date,
        MAX(decision_date) as latest_date
      FROM cases 
      WHERE decision_date IS NOT NULL AND decision_date != ''
    `);
    
    if (dateRangeResult.rows[0].earliest_date) {
      stats.date_range = {
        earliest: dateRangeResult.rows[0].earliest_date,
        latest: dateRangeResult.rows[0].latest_date
      };
    }
    
    // Get top courts
    const courtsResult = await client.query(`
      SELECT court_name, COUNT(*) as case_count
      FROM cases 
      WHERE court_name IS NOT NULL
      GROUP BY court_name
      ORDER BY case_count DESC
      LIMIT 10
    `);
    
    stats.top_courts = courtsResult.rows;
    
    return stats;
    
  } finally {
    client.release();
  }
}

/**
 * Main database function
 */
async function main() {
  let pool;
  
  try {
    const { jurisdiction, verbose, dryRun } = getCliArgs();
    const jurisdictionConfig = getJurisdiction(jurisdiction);
    
    console.log(`Loading case data for ${jurisdictionConfig.name} (${jurisdiction.toUpperCase()}) into database`);
    
    if (dryRun) {
      console.log('Dry run - database not modified');
      return;
    }
    
    // Create database connection
    pool = createPool();
    
    // Test connection
    const client = await pool.connect();
    await client.query('SELECT NOW()');
    client.release();
    
    if (verbose) console.log('✅ Database connection established');
    
    // Create schema
    await createSchema(pool, { verbose });
    
    // Load data
    const dataFile = path.join(PATHS.processed, `${jurisdiction}-cases.json`);
    
    try {
      await fs.access(dataFile);
    } catch {
      throw new Error(`Data file not found: ${dataFile}. Run extractor first.`);
    }
    
    const startTime = Date.now();
    const result = await loadCasesFromFile(pool, dataFile, { verbose });
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    // Get final statistics
    const stats = await getDatabaseStats(pool);
    
    console.log('\n📊 Database Loading Summary:');
    console.log(`  Cases inserted: ${result.inserted}`);
    console.log(`  Cases skipped: ${result.skipped}`);
    console.log(`  Duration: ${duration}s`);
    
    console.log('\n📈 Database Statistics:');
    console.log(`  Total cases: ${stats.cases}`);
    console.log(`  Total citations: ${stats.citations}`);
    console.log(`  Total case citations: ${stats.case_citations}`);
    console.log(`  Total opinions: ${stats.opinions}`);
    
    if (stats.date_range) {
      console.log(`  Date range: ${stats.date_range.earliest} to ${stats.date_range.latest}`);
    }
    
    if (stats.top_courts.length > 0) {
      console.log('\n🏛️  Top Courts:');
      stats.top_courts.forEach((court, index) => {
        console.log(`  ${index + 1}. ${court.court_name}: ${court.case_count} cases`);
      });
    }
    
    console.log('✅ Database loading completed successfully!');
    
  } catch (error) {
    console.error('❌ Database loading failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  } finally {
    if (pool) {
      await pool.end();
    }
  }
}

/**
 * Apply schema migration to add original_id columns
 */
async function migrateOriginalIds(pool, options = {}) {
  const { verbose = false } = options;
  
  if (verbose) console.log('Applying original_id schema migration...');
  
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Read and execute migration SQL
    const migrationSQL = await fs.readFile(
      path.join(path.dirname(import.meta.url.replace('file://', '')), '..', 'sql', 'migrate_original_ids.sql'),
      'utf8'
    );
    
    await client.query(migrationSQL);
    
    await client.query('COMMIT');
    
    if (verbose) console.log('✅ Original ID migration completed successfully');
    
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Upsert original_id for existing cases by matching case names
 * This is used to backfill original IDs for cases that were imported before the migration
 */
async function upsertOriginalIds(pool, cases, sourceConfig, options = {}) {
  const { verbose = false, dryRun = false } = options;
  
  if (verbose) console.log(`Upserting original IDs for ${cases.length} cases from source ${sourceConfig.id}...`);
  
  const client = await pool.connect();
  let totalUpdated = 0;
  let totalSkipped = 0;
  
  try {
    await client.query('BEGIN');
    
    for (const caseData of cases) {
      // Find existing case by name (safer than citation matching)
      const existingCase = await client.query(
        'SELECT id, original_id FROM cases WHERE name = $1',
        [caseData.name]
      );
      
      if (existingCase.rows.length === 0) {
        totalSkipped++;
        continue;
      }
      
      const dbCase = existingCase.rows[0];
      
      // Skip if original_id is already set
      if (dbCase.original_id !== null) {
        totalSkipped++;
        continue;
      }
      
      if (!dryRun) {
        // Update the original_id
        await client.query(
          'UPDATE cases SET original_id = $1 WHERE id = $2',
          [caseData.id, dbCase.id]
        );
      }
      
      totalUpdated++;
      
      if (verbose && totalUpdated % 100 === 0) {
        console.log(`  Updated ${totalUpdated} original IDs so far...`);
      }
    }
    
    if (!dryRun) {
      await client.query('COMMIT');
    } else {
      await client.query('ROLLBACK');
    }
    
    if (verbose) {
      const action = dryRun ? 'Would update' : 'Updated';
      console.log(`${action} ${totalUpdated} original IDs, skipped ${totalSkipped}`);
    }
    
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
  
  return { updated: totalUpdated, skipped: totalSkipped };
}

// Export functions for use in other modules
export { createPool, createSchema, insertCases, loadCasesFromFile, getDatabaseStats, insertMetadata, migrateOriginalIds, upsertOriginalIds };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
