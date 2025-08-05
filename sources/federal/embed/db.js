// Database connection and utility functions for embedding pipeline
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../.env') });
import { Client } from 'pg';

export async function openPg() {
  const connStr = process.env.EMBEDDING_DB_URL;
  console.log('[DEBUG] Using Postgres connection string:', connStr);
  if (!connStr) throw new Error('No DB connection string set (EMBEDDING_DB_URL)');
  const pg = new Client({ connectionString: connStr });
  await pg.connect();
  return pg;
}

export async function createTables(pg) {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS usc_elements (
      element_id TEXT PRIMARY KEY,
      element_type TEXT,
      heading TEXT,
      original_text TEXT
    );
  `);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS usc_elements_staging (
      element_id TEXT PRIMARY KEY,
      element_type TEXT,
      heading TEXT,
      original_text TEXT
    );
  `);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS usc_chunks (
      id SERIAL PRIMARY KEY,
      element_id TEXT NOT NULL,
      chunk_index INTEGER NOT NULL,
      content_type TEXT NOT NULL,
      heading TEXT,
      original_text TEXT,
      embedding vector(768),
      status TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (element_id, chunk_index, content_type)
    );
  `);
  await pg.query(`
    CREATE TABLE IF NOT EXISTS chunking_status (
      title TEXT PRIMARY KEY,
      completed_at TIMESTAMPTZ DEFAULT NOW(),
      num_chunks INTEGER
    );
  `);
}

// Only create usc_elements table (for element population script)
export async function createElementsTableOnly(pg) {
  await pg.query(`
    CREATE TABLE IF NOT EXISTS usc_elements (
      element_id TEXT PRIMARY KEY,
      element_type TEXT,
      heading TEXT,
      original_text TEXT
    );
  `);
}

export async function insertElementStaging(pg, element_id, element_type, heading, original_text) {
  await pg.query(
    `INSERT INTO usc_elements_staging (element_id, element_type, heading, original_text)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (element_id) DO NOTHING`,
    [element_id, element_type, heading, original_text]
  );
}

export async function insertElement(pg, element_id, element_type, heading, original_text) {
  await pg.query(
    `INSERT INTO usc_elements (element_id, element_type, heading, original_text)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (element_id) DO NOTHING`,
    [element_id, element_type, heading, original_text]
  );
}

export async function getChunksMissingEmbeddings(pg, limit = 100) {
  const res = await pg.query(
    `SELECT element_id, chunk_index, content_type, heading, original_text FROM usc_chunks WHERE embedding IS NULL LIMIT $1`,
    [limit]
  );
  return res.rows;
}

export async function updateChunkEmbeddingsBatch(pg, rows) {
  // rows: [{element_id, chunk_index, content_type, embedding}]
  if (rows.length === 0) return;
  const queries = rows.map((row, i) => `($${i*4+1}, $${i*4+2}, $${i*4+3}, $${i*4+4}::vector)`).join(',');
  const params = rows.flatMap(row => [row.element_id, row.chunk_index, row.content_type, row.embedding]);
  // Use a CTE for batch update
  await pg.query(
    `UPDATE usc_chunks AS c SET embedding = v.embedding
     FROM (VALUES ${queries}) AS v(element_id, chunk_index, content_type, embedding)
     WHERE c.element_id = v.element_id::text AND c.chunk_index = v.chunk_index::integer AND c.content_type = v.content_type::text`,
    params
  );
}

export async function insertChunksBatch(pg, chunkRows) {
  if (chunkRows.length === 0) return;
  const values = [];
  const params = [];
  chunkRows.forEach(([element_id, chunk_index, content_type, heading, original_text, embedding], i) => {
    const base = i * 6;
    values.push(`($${base+1}, $${base+2}, $${base+3}, $${base+4}, $${base+5}, $${base+6}, 'pending')`);
    params.push(element_id, chunk_index, content_type, heading, original_text, embedding);
  });
  await pg.query(
    `INSERT INTO usc_chunks (element_id, chunk_index, content_type, heading, original_text, embedding, status)
     VALUES ${values.join(',')}
     ON CONFLICT (element_id, chunk_index, content_type) DO UPDATE SET heading = EXCLUDED.heading, original_text = EXCLUDED.original_text, embedding = EXCLUDED.embedding, status = EXCLUDED.status`,
    params
  );
}

export async function markChunkingComplete(pg, title, num_chunks) {
  await pg.query(
    `INSERT INTO chunking_status (title, completed_at, num_chunks)
     VALUES ($1, NOW(), $2)
     ON CONFLICT (title) DO UPDATE SET completed_at = NOW(), num_chunks = $2`,
    [title, num_chunks]
  );
}

export async function isChunkingComplete(pg, title) {
  const res = await pg.query('SELECT 1 FROM chunking_status WHERE title = $1', [title]);
  return res.rows.length > 0;
}

export async function resetChunkingStatus(pg) {
  await pg.query('TRUNCATE chunking_status');
  await pg.query('DROP TABLE IF EXISTS usc_chunks');
  await pg.query('DROP TABLE IF EXISTS usc_elements');
  await createTables(pg); // Recreate all tables, including usc_chunks and usc_elements
}


export async function getPendingChunks(pg, limit) {
  const res = await pg.query(
    `SELECT element_id, chunk_index, heading, original_text FROM usc_chunks WHERE status = 'pending' LIMIT $1`,
    [limit]
  );
  return res.rows;
}

export async function updateEmbedding(pg, element_id, chunk_index, embedding, status = 'completed') {
  // Convert embedding array to bracketed string for pgvector
  const vectorStr = `[${embedding.join(',')}]`;
  await pg.query(
    `UPDATE usc_chunks SET embedding = $1, status = $2, updated_at = NOW() WHERE element_id = $3 AND chunk_index = $4`,
    [vectorStr, status, element_id, chunk_index]
  );
  console.log(`[DB WRITE] element_id=${element_id}, chunk_index=${chunk_index}, embedding_sample=${embedding ? embedding.slice(0,5) : embedding}`);
}

export async function insertOrSkipChunk(pg, element_id, chunk_index, heading, original_text) {
  const res = await pg.query(
    'SELECT status FROM usc_chunks WHERE element_id = $1 AND chunk_index = $2',
    [element_id, chunk_index]
  );
  if (res.rows.length && res.rows[0].status === 'completed') return false;
  await pg.query(
    `INSERT INTO usc_chunks (element_id, chunk_index, heading, original_text, status)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (element_id, chunk_index) DO UPDATE SET heading = EXCLUDED.heading, original_text = EXCLUDED.original_text, status = EXCLUDED.status`,
    [element_id, chunk_index, heading, original_text, 'pending']
  );
  return true;
}
