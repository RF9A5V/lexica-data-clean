// Embedding pipeline for all section_text.ndjson entries in data/parsed/title_XX/
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../.env') });
import cliProgress from 'cli-progress';
import fs from 'fs/promises';
import { openPg, createTables, resetChunkingStatus, isChunkingComplete } from './db.js';
import { runEtlForTitle } from './etl.js';
import { fetchEmbeddings } from './embedding.js';
import { iterSectionTextFiles } from './text_utils.js';

const TEST_MODE = process.argv.includes('--test');
const RESET_CHUNKING = process.argv.includes('--reset-chunking');
const EMBED_CHUNKS = process.argv.includes('--embed-chunks');
const PARSED_DIR = path.resolve(__dirname, '../../../data/parsed');
const CHUNK_SIZE = 512;
const CHUNK_STRIDE = 256;

async function main() {
  if (EMBED_CHUNKS) {
    // Embedding mode: update all rows with NULL embedding (delegated to embedding pipeline)
    const { getChunksMissingEmbeddings, updateChunkEmbeddingsBatch } = await import('./db.js');
    const { fetchEmbeddings, toPgvectorString } = await import('./embedding.js');
    let pg = await openPg();
    const pgCount = await pg.query('SELECT COUNT(*) FROM usc_chunks WHERE embedding IS NULL');
    const totalToEmbed = parseInt(pgCount.rows[0].count, 10);
    let totalUpdated = 0;
    const bar = new cliProgress.SingleBar({ format: `[EMBED] |{bar}| {percentage}% | {value}/{total} Chunks` }, cliProgress.Presets.shades_classic);
    bar.start(totalToEmbed, 0);
    const BATCH_SIZE = 100;
    while (true) {
      const rows = await getChunksMissingEmbeddings(pg, BATCH_SIZE);
      if (rows.length === 0) break;
      const texts = rows.map(row => row.original_text);
      let embeddings;
      try {
        embeddings = await fetchEmbeddings(texts);
      } catch (err) {
        console.error('[ERROR] Embedding server failed:', err);
        break;
      }
      const updateRows = rows.map((row, idx) => ({
        element_id: row.element_id,
        chunk_index: row.chunk_index,
        content_type: row.content_type,
        embedding: toPgvectorString(embeddings[idx])
      }));
      await updateChunkEmbeddingsBatch(pg, updateRows);
      totalUpdated += updateRows.length;
      bar.update(totalUpdated);
    }
    bar.stop();
    await pg.end();
    console.log(`[EMBED] Embedding update complete. Total updated: ${totalUpdated}`);
    return;
  }
  let pg;
  if (!TEST_MODE) {
    pg = await openPg();
    await createTables(pg);
    if (RESET_CHUNKING) {
      await resetChunkingStatus(pg);
      console.log('Chunking status reset.');
    }
  }

  for await (const { title, sectionTextFile } of iterSectionTextFiles(PARSED_DIR)) {
    // Check if already chunked
    if (!TEST_MODE && await isChunkingComplete(pg, title)) {
      console.log(`[${title}] Chunking already complete. Skipping.`);
      continue;
    }
    const bar = new cliProgress.SingleBar({ format: `[${title}] |{bar}| {percentage}% | {value}/{total} Chunks` }, cliProgress.Presets.shades_classic);
    await runEtlForTitle(title, sectionTextFile, pg, {
      chunkSize: CHUNK_SIZE,
      chunkStride: CHUNK_STRIDE,
      testMode: TEST_MODE,
      progressBar: bar
    });
  }
  if (!TEST_MODE && pg) await pg.end();
}

main();
