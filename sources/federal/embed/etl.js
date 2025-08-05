// etl.js
// Orchestrates ETL for a single title/file: loads elements, chunks, dedupes, reindexes, inserts into DB

import { loadElementsFromNdjson } from './elementLoader.js';
import { removeStopWords, splitTextToChunks, deduplicateChunks, reindexChunks } from './chunking.js';
import { insertChunksBatch, markChunkingComplete } from './db.js';

/**
 * Runs ETL pipeline for a single title/file
 * @param {string} title - Title (e.g., 'title_01')
 * @param {string} sectionTextFile - Path to NDJSON file
 * @param {object} pg - DB client
 * @param {object} options - { chunkSize, chunkStride, testMode, progressBar }
 */
export async function runEtlForTitle(title, sectionTextFile, pg, options = {}) {
  const {
    chunkSize = 512,
    chunkStride = 256,
    testMode = false,
    progressBar = null,
  } = options;

  // Load grouped elements
  const elements = await loadElementsFromNdjson(sectionTextFile);

  let chunkRows = [];
  let totalChunks = 0;

  for (const element of elements) {
    // Chunk body
    if (element.original_text) {
      const cleaned = removeStopWords(element.original_text);
      const bodyChunks = splitTextToChunks(cleaned, chunkSize, chunkStride);
      for (let i = 0; i < bodyChunks.length; i++) {
        chunkRows.push([element.identifier, i, 'body', element.heading, element.original_text]);
      }
      totalChunks += bodyChunks.length;
    }
    // Chunk heading
    if (element.heading && element.heading.trim()) {
      const headingChunks = splitTextToChunks(removeStopWords(element.heading), chunkSize, chunkStride);
      for (let i = 0; i < headingChunks.length; i++) {
        chunkRows.push([element.identifier, i, 'heading', element.heading, element.heading]);
      }
      totalChunks += headingChunks.length;
    }
  }

  // Deduplicate and reindex
  chunkRows = deduplicateChunks(chunkRows);
  chunkRows = reindexChunks(chunkRows);

  // Insert into DB or print
  if (!testMode) {
    // Batch insert chunks
    const BATCH_SIZE = 100;
    let processedChunks = 0;
    if (progressBar) progressBar.start(totalChunks, 0);
    for (let i = 0; i < chunkRows.length; i += BATCH_SIZE) {
      const batch = chunkRows.slice(i, i + BATCH_SIZE);
      await insertChunksBatch(pg, batch);
      processedChunks += batch.length;
      if (progressBar) progressBar.update(processedChunks);
    }
    if (progressBar) progressBar.stop();
    await markChunkingComplete(pg, title, totalChunks);
  } else {
    // In test mode, just print
    for (const [element_id, chunk_index, content_type, heading, original_text] of chunkRows) {
      console.log(`[TEST] ${element_id} [chunk ${chunk_index}][${content_type}]: ${original_text}`);
    }
  }
}
