// chunking.js
// Contains chunking, deduplication, and reindexing logic for embedding pipeline

/**
 * Removes stopwords from text. (Stub: expects actual implementation elsewhere)
 * @param {string} text
 * @returns {string}
 */
export function removeStopWords(text) {
  // This is a stub; actual implementation should be imported from text_utils.js
  return text;
}

/**
 * Splits text into overlapping chunks.
 * @param {string} text
 * @param {number} chunkSize
 * @param {number} chunkStride
 * @returns {string[]}
 */
export function splitTextToChunks(text, chunkSize, chunkStride) {
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    chunks.push(text.slice(i, i + chunkSize));
    if (i + chunkSize >= text.length) break;
    i += chunkStride;
  }
  return chunks;
}

/**
 * Deduplicate chunk rows by (element_id, chunk_index, content_type), allowing different content.
 * @param {Array} chunkRows - [element_id, chunk_index, content_type, heading, original_text]
 * @returns {Array}
 */
export function deduplicateChunks(chunkRows) {
  const dedupedMap = new Map();
  for (const row of chunkRows) {
    const key = `${row[0]}|${row[1]}|${row[2]}`;
    if (!dedupedMap.has(key)) {
      dedupedMap.set(key, [row]);
    } else {
      const arr = dedupedMap.get(key);
      // Check if content matches any existing row for this key
      const exists = arr.some(existing => existing[3] === row[3] && existing[4] === row[4]);
      if (!exists) {
        arr.push(row);
      }
    }
  }
  return Array.from(dedupedMap.values()).flat();
}

/**
 * Reindex chunk indices per (element_id, content_type).
 * @param {Array} chunkRows - [element_id, chunk_index, content_type, heading, original_text]
 * @returns {Array}
 */
export function reindexChunks(chunkRows) {
  const grouped = new Map();
  for (const row of chunkRows) {
    const groupKey = `${row[0]}|${row[2]}`; // element_id|content_type
    if (!grouped.has(groupKey)) grouped.set(groupKey, []);
    grouped.get(groupKey).push(row);
  }
  const reindexedChunkRows = [];
  for (const groupRows of grouped.values()) {
    groupRows.forEach((row, idx) => {
      row[1] = idx; // update chunk_index
      reindexedChunkRows.push(row);
    });
  }
  return reindexedChunkRows;
}
