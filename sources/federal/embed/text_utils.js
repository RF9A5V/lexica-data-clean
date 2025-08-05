// Text processing utilities for embedding pipeline

import fs from 'fs/promises';
import path from 'path';

export async function* iterSectionTextFiles(PARSED_DIR) {
  const dir = await fs.opendir(PARSED_DIR);
  for await (const dirent of dir) {
    if (dirent.isDirectory()) {
      const sectionTextFile = path.join(PARSED_DIR, dirent.name, 'section_text.ndjson');
      try {
        await fs.access(sectionTextFile);
        yield { title: dirent.name, sectionTextFile };
      } catch {}
    }
  }
}

const STOP_WORDS = new Set([
  "the", "and", "of", "to", "in", "a", "for", "is", "on", "that", "by", "with", "as", "at", "from", "or", "an", "be", "this", "which", "are", "it", "was", "not", "have", "has", "but", "their", "they", "will", "can", "if", "all", "any", "such", "may", "shall"
]);

export function removeStopWords(text) {
  return text
    .split(/\s+/)
    .filter(word => !STOP_WORDS.has(word.toLowerCase()))
    .join(" ");
}

export function splitTextToChunks(text, chunkSize = 512, stride = 256) {
  // Sliding window chunking with overlap
  const words = text.split(/\s+/);
  const chunks = [];
  for (let i = 0; i < words.length; i += stride) {
    chunks.push(words.slice(i, i + chunkSize).join(' '));
    if (i + chunkSize >= words.length) break; // Avoid trailing empty chunk
  }
  return chunks;
}
