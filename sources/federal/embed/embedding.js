import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

// Embedding server communication for embedding pipeline
import fetch from 'node-fetch';
import { removeStopWords } from './text_utils.js';

const EMBED_SERVER_URL = process.env.EMBED_SERVER_URL;
if (!EMBED_SERVER_URL) {
  throw new Error('EMBED_SERVER_URL is not set in environment variables. Please set it in your .env file.');
}
console.log(`[DEBUG] Using EMBED_SERVER_URL: ${EMBED_SERVER_URL}`);

export async function fetchEmbeddings(texts) {
  const cleanedTexts = texts.map(removeStopWords);
  const response = await fetch(EMBED_SERVER_URL + '/embed_batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ texts: cleanedTexts }),
  });
  if (!response.ok) throw new Error(`Embedding server error: ${response.status}`);
  const result = await response.json();
  if (!result.embeddings || result.embeddings.length !== texts.length) {
    throw new Error('Embedding server returned wrong number of embeddings');
  }
  return result.embeddings;
}

export function toPgvectorString(arr) {
  return `[${arr.join(',')}]`;
}

