import fs from 'fs/promises';

/**
 * Reads an NDJSON file and groups entries by identifier, extracting heading/body.
 * Returns an array of { identifier, type, heading, original_text } objects.
 * @param {string} filePath
 * @returns {Promise<Array<{identifier: string, type: string, heading: string|null, original_text: string|null}>>}
 */
export async function loadElementsFromNdjson(filePath) {
  const elementMap = new Map();
  const file = await fs.open(filePath);
  for await (const line of file.readLines()) {
    if (!line.trim()) continue;
    let entry;
    try {
      entry = JSON.parse(line);
    } catch { continue; }
    if (!elementMap.has(entry.identifier)) {
      elementMap.set(entry.identifier, { type: entry.type, heading: null, original_text: null });
    }
    const elem = elementMap.get(entry.identifier);
    if (entry.contentType === 'heading') {
      elem.heading = entry.content;
    } else if (entry.contentType === 'body') {
      elem.original_text = entry.content;
    }
  }
  await file.close();
  return Array.from(elementMap.entries()).map(([identifier, { type, heading, original_text }]) => ({
    identifier,
    type,
    heading,
    original_text,
  }));
}
