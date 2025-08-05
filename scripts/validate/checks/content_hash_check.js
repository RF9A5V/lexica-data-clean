// Checks for duplicate content across elements by hashing the text content.
// Uses a simple, fast hash (FNV-1a 32-bit) to avoid storing full text.

function fnv1a32(str) {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash.toString(16);
}

// This check is stateful, so we export a factory to create a fresh check for each file.
export function makeContentHashCheck() {
  // Returns a function that takes (allData, idx) and compares prev/next element content
  function contentHashCheck(allData, idx) {
    const data = allData[idx];
    if (!data || !data.content || !data.content.trim()) return null;
    const normalized = data.content.trim().toLowerCase();
    // Minimal skip list for administrative markers
    if (
      normalized === 'transferred' ||
      normalized === 'omitted' ||
      normalized === 'transferred]' ||
      normalized.startsWith('repealed.') ||
      normalized === '[repealed]' ||
      normalized === '[repealed].'
    ) return null;
    // Check previous (only if same type)
    if (idx > 0) {
      const prev = allData[idx - 1];
      if (
        prev &&
        prev.content &&
        prev.content.trim().toLowerCase() === normalized &&
        prev.type === data.type
      ) {
        return `Adjacent duplicate content found with previous element at line ${prev.lineNum}`;
      }
    }
    // Check next (only if same type)
    if (idx < allData.length - 1) {
      const next = allData[idx + 1];
      if (
        next &&
        next.content &&
        next.content.trim().toLowerCase() === normalized &&
        next.type === data.type
      ) {
        return `Adjacent duplicate content found with next element at line ${next.lineNum}`;
      }
    }
    return null;
  }
  return contentHashCheck;
}
