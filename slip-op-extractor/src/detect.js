/**
 * Detect which sub-parser to use for a given input file.
 *
 * Three flavours are recognised:
 *
 *   - 'html-modern' : the `current/3dseries/` URL pattern (post-redesign LRB
 *                     pages with semantic CSS classes — `<div class="case-info">`,
 *                     `<div class="parties">`, etc).
 *   - 'html-legacy' : the older `3dseries/` URL pattern, yellow-table layout
 *                     with `<table bgcolor="#FFFF80">` for metadata.
 *   - 'pdf'         : binary PDF on disk; processed via `pdftotext -layout`.
 *
 * `unknown` indicates we couldn't recognise the file at all — caller logs
 * and skips.
 */

import path from 'path';

/**
 * Inspect the file contents (and extension) to pick a parser.
 *
 * For HTML, we look at:
 *   1. Magic bytes — PDFs start with %PDF.
 *   2. The `saved from url=...` comment — distinguishes the modern URL
 *      pattern (`current/3dseries`) from the legacy pattern (`3dseries`).
 *   3. As a fallback, the presence of `<div class="case-info">` (modern)
 *      vs. `<table ... bgcolor="#FFFF80">` (legacy).
 */
export function detectFormat(filePath, sampleBuffer) {
  if (sampleBuffer.length >= 4 &&
      sampleBuffer[0] === 0x25 && sampleBuffer[1] === 0x50 &&
      sampleBuffer[2] === 0x44 && sampleBuffer[3] === 0x46) {
    return 'pdf';
  }

  const head = sampleBuffer.slice(0, 16384).toString('utf8');
  const savedFromUrl = head.match(/saved from url=\([^)]+\)([^\s>]+)/);
  if (savedFromUrl) {
    const url = savedFromUrl[1];
    if (url.includes('/current/3dseries/')) return 'html-modern';
    if (url.includes('/3dseries/')) return 'html-legacy';
  }

  if (head.includes('class="case-info"')) return 'html-modern';
  if (/<table[^>]*bgcolor="?#FFFF80"?/i.test(head)) return 'html-legacy';

  if (path.extname(filePath).toLowerCase() === '.pdf') return 'pdf';

  return 'unknown';
}
