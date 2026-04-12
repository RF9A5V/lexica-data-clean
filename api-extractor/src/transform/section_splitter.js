/**
 * NY Senate Section Splitter
 *
 * Parses a section's plain-text body into a flat list of sub-units
 * (subdivision, paragraph, subparagraph, clause, item) with proper
 * parent-child relationships.
 *
 * Design principles:
 *  - Structural markers are ONLY recognized at the start of a line (after
 *    optional whitespace). Cross-references like "§ 1234(a)(i)" appear
 *    mid-sentence and are never line-starting, so they are never misidentified.
 *  - Hierarchy is inferred by label TYPE rank:
 *      subdivision(1) > paragraph(2) > subparagraph(3) > clause(4) > item(5)
 *    A unit's parent is the nearest preceding unit with a strictly lower rank.
 *  - No token placeholders are inserted into parent text. The database
 *    parent_id relationship carries the hierarchy.
 *  - Continuation lines (no marker at line start) are appended to the
 *    currently active unit's text.
 *
 * Output: Array of unit objects:
 *  {
 *    type:       'subdivision' | 'paragraph' | 'subparagraph' | 'clause' | 'item'
 *    label:      string  — the identifier, e.g. '1', 'a', 'i', 'A', '3'
 *    rank:       number  — hierarchy depth (1=shallowest)
 *    path:       string[] — full label path from section root, e.g. ['1','a','i']
 *    parentPath: string[] — path of the parent unit ([]=section is parent)
 *    text:       string  — trimmed text content of this unit only
 *  }
 *
 * The returned array is ordered by appearance in the original text.
 */

const RANK = {
  subdivision:  1,
  paragraph:    2,
  subparagraph: 3,
  clause:       4,
  item:         5,
};

// Roman numeral labels used for subparagraphs.
// Listed in full to avoid partial matches (e.g. "v" matching inside "vi").
const ROMAN = new Set([
  'i','ii','iii','iv','v','vi','vii','viii','ix','x',
  'xi','xii','xiii','xiv','xv','xvi','xvii','xviii','xix','xx',
]);

/**
 * Detect whether the trimmed start of a line opens a new structural unit.
 * Returns { type, label, markerLength } measured on the TRIMMED line,
 * or null if the line is a continuation.
 *
 * Pattern order matters: subparagraph must be checked before paragraph so
 * that "(i)" is captured as a roman-numeral subparagraph, not a letter paragraph.
 */
function detectMarker(trimmedLine) {
  // Subdivision: "1." "3-a." "12-aaaa." followed by whitespace
  let m = trimmedLine.match(/^(\d+(?:-[a-z]+)*)\.[ \t]+/);
  if (m) {
    return { type: 'subdivision', label: m[1], markerLength: m[0].length };
  }

  // Paren-enclosed labels — check subparagraph (roman numerals) before paragraph
  m = trimmedLine.match(/^\(([a-z]+)\)[ \t]+/);
  if (m) {
    const label = m[1];
    if (ROMAN.has(label)) {
      return { type: 'subparagraph', label, markerLength: m[0].length };
    }
    return { type: 'paragraph', label, markerLength: m[0].length };
  }

  // Uppercase single letter — clause: "(A)"
  m = trimmedLine.match(/^\(([A-Z])\)[ \t]+/);
  if (m) {
    return { type: 'clause', label: m[1], markerLength: m[0].length };
  }

  // Digits in parens — item: "(1)"
  m = trimmedLine.match(/^\((\d+)\)[ \t]+/);
  if (m) {
    return { type: 'item', label: m[1], markerLength: m[0].length };
  }

  return null;
}

/**
 * Split a raw section text into sub-units.
 *
 * @param {string} raw  Full section text (may include the § header line).
 * @returns {{ caption: string, units: Array }}
 *   caption — text before the first structural marker (section intro/caption)
 *   units   — ordered array of sub-unit objects (see module docstring)
 */
export function splitSection(raw) {
  if (!raw || !raw.trim()) {
    return { caption: '', units: [] };
  }

  const text = raw.replace(/\r\n?/g, '\n');
  const lines = text.split('\n');

  const captionLines = [];
  const units = [];

  // Stack tracks the chain of currently-open units by index into `units`.
  // stack[0] is always a sentinel with rank=0 (the section itself).
  const stack = [{ rank: 0, idx: -1 }];

  let inUnits = false;

  for (const line of lines) {
    const trimmed = line.trimStart();
    const marker = detectMarker(trimmed);

    if (marker) {
      inUnits = true;
      const { type, label, markerLength } = marker;
      const rank = RANK[type];
      const firstLineText = trimmed.slice(markerLength).trimEnd();

      // Pop stack until top has a lower rank than the new unit
      while (stack.length > 1 && stack[stack.length - 1].rank >= rank) {
        stack.pop();
      }

      const parentStackEntry = stack[stack.length - 1];
      const parentPath = parentStackEntry.idx === -1
        ? []
        : units[parentStackEntry.idx].path;

      const path = [...parentPath, label];

      const unit = {
        type,
        label,
        rank,
        path,
        parentPath,
        text: firstLineText,
      };

      const idx = units.length;
      units.push(unit);
      stack.push({ rank, idx });

    } else if (!inUnits) {
      // Before any structural marker — this is section caption/intro text
      captionLines.push(line);
    } else {
      // Continuation line — belongs to the innermost open unit
      const top = stack[stack.length - 1];
      if (top.idx >= 0) {
        const current = units[top.idx];
        const appended = trimmed;
        if (appended) {
          current.text = current.text ? current.text + ' ' + appended : appended;
        }
      }
    }
  }

  // Strip the § section header from the caption if present
  const captionRaw = captionLines.join('\n').trim();
  const caption = captionRaw.replace(/^\s*§\s*[\w-]+\.\s*/, '').trim();

  // Trim all unit texts
  for (const u of units) {
    u.text = u.text.trim();
  }

  return { caption, units };
}

/**
 * Build a citation string for a sub-unit given the section citation and the
 * unit's path.
 *
 * Examples:
 *   sectionCitation="ABC § 1234", path=['1']      → "ABC § 1234(1)"
 *   sectionCitation="ABC § 1234", path=['1','a']   → "ABC § 1234(1)(a)"
 *   sectionCitation="ABC § 1234", path=['1','a','i']→ "ABC § 1234(1)(a)(i)"
 */
export function buildSubUnitCitation(sectionCitation, path) {
  if (!sectionCitation || !path || path.length === 0) return sectionCitation || null;
  const suffix = path.map((label, i) => {
    // First component (subdivision) uses numeric parens: (1)
    // Subsequent components (paragraph, subparagraph, etc.) use letter/roman parens
    return `(${label})`;
  }).join('');
  return sectionCitation + suffix;
}
