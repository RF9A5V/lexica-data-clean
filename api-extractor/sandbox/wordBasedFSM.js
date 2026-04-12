import getSectionText from "./db.js";

// Non-global marker regex; matches tokens like "1." or "10." or "1-a." as subdivision headers
const SUBDIVISION_MARKER = /^(\d+(?:-\w+)?)\.$/;

function isSubdivisionMarker(word) {
  const m = word.match(SUBDIVISION_MARKER);
  return m ? m[1] : null;
}

async function wordBasedFSM(sectionId) {
  const rawText = await getSectionText(sectionId);
  if (!rawText || typeof rawText !== 'string') return { header: '', subdivisions: [] };

  // Normalize newlines to spaces; preserve punctuation attached to tokens
  const words = rawText.replace(/\n/g, ' ').split(/\s+/g).map(w => w.trim()).filter(Boolean);

  // Heuristic: skip first two tokens which are often section number/label
  let i = 2;
  let headerParts = [];

  // Accumulate header until first period-ending token
  while (i < words.length) {
    headerParts.push(words[i]);
    const w = words[i];
    i++;
    if (/[.!?]$/.test(w)) break;
  }
  const header = headerParts.join(' ').trim();

  const subdivisions = [];
  let current = null;

  while (i < words.length) {
    const w = words[i];
    const id = isSubdivisionMarker(w);
    if (id) {
      // Close prior
      if (current) {
        current.text = current.text.trim();
        subdivisions.push(current);
      }
      // Start new subdivision
      current = { id, text: '' };
    } else if (current) {
      current.text += (current.text ? ' ' : '') + w;
    }
    i++;
  }

  if (current) {
    current.text = current.text.trim();
    subdivisions.push(current);
  }

  return { header, subdivisions };
}

export default wordBasedFSM;
