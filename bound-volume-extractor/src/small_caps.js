/**
 * Small-caps recombiner for NY official-reports PDFs.
 *
 * NY3d / AD3d / Misc 3d typeset party names and judge surnames in small-caps
 * style: the first letter at full point size, the rest at a smaller size on
 * the same baseline. pdfplumber sees these as separate words because their
 * font sizes differ. This module walks the words array and joins each
 * small-caps name back into one word — and crucially, restores the original
 * mixed case (so "MYERS" / "DIFIORE" come back as "Myers" / "DiFiore").
 *
 * Empirical patterns observed in 30 N.Y.3d:
 *
 *   Two-piece (most names — "Myers", "Schneiderman", "Stein"):
 *     'M'    sz=11.0  top=324.7
 *     'YERS' sz=7.0   top=327.8   ← ratio 0.64; top offset 3.1pt (~28% of base)
 *
 *   Multi-piece (names with internal capitals — "DiFiore", "McCain"):
 *     'D'    sz=11.0  top=A
 *     'I'    sz=7.0   top=A+~3
 *     'F'    sz=11.0  top=A
 *     'IORE' sz=7.0   top=A+~3
 *
 * Continuation rule: a word continues the current name if it is all caps,
 * adjacent on the x-axis (small or zero gap), and either
 *   (a) the same point size at the same baseline as the lead cap, OR
 *   (b) a smaller point size at a slightly lower top (small-caps body).
 *
 * Recovery rule for original case: cap-sized pieces stay uppercase, smaller
 * pieces get lowercased on merge. So the output preserves the human-readable
 * surname form, which is what we want for case names and authorship.
 */

// Leading-cap word may be preceded by punctuation that pdfplumber bundles
// (e.g. ",W" / "(S" when there's no space between punctuation and the cap).
// Capture group 1 is the alpha portion used for length-checking.
const STARTING_CAP_WITH_PREFIX = /^[^A-Za-z]*([A-Z][A-Z']*)$/;
const ALL_CAPS = /^[A-Z']+$/;

const MIN_CAP_SIZE          = 9;     // pt; anything smaller can't be the lead
const MAX_CAP_LENGTH        = 4;     // chars; lead cap is typically 1-3
const MIN_BODY_RATIO        = 0.40;  // body size must be ≥ this fraction of base
const MAX_BODY_RATIO        = 0.85;  // and < this (otherwise it's another cap, not a body)
const MIN_TOP_OFFSET_RATIO  = 0.05;  // body sits at least this much below cap baseline (as fraction of base size)
const MAX_TOP_OFFSET_RATIO  = 0.50;  // and at most this much
// Two thresholds: body-continuation has its own loose handling (no x-gap
// check at all), but same-baseline cap continuation (DiFiore F, McCain C)
// must be tightly packed — empirically <0.5pt between the previous body
// fragment and the next cap. The previous 3.5pt threshold accidentally
// fused two-name pairs like "Sekou Shutsha" (3.1pt inter-name gap) into
// one word "SekouShutsha". Lowered to 1.5pt to safely reject those while
// still accepting DiFiore-style intra-name continuation.
const MAX_CAP_CAP_GAP       = 1.5;
const MIN_X_GAP             = -1.5;  // slight overlap allowed (kerning quirks)
const SAME_SIZE_TOLERANCE   = 0.5;   // pt; size delta below this counts as "same size"
const SAME_BASELINE_TOL     = 1.0;   // pt; top delta below this counts as "same baseline"

/**
 * Walk a page's word array and return a new array where small-caps name
 * sequences have been merged into single words. Non-name words pass through
 * untouched. Each merged word carries `_recombined: true` and
 * `_components: n` so callers can audit / debug.
 *
 * Two continuation flavors:
 *   - **Same-baseline cap** (DiFiore F, McCain C): same point size at same
 *     baseline. Requires tight x-gap (<= MAX_X_GAP) since these are letters
 *     of one word, not separate words.
 *   - **Body fragment** (small-caps body letters under a lead cap): smaller
 *     point size at slightly lower top. Does NOT require tight x-gap — the
 *     body of a compound name like "People OF THE State OF New York" has
 *     real word boundaries between EOPLE / OF / THE that look like
 *     normal-text spacing in pdfplumber's word extraction. We stop body
 *     collection only at the next same-baseline cap (which begins the next
 *     name), or at any word that's neither a body fragment nor a cap.
 */
export function recombineWords(words) {
  const out = [];
  let i = 0;
  while (i < words.length) {
    const start = words[i];
    if (!isStartingCap(start)) {
      out.push(start);
      i++;
      continue;
    }

    const collected = [start];
    let lastX1 = start.x1;
    let j = i + 1;
    // Strip leading punctuation (e.g. ",W" / "(S") to get the alpha portion.
    // The same-size cap continuation rule is meant for the second cap in
    // multi-cap names like "DiFiore" or "McCain", where each cap is a
    // single letter. Multi-letter leads like "OF", "MTR", "LLC" are
    // complete words on their own and must NOT be joined with adjacent
    // same-size caps.
    const leadAlpha = (start.text.match(/[A-Za-z']+/) || [''])[0];
    const allowSameSizeCap = leadAlpha.length === 1;
    while (j < words.length) {
      const w = words[j];
      // Same-baseline cap (DiFiore-style continuation): requires tight x-gap.
      if (isSameBaselineCap(w, start.size, start.top)) {
        if (!allowSameSizeCap) break;
        const xGap = w.x0 - lastX1;
        if (xGap < MIN_X_GAP || xGap > MAX_CAP_CAP_GAP) break;
        collected.push(w);
        lastX1 = w.x1;
        j++;
        continue;
      }
      // Body fragment: small-caps body letters under the lead. No x-gap
      // constraint — compound captions ("People OF THE State OF New York")
      // place body words with normal inter-word spacing.
      if (isBodyContinuation(w, start.size, start.top)) {
        collected.push(w);
        lastX1 = w.x1;
        j++;
        continue;
      }
      // Anything else (lowercase text, punctuation, blank line, etc.) ends
      // the body collection.
      break;
    }

    if (collected.length > 1) {
      out.push(mergeWords(collected));
      i = j;
    } else {
      out.push(start);
      i++;
    }
  }
  return out;
}

function isSameBaselineCap(word, baseSize, baseTop) {
  if (!word || typeof word.size !== 'number') return false;
  if (!ALL_CAPS.test(word.text)) return false;
  if (Math.abs(word.size - baseSize) >= SAME_SIZE_TOLERANCE) return false;
  return Math.abs(word.top - baseTop) < SAME_BASELINE_TOL;
}

function isBodyContinuation(word, baseSize, baseTop) {
  if (!word || typeof word.size !== 'number') return false;
  if (!ALL_CAPS.test(word.text)) return false;
  if (word.size >= baseSize) return false;
  const sizeRatio = word.size / baseSize;
  if (sizeRatio < MIN_BODY_RATIO || sizeRatio > MAX_BODY_RATIO) return false;
  const topOffsetRatio = (word.top - baseTop) / baseSize;
  return topOffsetRatio >= MIN_TOP_OFFSET_RATIO && topOffsetRatio <= MAX_TOP_OFFSET_RATIO;
}

function isStartingCap(word) {
  if (!word || typeof word.size !== 'number') return false;
  if (word.size < MIN_CAP_SIZE) return false;
  const m = word.text.match(STARTING_CAP_WITH_PREFIX);
  if (!m) return false;
  return m[1].length <= MAX_CAP_LENGTH;
}

// X-gap threshold above which two consecutive body fragments are treated as
// separate words within a compound caption, e.g. "People OF THE State" puts
// EOPLE/OF and OF/THE at ~4pt gaps that we must preserve as spaces. Same-name
// body fragments (the rest of one surname after the lead cap) are tightly
// packed at <1pt, so 1pt is a safe cutoff.
const BODY_WORD_BREAK_GAP = 1.5;

function mergeWords(parts) {
  const baseSize = parts[0].size;
  let text = '';
  for (let k = 0; k < parts.length; k++) {
    const p = parts[k];
    const isBody = !(Math.abs(p.size - baseSize) < SAME_SIZE_TOLERANCE);
    if (k > 0) {
      const prev = parts[k - 1];
      const prevIsBody = !(Math.abs(prev.size - baseSize) < SAME_SIZE_TOLERANCE);
      // Insert a space ONLY between two body fragments separated by more
      // than the same-name packing gap. Cap→body, body→cap, and cap→cap
      // boundaries all stay glueless ("DiFiore", "Smith"). Body→body within
      // one surname is also glueless ("PE OPLE" never happens; small-caps
      // packing is sub-1pt).
      if (isBody && prevIsBody && (p.x0 - prev.x1) > BODY_WORD_BREAK_GAP) {
        text += ' ';
      }
    }
    if (isBody) {
      text += p.text.toLowerCase();    // small-cap body — these were lowercase letters
    } else {
      text += p.text;                  // cap-sized piece — preserve original case
    }
  }
  return {
    text,
    x0: parts[0].x0,
    x1: parts[parts.length - 1].x1,
    top: parts[0].top,
    bottom: Math.max(...parts.map(p => p.bottom)),
    size: baseSize,
    fontname: parts[0].fontname,
    _recombined: true,
    _components: parts.length,
  };
}

/**
 * Convenience wrapper: rebuild a single-string line from a recombined word
 * array, joining with single spaces. Useful when downstream code wants the
 * caption / signatory line as plain text rather than a word stream.
 */
export function joinWords(words) {
  return words.map(w => w.text).join(' ');
}
