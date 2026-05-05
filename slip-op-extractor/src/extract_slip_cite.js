/**
 * Slip-opinion citation parser.
 *
 * Recognised forms (all observed in raw/html samples):
 *   "2026 NY Slip Op 02720"
 *   "2026 NY Slip Op 50206(U)"
 *   "2003 NYSlipOp 17885"           (older single-token spelling)
 *   "2003 NY Slip Op 17890 [1 NY3d 29]"  (with parallel cite — bracketed
 *                                          part is consumed by `findParallelCite`
 *                                          in shared.js, not this parser)
 *
 * Returns { year, slipOpNumber, isUnreported } on success, null on failure.
 *
 * Mirrors `parseSlipOpCite` in co-collection so the two stay aligned. We
 * duplicate the logic here rather than import — co-data extractors are
 * deliberately self-contained, with no cross-package imports.
 */
const SLIP_OP_RE = /(\d{4})\s*ny\s*slip\s*op\s*(\d+)\s*(\(u\))?/i;

export function parseSlipOpCiteFromText(input) {
  if (typeof input !== 'string') return null;
  const m = input.match(SLIP_OP_RE);
  if (!m) return null;
  const year = parseInt(m[1], 10);
  if (!Number.isInteger(year) || year < 1900 || year > 2200) return null;
  const slipOpNumber = String(parseInt(m[2], 10));
  if (!/^\d+$/.test(slipOpNumber)) return null;
  return { year, slipOpNumber, isUnreported: Boolean(m[3]) };
}
