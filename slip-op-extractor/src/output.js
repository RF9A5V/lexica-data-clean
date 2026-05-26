/**
 * Serialize parsed slip-op cases into the bulk-ingest JSON contract.
 *
 * Schema_version: 0.3.
 *
 * The shape mirrors the bound-volume-extractor's `cases.json` so co-collection
 * can ingest either kind of payload through the same `insertCase` path. Slip-op
 * payloads diverge in two ways from bound-volume payloads:
 *
 *   * `volume` is omitted. Slip-ops aren't published in a reporter volume yet
 *     — that's the whole point. The 0.3 validator makes the field optional.
 *   * Citations are expressed as the rich `citations: [{cite, citation_type,
 *     curie?}]` array (also a 0.3 feature). Slip-ops always carry a
 *     `slip_op` typed citation. When the slip-op metadata embeds the
 *     bound-volume reporter cite (e.g. "2003 NY Slip Op 17890 [1 NY3d 29]"),
 *     that bracketed cite is the *official* citation for the case — the
 *     same cite the bound-volume extractor would emit when the volume is
 *     later loaded. We therefore label it citation_type='official' (not
 *     'parallel') and pre-stamp it with the bound-volume curie format
 *     (`<reporter>:<vol>:<page>:<slug>`) so the server's matcher can
 *     auto-link via tier-1 against an already-ingested bound-volume case
 *     without needing name+date heuristics.
 *
 * Top-level metadata is per-document (one slip op per file, vs. the bound-vol
 * "many cases per volume" shape). The downstream compile step bundles many
 * per-doc payloads into a single per-source merged payload for upload.
 *
 * Field names match co-collection's `validateParsedVolume`:
 *   schema_version    — '0.3'
 *   batch_id          — uuid-ish per-document identifier
 *   source_pdf_sha256 — SHA-256 of the source bytes (PDF or HTML)
 *   target_source_db  — source ref the case is destined for
 *   volume            — omitted (null)
 *
 * Opinion CURIEs get a placeholder derived from `case_curie`; the inserter
 * regenerates the real curie from case_id at insert time.
 */

import { randomUUID } from 'crypto';
import { PARSER_VERSION, slugify, findParallelCite } from './shared.js';

const SCHEMA_VERSION = '0.3';

export function buildPayload({ caseObj, sourceSha256 }) {
  if (!caseObj) {
    return {
      schema_version: SCHEMA_VERSION,
      batch_id: randomUUID(),
      parser_version: PARSER_VERSION,
      source_pdf_sha256: sourceSha256 || '',
      target_source_db: '',
      cases: [],
    };
  }

  const sourceRef = caseObj._routing?.source_ref || '';

  // Strip the routing hint from the case object itself — it's metadata for
  // the orchestrator, not part of the bulk-ingest contract.
  const { _routing, ...cleanCase } = caseObj;

  // 0.3 citations: convert the case's slip-op metadata + any parallel cite
  // into the rich citations[] array. We deliberately drop the legacy
  // `citation` (string) and `parallel_cites` (string[]) fields — the 0.3
  // validator rejects mixed-shape cases.
  const citations = buildCitations(cleanCase);
  delete cleanCase.citation;
  delete cleanCase.parallel_cites;
  cleanCase.citations = citations;

  // Populate placeholder opinion.curie. Silences the validator's missing-
  // curie warning; the inserter regenerates the real curie from case_id at
  // insert time and ignores this value.
  if (Array.isArray(cleanCase.opinions)) {
    cleanCase.opinions = cleanCase.opinions.map(op => ({
      ...op,
      curie: op.curie ?? `${cleanCase.case_curie}#${op.opinion_index ?? 0}-${slugify(op.opinion_type || 'opinion')}`,
    }));
  }

  return {
    schema_version: SCHEMA_VERSION,
    batch_id: randomUUID(),
    parser_version: PARSER_VERSION,
    source_pdf_sha256: sourceSha256 || '',
    target_source_db: sourceRef,
    // volume omitted intentionally — see module docstring.
    cases: [cleanCase],
  };
}

/**
 * Construct the 0.3 `citations[]` array for a slip-op case.
 *
 * Always emits the slip-op cite itself with citation_type='slip_op' and the
 * `nyslopop:` (or `nyslopopu:`) CURIE the parsers attached as `case_curie`.
 *
 * When the case carries a `parallel_cites` array (the slip-op metadata
 * often embeds the bound-volume cite once known, e.g. "2003 NY Slip Op
 * 17890 [1 NY3d 29]"), each entry is added with citation_type='official'
 * — that bracketed cite IS the official reporter citation. Where the cite
 * parses cleanly to (volume, reporter, page) and the case has a
 * `name_abbreviation`, we synthesise the bound-volume curie
 * `<reporter-norm>:<vol>:<page>:<slug>` so the server-side matcher can
 * tier-1 auto-link against an already-loaded bound-volume case. Cites we
 * can't normalise (e.g. Misc 3d "(A)" page suffixes) are emitted without
 * a curie and rely on the matcher's tier-1.25 cite-string fallback.
 */
function buildCitations(caseObj) {
  const out = [];

  // Primary slip-op citation. The parsers populate caseObj.citation as the
  // canonical "YYYY NY Slip Op NNNNN" string, and caseObj.case_curie as the
  // matching nyslopop:/nyslopopu: CURIE.
  if (caseObj.citation) {
    out.push({
      cite: String(caseObj.citation),
      citation_type: 'slip_op',
      curie: caseObj.case_curie || null,
    });
  }

  // Bound-volume reporter cite(s) embedded in the slip-op metadata. These
  // are the OFFICIAL cites for the case once published — same cite string
  // the bound-volume extractor emits when the volume itself is loaded.
  for (const pc of caseObj.parallel_cites || []) {
    if (!pc) continue;
    const curie = officialCurieFor(pc, caseObj.name_abbreviation);
    out.push({
      cite: String(pc),
      citation_type: 'official',
      ...(curie ? { curie } : {}),
    });
  }

  return out;
}

/**
 * Synthesise a bound-volume case curie from a reporter cite string and the
 * case's name_abbreviation. Returns null when the cite can't be parsed
 * cleanly (unknown reporter, non-numeric page suffixes like "129(A)") or
 * when no name is available to slug.
 *
 * The curie format mirrors what the bound-volume extractor produces:
 *   `<reporter-lower-no-space>:<volume>:<firstPage>:<slug>`
 *   e.g. "1 NY3d 1" + "Bansbach v Zinn" → "ny3d:1:1:bansbach-v-zinn"
 */
function officialCurieFor(citeStr, nameAbbreviation) {
  if (!nameAbbreviation) return null;
  const parsed = findParallelCite(`[${citeStr}]`);
  if (!parsed) return null;
  if (!Number.isInteger(parsed.volume)) return null;
  if (!/^\d+$/.test(parsed.page)) return null;
  const reporterNorm = String(parsed.reporter).toLowerCase().replace(/\s+/g, '');
  if (!/^[a-z][a-z0-9]*$/.test(reporterNorm)) return null;
  const slug = slugify(nameAbbreviation);
  if (!slug) return null;
  return `${reporterNorm}:${parsed.volume}:${parsed.page}:${slug}`;
}
