/**
 * Serialize parsed slip-op cases into the bulk-ingest JSON contract.
 *
 * The shape mirrors the bound-volume-extractor's `cases.json` so co-collection
 * can ingest either kind of payload through the same `insertCase` path. Top-
 * level metadata is per-document (one slip op per file, vs. the bound-vol
 * "many cases per volume" shape).
 *
 * Field names match co-collection's `validateParsedVolume`:
 *   schema_version    — '0.2' (the version the validator currently expects)
 *   batch_id          — uuid-ish per-document identifier
 *   source_pdf_sha256 — SHA-256 of the source bytes (PDF or HTML)
 *   target_source_db  — source ref the case is destined for
 *   volume.volume     — slip-op year (slip ops have no real volume; year is the
 *                       closest stable label and lets the audit page sort).
 *
 * Opinion CURIEs are populated with a placeholder derived from `case_curie` so
 * the validator's "Opinion missing curie — would be skipped" warning fires
 * once-per-case instead of once-per-opinion. The inserter (post PR 6) ignores
 * the parser-supplied opinion curie and regenerates from case_id at insert
 * time — see inserter.js comment near generateOpinionCurie.
 */

import { randomUUID } from 'crypto';
import { PARSER_VERSION, slugify } from './shared.js';

const SCHEMA_VERSION = '0.2';

export function buildPayload({ caseObj, sourceSha256 }) {
  if (!caseObj) {
    return {
      schema_version: SCHEMA_VERSION,
      batch_id: randomUUID(),
      parser_version: PARSER_VERSION,
      source_pdf_sha256: sourceSha256 || '',
      target_source_db: '',
      volume: null,
      cases: [],
    };
  }

  const reporter = caseObj._routing?.reporter || 'Misc 3d';
  const sourceRef = caseObj._routing?.source_ref || '';
  const year = extractYearFromCurie(caseObj.case_curie);

  // Strip the routing hint from the case object itself — it's metadata for
  // the orchestrator, not part of the bulk-ingest contract.
  const { _routing, ...cleanCase } = caseObj;

  // Populate a placeholder opinion.curie. This silences the validator's
  // missing-curie warning; the inserter regenerates the real curie from
  // case_id at insert time and ignores this value.
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
    volume: {
      reporter,                  // → COURT_DEFAULTS lookup in inserter.js
      volume: year,              // slip ops use the year as their pseudo-volume
      year,
      kind: 'slip_op',
    },
    cases: [cleanCase],
  };
}

function extractYearFromCurie(curie) {
  if (!curie) return null;
  const m = curie.match(/^nyslopopu?:(\d{4}):/);
  return m ? parseInt(m[1], 10) : null;
}
