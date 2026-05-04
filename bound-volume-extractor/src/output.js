import { writeFile } from 'fs/promises';

/**
 * Serialize a parse result to a single JSON document. This is the canonical
 * output format and is what the future co-collection import UI will consume.
 *
 * The shape is intentionally flat and verbose — every record carries
 * `batch_id`, source attribution, and the originating PDF's hash so any row
 * can be traced back to its provenance for audit/rollback.
 */
export async function writeJson(outPath, result) {
  const doc = {
    schema_version: '0.2',
    batch_id: result.batch_id,
    parser_version: result.parser_version,
    source_pdf: result.source_pdf,
    source_pdf_sha256: result.source_pdf_sha256,
    parsed_at: result.parsed_at,
    target_source_db: result.volume?.source_db || null,
    volume: result.volume,
    cases: result.cases,
    stats: result.stats,
    warnings: result.warnings,
  };
  await writeFile(outPath, JSON.stringify(doc, null, 2));
  return doc;
}

/**
 * Serialize a parse result to a SQL script suitable for direct application
 * to a source DB via `psql`.
 *
 * Output shape: a single transaction with one CTE-based statement per case
 * that inserts the case row, its citations, and its opinions in one shot.
 * Case and opinion CURIEs are computed inline and embedded as literals so
 * they're stable across runs and identical to what co-collection would
 * compute for the same data.
 *
 * Citation types follow the agreed convention:
 *   `official` — bound-volume cite (NY3d / AD3d / Misc 3d)
 *   `parallel` — regional reporter (NE3d, NYS3d). Slip-op cites added later
 *                by the slip-ingest flow will use `slip_op` as a third type.
 *
 * Idempotency: the script is NOT idempotent on its own. Running it twice
 * inserts duplicate rows. Rollback uses the case CURIEs from the audit
 * JSON: `DELETE FROM cases WHERE curie IN ('…', '…', …)` (citations and
 * opinions cascade-delete via FK).
 *
 * Provenance is tracked entirely in the audit JSON — `original_id` and
 * `file_name` are left NULL since those are CAP-specific. The audit file
 * holds the (case_curie → batch_id) mapping needed for rollback.
 */
export async function writeSql(outPath, result) {
  const cases = result.cases || [];
  const volumeMeta = result.volume;
  // CURIEs are assigned by parser.js — read directly from each case.
  const cured = cases.filter(c => c.case_curie).length;
  const collisionCount = cases.filter(c => c.case_curie?.match(/:\d{2}$/)).length;
  const caseCount = cured;

  const courtName = courtNameFor(volumeMeta);
  const courtAbbrev = 'N.Y.';
  const courtId = courtIdFor(volumeMeta);
  const jurisdictionName = 'New York';
  const jurisdictionAbbreviation = 'N.Y.';
  const jurisdictionId = 1;

  const lines = [];
  lines.push(`-- bound-volume-extractor SQL output`);
  lines.push(`-- batch_id:         ${result.batch_id}`);
  lines.push(`-- parser_version:   ${result.parser_version}`);
  lines.push(`-- source_pdf:       ${result.source_pdf}`);
  lines.push(`-- source_pdf_sha:   ${result.source_pdf_sha256}`);
  lines.push(`-- target_source_db: ${volumeMeta?.source_db || '<unknown>'}`);
  lines.push(`-- parsed_at:        ${result.parsed_at}`);
  lines.push(`-- cases:            ${cases.length}  (curies assigned: ${caseCount}, collisions: ${collisionCount})`);
  lines.push(`-- opinions:         ${cases.reduce((n, c) => n + (c.opinions?.length || 0), 0)}`);
  lines.push(``);
  lines.push(`-- Apply this file with psql against ${volumeMeta?.source_db || '<source DB>'}.`);
  lines.push(`-- Rollback: DELETE FROM cases WHERE curie IN (...) — case CURIEs are listed`);
  lines.push(`-- in the matching audit JSON. citations and opinions cascade-delete via FK.`);
  lines.push(``);
  lines.push(`BEGIN;`);
  lines.push(``);
  // cases.id has no DEFAULT — assign IDs from a temp sequence seeded from the
  // current MAX(id). Sequence is session-scoped; we DROP it explicitly before
  // COMMIT so re-running the script in the same session doesn't choke on a
  // stale "relation already exists".
  lines.push(`DROP SEQUENCE IF EXISTS bve_case_ids;`);
  lines.push(`CREATE TEMPORARY SEQUENCE bve_case_ids;`);
  lines.push(`SELECT setval('bve_case_ids', COALESCE((SELECT MAX(id) FROM cases), 0) + 1, false);`);
  lines.push(``);

  for (const c of cases) {
    if (!c.case_curie) {
      lines.push(`-- SKIPPED case: no CURIE could be derived (missing volume_page or name)`);
      lines.push(`--   citation: ${c.citation || '?'}`);
      lines.push(`--   parallel: ${(c.parallel_cites || []).join(' / ')}`);
      lines.push(``);
      continue;
    }
    lines.push(emitCaseStatement(c, {
      courtName, courtAbbrev, courtId,
      jurisdictionName, jurisdictionAbbreviation, jurisdictionId,
    }));
    lines.push(``);
  }

  lines.push(`COMMIT;`);
  lines.push(``);
  await writeFile(outPath, lines.join('\n'));
}

/**
 * Map our parser's coarse `volume.court` ("Court of Appeals" / "Appellate
 * Division" / "Trial Courts") onto the DB's `court_name` convention,
 * matching the form CAP-imported rows use.
 */
function courtNameFor(volumeMeta) {
  switch (volumeMeta?.reporter) {
    case 'NY3d':    return 'New York Court of Appeals';
    case 'AD3d':    return 'New York Supreme Court, Appellate Division';
    case 'Misc 3d': return 'New York Supreme Court';  // generic — Misc 3d covers multiple trial courts
    default:        return volumeMeta?.court || null;
  }
}

/**
 * Dominant `court_id` per source DB — observed values from the existing
 * CAP-imported rows. Misc 3d has no single right answer (Supreme/Surrogate's/
 * Civil/Criminal/etc. all publish there); we default to the most common
 * (Supreme Court). Fine-grained court detection is a downstream concern.
 */
function courtIdFor(volumeMeta) {
  switch (volumeMeta?.reporter) {
    case 'NY3d':    return 24653;  // New York Court of Appeals
    case 'AD3d':    return 8994;   // New York Supreme Court, Appellate Division
    case 'Misc 3d': return 8791;   // New York Supreme Court (default for Misc 3d)
    default:        return null;
  }
}

/**
 * Convert our parser's compact reporter form ("30 NY3d 1", "85 NE3d 57",
 * "62 NYS3d 838", "157 AD3d 627", "57 Misc 3d 1") to the dotted form the
 * source DBs use ("30 N.Y.3d 1", etc.).
 */
function normalizeCite(cite) {
  if (!cite) return cite;
  return cite
    .replace(/\bNYS(\d?d?)\b/g, 'N.Y.S.$1')
    .replace(/\bNY(\d?d?)\b/g, 'N.Y.$1')
    .replace(/\bNE(\d?d?)\b/g, 'N.E.$1')
    .replace(/\bAD(\d?d?)\b/g, 'A.D.$1')
    .replace(/\bMisc\s+(\d?d?)\b/g, 'Misc. $1')
    // Collapse "N.Y." / "N.Y.S." that became "N.Y.." before a space-d.
    .replace(/\.{2,}/g, '.');
}

/**
 * Emit a single CTE statement that inserts one case + its citations + its
 * opinions. `WITH new_case AS (… RETURNING id)` carries the auto-assigned
 * id forward to the citations and opinions inserts.
 */
function emitCaseStatement(c, ctx) {
  const {
    courtName, courtAbbrev, courtId,
    jurisdictionName, jurisdictionAbbreviation, jurisdictionId,
  } = ctx;
  // source_url is computed at parser stage so it lands in the JSON too.
  const sourceUrl = c.source_url ?? null;
  const out = [];
  out.push(`-- ${c.citation || '?'}  ${c.parallel_cites?.[0] ? '/ ' + c.parallel_cites[0] + '  ' : ''}— ${c.name || '?'}`);
  out.push(`WITH new_case AS (`);
  out.push(`  INSERT INTO cases (`);
  out.push(`    id, name, name_abbreviation, decision_date, docket_number,`);
  out.push(`    first_page, last_page, court_name, court_name_abbreviation, court_id,`);
  out.push(`    jurisdiction_name, jurisdiction_abbreviation, jurisdiction_id,`);
  out.push(`    curie, source_url, court_department`);
  out.push(`  ) VALUES (`);
  out.push(`    nextval('bve_case_ids'),`);
  out.push(`    ${sqlString(c.caption_text || c.name)},`);
  out.push(`    ${sqlString(c.name)},`);
  out.push(`    ${sqlString(c.decision_date)},`);
  out.push(`    ${sqlString(c.docket_number)},`);
  out.push(`    ${sqlString(c.first_page != null ? String(c.first_page) : null)},`);
  out.push(`    ${sqlString(c.last_page != null ? String(c.last_page) : null)},`);
  out.push(`    ${sqlString(courtName)},`);
  out.push(`    ${sqlString(courtAbbrev)},`);
  out.push(`    ${courtId == null ? 'NULL' : courtId},`);
  out.push(`    ${sqlString(jurisdictionName)},`);
  out.push(`    ${sqlString(jurisdictionAbbreviation)},`);
  out.push(`    ${jurisdictionId == null ? 'NULL' : jurisdictionId},`);
  out.push(`    ${sqlString(c.case_curie)},`);
  out.push(`    ${sqlString(sourceUrl)},`);
  out.push(`    ${c.court_department == null ? 'NULL' : c.court_department}`);
  out.push(`  ) RETURNING id`);
  out.push(`)`);

  const citeRows = [];
  if (c.citation) citeRows.push({ type: 'official', cite: normalizeCite(c.citation) });
  for (const pc of c.parallel_cites || []) citeRows.push({ type: 'parallel', cite: normalizeCite(pc) });

  const opinions = c.opinions || [];

  // Emit citations first (if any) as a CTE, then one CTE per opinion that
  // returns the new opinion's id, then a paired CTE per opinion that
  // inserts that opinion's footnotes (when any). The final statement is a
  // dummy SELECT — Postgres requires a terminal statement after a chain
  // of WITH ... clauses.
  if (citeRows.length) {
    out.push(`, inserted_cites AS (`);
    out.push(`  INSERT INTO citations (case_id, citation_type, cite)`);
    const citeUnion = citeRows.map((r, i) =>
      `  ${i === 0 ? 'SELECT' : 'UNION ALL SELECT'} id, ${sqlString(r.type)}, ${sqlString(r.cite)} FROM new_case`
    ).join('\n');
    out.push(citeUnion);
    out.push(`  RETURNING case_id`);
    out.push(`)`);
  }

  // Per-case captions. Even single-caption cases get a row here so
  // downstream queries can rely on the table being populated. For
  // consolidated appeals (multi-caption), each entry is an action's
  // caption and short name; caption_index 0 is the lead.
  const captions = c.captions || [];
  if (captions.length) {
    out.push(`, inserted_captions AS (`);
    out.push(`  INSERT INTO case_captions (case_id, caption_index, name, name_abbreviation, docket_number)`);
    const capUnion = captions.map((cap, i) =>
      `  ${i === 0 ? 'SELECT' : 'UNION ALL SELECT'} id, ${cap.caption_index ?? i}, ` +
      `${sqlString(cap.name)}, ${sqlString(cap.name_abbreviation)}, ${sqlString(cap.docket_number)} FROM new_case`
    ).join('\n');
    out.push(capUnion);
    out.push(`  RETURNING case_id`);
    out.push(`)`);
  }

  if (opinions.length === 0) {
    // No opinions: terminal SELECT ties off the WITH chain.
    out.push(`SELECT id FROM new_case;`);
    return out.join('\n');
  }

  // Per-opinion CTEs: ins_op_<i> returns the new opinion id; ins_fn_<i>
  // (when this opinion has footnotes) inserts the footnote rows keyed
  // off that id. CTE chain stays comma-separated; the very first CTE
  // already exists (`new_case`), so each subsequent one starts with `, `.
  for (let i = 0; i < opinions.length; i++) {
    const op = opinions[i];
    const opAlias = `ins_op_${i}`;
    out.push(`, ${opAlias} AS (`);
    out.push(`  INSERT INTO opinions (case_id, opinion_type, author, text, opinion_index, curie, page_breaks)`);
    out.push(
      `  SELECT id, ${sqlString(op.opinion_type)}, ${sqlString(op.author)}, ${sqlString(op.text)}, ` +
      `${op.opinion_index}, ${sqlString(op.curie)}, ${jsonbValue(op.page_breaks)} FROM new_case`
    );
    out.push(`  RETURNING id`);
    out.push(`)`);

    const footnotes = (op.footnotes || []).filter(fn => fn);
    if (footnotes.length) {
      out.push(`, ins_fn_${i} AS (`);
      out.push(`  INSERT INTO opinion_footnotes (opinion_id, opinion_curie, footnote_index, marker, text, body_offset, page_index, volume_page)`);
      const fnLines = footnotes.map((fn, fi) =>
        `  ${fi === 0 ? 'SELECT' : 'UNION ALL SELECT'} id, ${sqlString(op.curie)}, ${fn.footnote_index ?? fi}, ` +
        `${sqlString(fn.marker)}, ${sqlString(fn.text)}, ` +
        `${fn.body_offset == null ? 'NULL' : fn.body_offset}, ` +
        `${fn.page_index == null ? 'NULL' : fn.page_index}, ` +
        `${fn.volume_page == null ? 'NULL' : fn.volume_page} FROM ${opAlias}`
      ).join('\n');
      out.push(fnLines);
      out.push(`  RETURNING opinion_id`);
      out.push(`)`);
    }
  }

  // Terminal statement: pick something cheap that ties off the CTE chain.
  out.push(`SELECT id FROM new_case;`);
  return out.join('\n');
}

// Format a JS array of `{ offset, page_index, volume_page }` page-break
// records as a SQL JSONB literal. Empty / missing → SQL NULL.
function jsonbValue(v) {
  if (v == null || (Array.isArray(v) && v.length === 0)) return 'NULL';
  return `${sqlString(JSON.stringify(v))}::jsonb`;
}

/**
 * Format a JS value as a SQL string literal (quoted, with single quotes
 * doubled) or NULL. Postgres handles multi-line strings natively, so no
 * special escaping for `\n`.
 */
function sqlString(v) {
  if (v === null || v === undefined) return 'NULL';
  const s = String(v);
  return `'${s.replace(/'/g, "''")}'`;
}
