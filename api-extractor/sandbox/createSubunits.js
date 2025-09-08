import pg from "pg";
import { createHash } from "crypto";
import { getTokenizedText } from "./parser.js";

const { Pool } = pg;

// Configuration via env vars with sensible defaults
const DB_CONFIG = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME || "nysenate_legislative",
  user: process.env.DB_USER || "dev",
  password: process.env.DB_PASSWORD || "dev",
};

const DEFAULT_OPTIONS = {
  lawId: process.env.LAW_ID || null, // e.g., 'ABC' (uppercase)
  limit: Number(process.env.LIMIT || 0) || null, // optional cap on sections
  offset: Number(process.env.OFFSET || 0) || 0,
  dryRun: (process.env.DRY_RUN ?? "true").toLowerCase() !== "false",
  applyMigrations: (process.env.APPLY_MIGRATIONS ?? "true").toLowerCase() !== "false",
};

// Layer codes to enforce sort order when all subunits are direct children of the section
const LAYER = {
  subdivision: 1,
  paragraph: 2,
  subparagraph: 3,
};

function zeroPad(n, width = 3) {
  const s = String(n);
  return s.length >= width ? s : "0".repeat(width - s.length) + s;
}

function alphaIndex(id) {
  // map 'a'->1, 'b'->2, 'aa'->27, etc. Lowercase preferred for tokens
  if (!id) return 0;
  const s = String(id).toLowerCase();
  let result = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 97 || c > 122) return 0; // non a-z
    result = result * 26 + (c - 96);
  }
  return result;
}

function romanToInt(roman) {
  if (!roman) return 0;
  const s = roman.toLowerCase();
  const map = { i: 1, v: 5, x: 10, l: 50, c: 100, d: 500, m: 1000 };
  let total = 0, prev = 0;
  for (let i = s.length - 1; i >= 0; i--) {
    const val = map[s[i]] || 0;
    if (val < prev) total -= val; else total += val;
    prev = val;
  }
  return total;
}

function computeSortKey(unitType, localId, pathParts = []) {
  // Build a hierarchical sort key using ancestor indices + current index
  // Example: subdivision 1 => 1.001
  //          paragraph a under subdiv 1 => 1.001.2.001
  //          subparagraph i under paragraph a => 1.001.2.001.3.001
  const parts = [];

  const pushIndex = (t, id) => {
    const layer = LAYER[t] || 9;
    let idx = 0;
    if (t === "subdivision") idx = Number(String(id).replace(/[^0-9]/g, "")) || 0;
    else if (t === "paragraph") idx = alphaIndex(id);
    else if (t === "subparagraph") idx = romanToInt(id);
    parts.push(`${layer}.${zeroPad(idx)}`);
  };

  for (const p of pathParts) pushIndex(p.unitType, p.localId);
  pushIndex(unitType, localId);

  return parts.join(".");
}

function composeLocalPath(pathParts = [], localId) {
  // Human-readable path like 1.a.i where localId is appended last
  const prior = pathParts.map(p => String(p.localId)).filter(Boolean);
  return [...prior, String(localId)].join(".");
}

function generateSubunitId(sectionId, unitType, localPath) {
  return `${sectionId}:${unitType}:${localPath}`;
}

function checksumFor(text, parserVersion = "v1") {
  return createHash("sha256").update(`${parserVersion}\n${text || ""}`).digest("hex");
}

function plainChecksum(text) {
  return createHash("sha256").update(text || "").digest("hex");
}

function parseToken(token) {
  // token like {SUBDIVISION_1}, {PARAGRAPH_a}, {SUBPARAGRAPH_i}
  if (!token) return null;
  const m = token.match(/^\{(?<kind>SUBDIVISION|PARAGRAPH|SUBPARAGRAPH)_(?<id>[^}]+)\}$/);
  if (!m) return null;
  const kind = m.groups.kind.toLowerCase(); // maps to unit_type
  const id = m.groups.id.trim();
  return { unitType: kind, localId: id };
}

async function applyMigrations(pool) {
  const stmts = [
    `ALTER TABLE unit_text_versions ADD COLUMN IF NOT EXISTS text_tokenized TEXT;`,
    `ALTER TABLE unit_text_versions ADD COLUMN IF NOT EXISTS citations_extracted JSONB;`,
    `ALTER TABLE unit_text_versions ADD COLUMN IF NOT EXISTS tokenization_checksum TEXT;`,
  ];
  for (const s of stmts) {
    await pool.query(s);
  }
}

async function* iterateSections(pool, { lawId = null, limit = null, offset = 0 } = {}) {
  // Select current-effective version for each section
  const params = [];
  let where = `u.unit_type = 'section'`;
  if (lawId) {
    params.push(lawId);
    where += ` AND u.law_id = $${params.length}`;
  }
  // We pull effective_start and effective_end for propagation to subunits
  const sql = `
    SELECT
      u.id AS unit_id,
      u.law_id,
      utv.id AS utv_id,
      utv.effective_start,
      utv.effective_end,
      utv.text_plain
    FROM units u
    JOIN LATERAL (
      SELECT id, text_plain, effective_start, effective_end
      FROM unit_text_versions t
      WHERE t.unit_id = u.id
        AND t.effective_start <= CURRENT_DATE
        AND (t.effective_end IS NULL OR t.effective_end > CURRENT_DATE)
      ORDER BY t.effective_start DESC
      LIMIT 1
    ) utv ON TRUE
    WHERE ${where}
    ORDER BY u.id
    ${limit ? `LIMIT ${Number(limit)}` : ""}
    ${offset ? `OFFSET ${Number(offset)}` : ""}
  `;
  const res = await pool.query(sql, params);
  for (const row of res.rows) {
    yield row;
  }
}

async function upsertSubunit(pool, { sectionId, lawId, effectiveStart, effectiveEnd }, match, pathParts = []) {
  // Determine unit type and local id from the token
  const tokenInfo = parseToken(match.token);
  if (!tokenInfo) return { skipped: true, reason: "Unrecognized token", token: match.token };
  const { unitType, localId } = tokenInfo;

  // Extend path with current segment
  const fullPath = [...pathParts, { unitType, localId }];

  // Construct deterministic id and sort_key with ancestor context
  const localPathStr = composeLocalPath(pathParts, localId);
  const subunitId = generateSubunitId(sectionId, unitType, localPathStr);
  const sortKey = computeSortKey(unitType, localId, pathParts);

  // Text values
  const textPlain = match?.groups?.text ?? null;
  const textTokenized = match?.tokenizedText ?? null;
  const checksum = checksumFor(textTokenized ?? textPlain ?? "");

  // Upsert unit
  const upsertUnitSql = `
    INSERT INTO units (id, unit_type, number, label, parent_id, sort_key, citation, canonical_id, source_id, law_id)
    VALUES ($1, $2, $3, NULL, $4, $5, NULL, $1, 'nysenate', $6)
    ON CONFLICT (id) DO UPDATE SET
      unit_type = EXCLUDED.unit_type,
      number = EXCLUDED.number,
      parent_id = EXCLUDED.parent_id,
      sort_key = EXCLUDED.sort_key,
      law_id = EXCLUDED.law_id,
      updated_at = NOW()
  `;
  const upsertUnitParams = [subunitId, unitType, String(localId), sectionId, sortKey, lawId];

  // Upsert unit_text_versions by (unit_id, effective_start)
  const upsertUtvSql = `
    INSERT INTO unit_text_versions (unit_id, effective_start, effective_end, text_html, text_plain, checksum, text_tokenized, citations_extracted, tokenization_checksum)
    VALUES ($1, $2, $3, NULL, $4, $5, $6, NULL, $7)
    ON CONFLICT (unit_id, effective_start) DO UPDATE SET
      effective_end = EXCLUDED.effective_end,
      text_html = EXCLUDED.text_html,
      text_plain = EXCLUDED.text_plain,
      checksum = EXCLUDED.checksum,
      text_tokenized = EXCLUDED.text_tokenized,
      tokenization_checksum = EXCLUDED.tokenization_checksum
  `;
  const upsertUtvParams = [
    subunitId,
    effectiveStart,
    effectiveEnd,
    textPlain,
    plainChecksum(textPlain || ""),
    textTokenized,
    checksum,
  ];

  await pool.query(upsertUnitSql, upsertUnitParams);
  await pool.query(upsertUtvSql, upsertUtvParams);

  // Also upsert nested matches directly under the section (flattening) carrying full path for uniqueness
  if (Array.isArray(match.matches) && match.matches.length > 0) {
    for (const child of match.matches) {
      await upsertSubunit(pool, { sectionId, lawId, effectiveStart, effectiveEnd }, child, fullPath);
    }
  }

  return { subunitId, unitType, localId };
}

async function processSection(pool, sectionRow, options) {
  const { dryRun } = options;
  const { unit_id: sectionId, utv_id: sectionUtvId, law_id: lawId, effective_start, effective_end } = sectionRow;

  const parsed = await getTokenizedText(sectionId);
  if (!parsed) return { sectionId, skipped: true, reason: "No text returned from parser" };

  const { text: tokenizedText, matches, citations } = parsed;

  if (dryRun) {
    return {
      sectionId,
      plan: {
        updateUtv: {
          utvId: sectionUtvId,
          text_tokenized: tokenizedText?.slice(0, 200) + (tokenizedText && tokenizedText.length > 200 ? "…" : ""),
          citations_extracted_count: Array.isArray(citations) ? citations.length : 0,
        },
        subunits: Array.isArray(matches) ? matches.map(m => ({ token: m.token, hasNested: Array.isArray(m.matches) && m.matches.length > 0 })) : [],
      },
    };
  }

  // Real write flow under one transaction per section
  await pool.query("BEGIN");
  try {
    // Update section's current utv with tokenized text and citations JSON
    const updateUtvSql = `
      UPDATE unit_text_versions
      SET text_tokenized = $1,
          citations_extracted = $2::jsonb,
          tokenization_checksum = $3
      WHERE id = $4
    `;
    const updateUtvParams = [
      tokenizedText,
      JSON.stringify(citations || []),
      checksumFor(tokenizedText || ""),
      sectionUtvId,
    ];
    await pool.query(updateUtvSql, updateUtvParams);

    // Upsert subunits (flattened; all parented to section)
    if (Array.isArray(matches)) {
      for (const match of matches) {
        await upsertSubunit(pool, { sectionId, lawId, effectiveStart: effective_start, effectiveEnd: effective_end }, match, []);
      }
    }

    await pool.query("COMMIT");
  } catch (err) {
    await pool.query("ROLLBACK");
    throw err;
  }

  return { sectionId, updated: true, subunitsCount: Array.isArray(matches) ? matches.length : 0 };
}

export async function run(userOptions = {}) {
  const options = { ...DEFAULT_OPTIONS, ...userOptions };
  const pool = new Pool(DB_CONFIG);

  try {
    if (options.applyMigrations) {
      await applyMigrations(pool);
    }

    const results = { processed: 0, updated: 0, skipped: 0, errors: 0 };

    for await (const sectionRow of iterateSections(pool, options)) {
      results.processed++;
      try {
        const res = await processSection(pool, sectionRow, options);
        if (res.skipped) results.skipped++; else if (res.updated || res.plan) results.updated++;
      } catch (err) {
        results.errors++;
        console.error(`Error processing section ${sectionRow.unit_id}:`, err.message);
      }
    }

    if (options.dryRun) {
      console.log(`Dry run complete. Sections considered: ${results.processed}, with plans for ${results.updated}. Errors: ${results.errors}.`);
    } else {
      console.log(`Write complete. Sections processed: ${results.processed}, updated: ${results.updated}, errors: ${results.errors}.`);
    }

    return results;
  } finally {
    await pool.end();
  }
}

// Guarded CLI entry (not automatically executed). To run later:
//   LAW_ID=ABC DRY_RUN=true node co-data/api-extractor/sandbox/createSubunits.js
//   DRY_RUN=false to write changes; LIMIT/OFFSET to chunk; APPLY_MIGRATIONS=false to skip ALTERs.
if (import.meta.url === `file://${process.argv[1]}`) {
  // Intentionally do not auto-run. Uncomment when ready to execute from CLI.
  run().catch(err => { console.error(err); process.exit(1); });
}