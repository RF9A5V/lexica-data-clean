import { pool } from './connection.js';

export async function selectNextBatch({ limit, lawId, promptVersion, model, keywordsSetVersion }) {
  const params = [promptVersion, model, keywordsSetVersion, lawId ?? null, limit];
  const { rows } = await pool.query(
    `SELECT cst.unit_id, cst.text_version_id, cst.law_id, cst.label, cst.text_plain
     FROM current_section_text cst
     LEFT JOIN unit_enrichments e
       ON e.unit_id = cst.unit_id
      AND e.text_version_id = cst.text_version_id
      AND e.prompt_version = $1
      AND e.model = $2
      AND e.keywords_set_version = $3
     WHERE e.id IS NULL
       AND ($4::TEXT IS NULL OR cst.law_id = $4)
     ORDER BY cst.unit_id
     LIMIT $5`,
    params
  );
  return rows;
}

export async function insertEnrichment({ unitId, textVersionId, lawId, label, promptVersion, model, keywordsSetVersion, digest, jsonRaw, status, errorMessage, promptHash }) {
  const { rows } = await pool.query(
    `INSERT INTO unit_enrichments
      (unit_id, text_version_id, law_id, label, prompt_version, model, keywords_set_version, digest, json_raw, status, error_message, prompt_hash)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
     ON CONFLICT (unit_id, text_version_id, prompt_version, model, keywords_set_version)
     DO UPDATE SET digest = EXCLUDED.digest, json_raw = EXCLUDED.json_raw, status = EXCLUDED.status, error_message = EXCLUDED.error_message, updated_at = NOW(), prompt_hash = EXCLUDED.prompt_hash
     RETURNING id`,
    [unitId, textVersionId, lawId, label, promptVersion, model, keywordsSetVersion, digest, jsonRaw, status, errorMessage ?? null, promptHash]
  );
  return rows[0].id;
}

export async function insertUsage({ enrichmentId, provider, model, promptTokens, completionTokens, totalTokens, costUsd }) {
  await pool.query(
    `INSERT INTO enrichment_usage (enrichment_id, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd)
     VALUES ($1,$2,$3,$4,$5,$6,$7)`,
    [enrichmentId, provider, model, promptTokens ?? null, completionTokens ?? null, totalTokens ?? null, costUsd ?? null]
  );
}

export async function getAllowedKeywords({ keywordsSetVersion }) {
  const { rows } = await pool.query(
    `SELECT id, name, slug FROM keywords WHERE keywords_set_version = $1 ORDER BY name`,
    [keywordsSetVersion]
  );
  return rows;
}

export async function getKeywordIdsBySlugs(slugs) {
  if (!slugs.length) return [];
  const params = slugs;
  const placeholders = params.map((_, i) => `$${i + 1}`).join(',');
  const { rows } = await pool.query(
    `SELECT id, slug FROM keywords WHERE slug IN (${placeholders})`,
    params
  );
  return rows;
}

export async function deleteUnitKeywordsForConfig({ unitId, promptVersion, keywordsSetVersion, model }) {
  await pool.query(
    `DELETE FROM unit_keywords WHERE unit_id = $1 AND prompt_version = $2 AND keywords_set_version = $3 AND model = $4`,
    [unitId, promptVersion, keywordsSetVersion, model]
  );
}

export async function upsertUnitKeywords({ unitId, keywordIds, enrichmentId, promptVersion, keywordsSetVersion, model }) {
  if (!keywordIds.length) return;
  const values = keywordIds.map((kid, idx) => `($1, $${idx + 2}, $${keywordIds.length + 2}, $${keywordIds.length + 3}, $${keywordIds.length + 4}, $${keywordIds.length + 5}, NOW())`).join(',');
  const params = [unitId, ...keywordIds, enrichmentId ?? null, keywordsSetVersion, promptVersion, model];
  await pool.query(
    `INSERT INTO unit_keywords (unit_id, keyword_id, enrichment_id, keywords_set_version, prompt_version, model, created_at)
     VALUES ${values}
     ON CONFLICT (unit_id, keyword_id, prompt_version, keywords_set_version, model) DO NOTHING`,
    params
  );
}

// Upsert keywords with tier classification. Returns rows with id, name, slug, tier.
export async function upsertKeywordsBulk({ items, keywordsSetVersion }) {
  if (!items?.length) return [];
  const cols = ['name', 'slug', 'keywords_set_version', 'tier'];
  const values = items.map((it, i) => `($${i * 4 + 1}, $${i * 4 + 2}, $${i * 4 + 3}, $${i * 4 + 4})`).join(',');
  const params = items.flatMap(it => [it.name, it.slug, keywordsSetVersion, it.tier ?? null]);
  const { rows } = await pool.query(
    `INSERT INTO keywords (${cols.join(', ')})
     VALUES ${values}
     ON CONFLICT (name)
     DO UPDATE SET slug = EXCLUDED.slug, keywords_set_version = EXCLUDED.keywords_set_version, tier = EXCLUDED.tier
     RETURNING id, name, slug, tier`,
    params
  );
  return rows;
}

// Insert or update structured taxonomy for a unit+enrichment
export async function insertOrUpsertUnitTaxonomy({ unitId, enrichmentId, taxonomy }) {
  await pool.query(
    `INSERT INTO unit_taxonomy (unit_id, enrichment_id, taxonomy)
     VALUES ($1, $2, $3)
     ON CONFLICT (unit_id, enrichment_id)
     DO UPDATE SET taxonomy = EXCLUDED.taxonomy, updated_at = NOW()`,
    [unitId, enrichmentId, taxonomy]
  );
}
