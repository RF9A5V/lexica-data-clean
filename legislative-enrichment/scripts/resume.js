import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { pool } from '../src/db/connection.js';
import { buildSystemPrompt, buildUserPrompt } from '../src/prompts/templates.js';
import { fetchEnrichment } from '../src/services/openai.js';
import { insertUsage, deleteUnitKeywordsForConfig, upsertUnitKeywords, upsertKeywordsBulk, insertOrUpsertUnitTaxonomy } from '../src/db/queries.js';

function slugify(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

async function main() {
  const argv = await yargs(hideBin(process.argv))
    .option('limit', { type: 'number', default: 50 })
    .option('model', { type: 'string', default: process.env.MODEL || 'gpt-4o-mini' })
    .option('replace', { type: 'boolean', default: false })
    .help().argv;

  const { rows } = await pool.query(
    `SELECT e.id AS enrichment_id, e.unit_id, e.text_version_id, e.law_id, e.label, cst.text_plain,
            e.prompt_version, e.keywords_set_version
     FROM unit_enrichments e
     JOIN current_section_text cst ON cst.unit_id = e.unit_id AND cst.text_version_id = e.text_version_id
     WHERE e.status = 'failed'
     ORDER BY e.id
     LIMIT $1`,
    [argv.limit]
  );

  if (rows.length === 0) {
    console.log('No failed enrichments to resume.');
    await pool.end();
    return;
  }

  for (const r of rows) {
    const systemPrompt = buildSystemPrompt();
    const userPrompt = buildUserPrompt({ lawId: r.law_id, label: r.label, text: r.text_plain });
    try {
      const { result, usage } = await fetchEnrichment({ systemPrompt, userPrompt, model: argv.model });

      // Persist enrichment update
      await pool.query(`UPDATE unit_enrichments SET digest=$1, json_raw=$2, status='succeeded', error_message=NULL, updated_at=NOW() WHERE id=$3`, [result.digest, result, r.enrichment_id]);

      // Store full taxonomy JSON
      await insertOrUpsertUnitTaxonomy({ unitId: r.unit_id, enrichmentId: r.enrichment_id, taxonomy: result });

      const promptTokens = usage?.prompt_tokens ?? null;
      const completionTokens = usage?.completion_tokens ?? null;
      const totalTokens = usage?.total_tokens ?? (promptTokens && completionTokens ? promptTokens + completionTokens : null);
      await insertUsage({ enrichmentId: r.enrichment_id, provider: 'openai', model: argv.model, promptTokens, completionTokens, totalTokens, costUsd: null });

      // Build taxonomy items and upsert keywords with tiers
      const items = [];
      const pushItems = (arr, tier) => {
        for (const name of Array.from(new Set((arr || []).map(x => x.trim()).filter(Boolean)))) {
          items.push({ name, slug: slugify(name), tier });
        }
      };
      pushItems(result.field_of_law, 'field_of_law');
      pushItems(result.doctrines, 'doctrine');
      pushItems(result.distinguishing_factors, 'distinguishing_factor');
      const deduped = Array.from(new Map(items.map(it => [it.slug, it])).values());
      const upserted = await upsertKeywordsBulk({ items: deduped, keywordsSetVersion: r.keywords_set_version || process.env.KEYWORDS_SET_VERSION || 'v1' });
      const keywordIds = upserted.map(x => x.id);

      if (argv.replace) {
        await deleteUnitKeywordsForConfig({ unitId: r.unit_id, promptVersion: r.prompt_version, keywordsSetVersion: r.keywords_set_version, model: argv.model });
      }
      await upsertUnitKeywords({ unitId: r.unit_id, keywordIds, enrichmentId: r.enrichment_id, promptVersion: r.prompt_version, keywordsSetVersion: r.keywords_set_version, model: argv.model });
    } catch (err) {
      console.error(`Resume failed for enrichment ${r.enrichment_id}:`, err?.message || err);
    }
  }

  await pool.end();
}

main().catch(async (err) => {
  console.error('resume failed:', err);
  await pool.end();
  process.exit(1);
});
