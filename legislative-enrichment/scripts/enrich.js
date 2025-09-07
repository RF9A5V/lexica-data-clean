import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import pLimit from 'p-limit';
import { pool } from '../src/db/connection.js';
import { selectNextBatch, insertEnrichment, insertUsage, deleteUnitKeywordsForConfig, upsertUnitKeywords, upsertKeywordsBulk, insertOrUpsertUnitTaxonomy } from '../src/db/queries.js';
import { buildSystemPrompt, buildUserPrompt } from '../src/prompts/templates.js';
import { fetchEnrichment } from '../src/services/openai.js';
import { sha256Hex } from '../src/util/hash.js';

function slugify(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

async function processBatch(opts) {
  const rows = await selectNextBatch(opts);
  if (rows.length === 0) return 0;

  const limit = pLimit(opts.concurrency);
  await Promise.all(rows.map(row => limit(async () => {
    const { unit_id, text_version_id, law_id, label, text_plain } = row;

    const systemPrompt = buildSystemPrompt();
    const userPrompt = buildUserPrompt({ lawId: law_id, label, text: text_plain });
    const promptHash = sha256Hex([opts.promptVersion, opts.model, opts.keywordsSetVersion, law_id, label ?? '', text_plain].join('\n---\n'));

    try {
      const { result, usage } = await fetchEnrichment({ systemPrompt, userPrompt, model: opts.model });

      // Build taxonomy keyword items with tiers
      const items = [];
      const pushItems = (arr, tier) => {
        for (const name of Array.from(new Set((arr || []).map(x => x.trim()).filter(Boolean)))) {
          items.push({ name, slug: slugify(name), tier });
        }
      };
      pushItems(result.field_of_law, 'field_of_law');
      pushItems(result.doctrines, 'doctrine');
      pushItems(result.distinguishing_factors, 'distinguishing_factor');
      // Deduplicate by slug
      const dedupedMap = new Map(items.map(it => [it.slug, it]));
      const dedupedItems = Array.from(dedupedMap.values());

      // Insert/Upsert enrichment
      const enrichmentId = await insertEnrichment({
        unitId: unit_id,
        textVersionId: text_version_id,
        lawId: law_id,
        label,
        promptVersion: opts.promptVersion,
        model: opts.model,
        keywordsSetVersion: opts.keywordsSetVersion,
        digest: result.digest,
        jsonRaw: result,
        status: 'succeeded',
        errorMessage: null,
        promptHash,
      });

      // Store full taxonomy JSON
      await insertOrUpsertUnitTaxonomy({ unitId: unit_id, enrichmentId, taxonomy: result });

      // Usage
      const promptTokens = usage?.prompt_tokens ?? null;
      const completionTokens = usage?.completion_tokens ?? null;
      const totalTokens = usage?.total_tokens ?? (promptTokens && completionTokens ? promptTokens + completionTokens : null);
      await insertUsage({
        enrichmentId,
        provider: 'openai',
        model: opts.model,
        promptTokens,
        completionTokens,
        totalTokens,
        costUsd: null, // optional: compute via pricing table
      });

      // Upsert keywords with tiers, then link
      const upserted = await upsertKeywordsBulk({ items: dedupedItems, keywordsSetVersion: opts.keywordsSetVersion });
      const keywordIds = upserted.map(r => r.id);

      if (opts.replace) {
        await deleteUnitKeywordsForConfig({ unitId: unit_id, promptVersion: opts.promptVersion, keywordsSetVersion: opts.keywordsSetVersion, model: opts.model });
      }
      await upsertUnitKeywords({ unitId: unit_id, keywordIds, enrichmentId, promptVersion: opts.promptVersion, keywordsSetVersion: opts.keywordsSetVersion, model: opts.model });
    } catch (err) {
      // Record failed enrichment for audit
      await insertEnrichment({
        unitId: unit_id,
        textVersionId: text_version_id,
        lawId: law_id,
        label,
        promptVersion: opts.promptVersion,
        model: opts.model,
        keywordsSetVersion: opts.keywordsSetVersion,
        digest: '',
        jsonRaw: { error: String(err?.message || err) },
        status: 'failed',
        errorMessage: String(err?.message || err),
        promptHash,
      });
      console.error(`Unit ${unit_id} failed:`, err?.message || err);
    }
  })));

  return rows.length;
}

async function main() {
  const argv = await yargs(hideBin(process.argv))
    .option('law', { type: 'string', describe: 'Filter by law_id (e.g., PEN)' })
    .option('batch-size', { type: 'number', default: parseInt(process.env.BATCH_SIZE || '50', 10) })
    .option('concurrency', { type: 'number', default: parseInt(process.env.CONCURRENCY || '4', 10) })
    .option('prompt-version', { type: 'string', default: process.env.PROMPT_VERSION || 'v1' })
    .option('keywords-set-version', { type: 'string', default: process.env.KEYWORDS_SET_VERSION || 'v1' })
    .option('model', { type: 'string', default: process.env.MODEL || 'gpt-4o-mini' })
    .option('replace', { type: 'boolean', default: false, describe: 'Replace unit_keywords for this config' })
    .help().argv;

  const opts = {
    limit: argv['batch-size'],
    lawId: argv.law,
    concurrency: argv.concurrency,
    promptVersion: argv['prompt-version'],
    model: argv.model,
    keywordsSetVersion: argv['keywords-set-version'],
    replace: argv.replace,
  };

  let total = 0;
  while (true) {
    const n = await processBatch(opts);
    total += n;
    if (n === 0) break;
  }

  console.log(`Enrichment complete. processed=${total}`);
  await pool.end();
}

main().catch(async (err) => {
  console.error('enrich failed:', err);
  await pool.end();
  process.exit(1);
});
