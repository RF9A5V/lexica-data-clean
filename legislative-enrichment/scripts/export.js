import 'dotenv/config';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { pool } from '../src/db/connection.js';

async function exportNdjson({ law }) {
  const { rows } = await pool.query(
    `SELECT e.*, u.label AS unit_label, ut.taxonomy
     FROM unit_enrichments e
     JOIN units u ON u.id = e.unit_id
     LEFT JOIN unit_taxonomy ut ON ut.unit_id = e.unit_id AND ut.enrichment_id = e.id
     WHERE ($1::TEXT IS NULL OR e.law_id = $1)
     ORDER BY e.id`,
    [law ?? null]
  );
  for (const r of rows) {
    console.log(JSON.stringify(r));
  }
}

async function exportCsv({ law }) {
  const { rows } = await pool.query(
    `SELECT e.id, e.unit_id, e.text_version_id, e.law_id, e.label, e.prompt_version, e.model, e.keywords_set_version, e.digest, e.status, e.created_at,
            CASE WHEN jsonb_typeof(ut.taxonomy->'field_of_law') = 'array' THEN jsonb_array_length(ut.taxonomy->'field_of_law') ELSE 0 END AS fol_count,
            CASE WHEN jsonb_typeof(ut.taxonomy->'doctrines') = 'array' THEN jsonb_array_length(ut.taxonomy->'doctrines') ELSE 0 END AS doctrines_count,
            CASE WHEN jsonb_typeof(ut.taxonomy->'distinguishing_factors') = 'array' THEN jsonb_array_length(ut.taxonomy->'distinguishing_factors') ELSE 0 END AS df_count
     FROM unit_enrichments e
     LEFT JOIN unit_taxonomy ut ON ut.unit_id = e.unit_id AND ut.enrichment_id = e.id
     WHERE ($1::TEXT IS NULL OR e.law_id = $1)
     ORDER BY e.id`,
    [law ?? null]
  );
  console.log(['id','unit_id','text_version_id','law_id','label','prompt_version','model','keywords_set_version','digest','status','created_at','field_of_law_count','doctrines_count','distinguishing_factors_count'].join(','));
  for (const r of rows) {
    const vals = [r.id, r.unit_id, r.text_version_id, r.law_id, JSON.stringify(r.label ?? ''), r.prompt_version, r.model, r.keywords_set_version, JSON.stringify(r.digest ?? ''), r.status, r.created_at.toISOString(), r.fol_count, r.doctrines_count, r.df_count];
    console.log(vals.join(','));
  }
}

async function main() {
  const argv = await yargs(hideBin(process.argv))
    .option('format', { type: 'string', choices: ['ndjson', 'csv'], default: 'ndjson' })
    .option('law', { type: 'string' })
    .help().argv;

  if (argv.format === 'ndjson') await exportNdjson({ law: argv.law });
  else await exportCsv({ law: argv.law });

  await pool.end();
}

main().catch(async (err) => {
  console.error('export failed:', err);
  await pool.end();
  process.exit(1);
});
