import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { pool } from '../src/db/connection.js';
import { KeywordsSeedSchema } from '../src/util/schema.js';

function slugify(name) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const argv = await yargs(hideBin(process.argv))
    .option('file', { type: 'string', demandOption: false, describe: 'Path to keywords JSON' })
    .help().argv;

  const filePath = argv.file
    ? path.resolve(process.cwd(), argv.file)
    : path.resolve(__dirname, '../keywords.json');

  const raw = fs.readFileSync(filePath, 'utf8');
  const payload = JSON.parse(raw);
  const data = KeywordsSeedSchema.parse(payload);

  let inserted = 0, updated = 0, skipped = 0;
  for (const name of data.keywords) {
    const slug = slugify(name);
    const res = await pool.query(
      `INSERT INTO keywords (name, slug, keywords_set_version)
       VALUES ($1,$2,$3)
       ON CONFLICT (slug)
       DO UPDATE SET name = EXCLUDED.name, keywords_set_version = EXCLUDED.keywords_set_version
       RETURNING (xmax = 0) AS inserted`,
      [name, slug, data.version]
    );
    const row = res.rows[0];
    if (row?.inserted) inserted++; else updated++;
  }

  console.log(`Seed complete. inserted=${inserted} updated=${updated} skipped=${skipped}`);
  await pool.end();
}

main().catch(async (err) => {
  console.error('seed-keywords failed:', err);
  await pool.end();
  process.exit(1);
});
