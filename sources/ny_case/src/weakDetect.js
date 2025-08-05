import { Client } from 'pg';
import cliProgress from 'cli-progress';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

dotenv.config({ path: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../.env') });

const MIN_OPINION_LEN = 100;
const RATIO_THRESHOLD = 0.3;
const BATCH_SIZE = 100;

async function main() {
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  // Get all opinion IDs first
  const { rows: ids } = await pg.query('SELECT id FROM opinions');

  const bar = new cliProgress.SingleBar({
    format: 'WeakDetect |{bar}| {percentage}% | {value}/{total} Opinions',
    hideCursor: true
  }, cliProgress.Presets.shades_classic);
  bar.start(ids.length, 0);

  let batch = [];
  let processed = 0;

  for (const { id } of ids) {
    // Fetch opinion_paragraphs and opinion_sentences for this opinion
    const { rows: paraRows } = await pg.query('SELECT raw_text FROM opinion_paragraphs WHERE opinion_id = $1', [id]);
    const { rows: sentRows } = await pg.query('SELECT sentence_text FROM opinion_sentences WHERE opinion_id = $1', [id]);
    const opinionText = paraRows.map(r => r.raw_text || '').join(' ');
    const sentenceText = sentRows.map(r => r.sentence_text || '').join(' ');
    const opinionLen = opinionText.replace(/\s+/g, '').length;
    const sentenceLen = sentenceText.replace(/\s+/g, '').length;
    const ratio = opinionLen / Math.max(sentenceLen, 1);
    const substantial = (opinionLen >= MIN_OPINION_LEN && ratio >= RATIO_THRESHOLD);
    batch.push({ id, substantial });

    if (batch.length >= BATCH_SIZE) {
      await updateBatch(pg, batch);
      processed += batch.length;
      bar.update(processed);
      batch = [];
    }
  }
  // Final batch
  if (batch.length > 0) {
    await updateBatch(pg, batch);
    processed += batch.length;
    bar.update(processed);
  }

  bar.stop();
  await pg.end();
}

async function updateBatch(pg, batch) {
  // Use VALUES for efficient bulk update, cast substantial to boolean
  const values = batch.map((o, i) => `($${i * 2 + 1}, $${i * 2 + 2}::boolean)`).join(', ');
  const params = batch.flatMap(o => [o.id, o.substantial]);
  const sql = `
    UPDATE opinions AS o SET substantial = v.substantial
    FROM (VALUES ${values}) AS v(id, substantial)
    WHERE o.id = v.id::integer
  `;
  await pg.query(sql, params);
}

main().catch(e => {
  console.error('[weakDetect] Fatal error:', e);
  process.exit(1);
});
