import { Client } from "pg";
import { fetchEmbeddings, toPgvectorString } from "../../federal/embed/embedding.js";
import cliProgress from "cli-progress";
import dotenv from "dotenv";
import path, { dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

async function main() {
  const BATCH_SIZE = 100;
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  const { rows: opinionSentences } = await pg.query("SELECT id, sentence_text FROM opinion_sentences WHERE embedding_vector IS NULL");

  let sentenceBatch = [];

  // Progress bar setup
  const total = opinionSentences.length;
  const bar = new cliProgress.SingleBar({
    format: 'Embedding Progress |{bar}| {percentage}% || {value}/{total} sentences',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });
  bar.start(total, 0);
  let processed = 0;

  for (const sentence of opinionSentences) {
    sentenceBatch.push(sentence);

    if (sentenceBatch.length >= BATCH_SIZE) {
      const embeddings = await fetchEmbeddings(sentenceBatch.map(s => s.sentence_text));
      // Build VALUES clause and parameter array
      const valuesClause = sentenceBatch.map((s, i) => `($${i*2+1}, $${i*2+2})`).join(', ');
      const params = [];
      for (let i = 0; i < sentenceBatch.length; i++) {
        params.push(sentenceBatch[i].id, toPgvectorString(embeddings[i]));
      }
      const sql = `
        UPDATE opinion_sentences AS os
        SET embedding_vector = v.embedding::vector
        FROM (VALUES ${valuesClause}) AS v(id, embedding)
        WHERE os.id = v.id::integer
      `;
      try {
        await pg.query(sql, params);
      } catch (err) {
        bar.stop();
        console.error(`[BATCH ERROR] Failed to update batch`);
        console.error(err);
        pg.end();
        return;
      }
      processed += sentenceBatch.length;
      bar.update(processed);
      sentenceBatch = [];
    }
  }

  // Handle any remaining sentences in the last batch
  if (sentenceBatch.length > 0) {
    const embeddings = await fetchEmbeddings(sentenceBatch.map(s => s.sentence_text));
    const valuesClause = sentenceBatch.map((s, i) => `($${i*2+1}, $${i*2+2})`).join(', ');
    const params = [];
    for (let i = 0; i < sentenceBatch.length; i++) {
      params.push(sentenceBatch[i].id, toPgvectorString(embeddings[i]));
    }
    const sql = `
      UPDATE opinion_sentences AS os
      SET embedding_vector = v.embedding::vector
      FROM (VALUES ${valuesClause}) AS v(id, embedding)
      WHERE os.id = v.id::integer
    `;
    try {
      await pg.query(sql, params);
    } catch (err) {
      bar.stop();
      console.error(`[FINAL BATCH ERROR] Failed to update final batch`);
      console.error(err);
      pg.end();
      return;
    }
    processed += sentenceBatch.length;
    bar.update(processed);
  }

  bar.stop();
  await pg.end();
}

await main();