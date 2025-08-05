import { Client } from "pg";
import { fetchEmbeddings, toPgvectorString } from "../../federal/embed/embedding.js";
import cliProgress from "cli-progress";

async function embedOpinionParagraphs() {
  const BATCH_SIZE = 100;
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  const { rows: opinionParagraphs } = await pg.query("SELECT id, raw_text FROM opinion_paragraphs WHERE embedding IS NULL");

  let paragraphBatch = [];

  // Progress bar setup
  const total = opinionParagraphs.length;
  const bar = new cliProgress.SingleBar({
    format: 'Embedding Progress |{bar}| {percentage}% || {value}/{total} paragraphs',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });
  bar.start(total, 0);
  let processed = 0;

  for (const paragraph of opinionParagraphs) {
    paragraphBatch.push(paragraph);

    if (paragraphBatch.length >= BATCH_SIZE) {
      const embeddings = await fetchEmbeddings(paragraphBatch.map(p => p.raw_text));
      // Build VALUES clause and parameter array
      const valuesClause = paragraphBatch.map((p, i) => `($${i*2+1}, $${i*2+2})`).join(', ');
      const params = [];
      for (let i = 0; i < paragraphBatch.length; i++) {
        params.push(paragraphBatch[i].id, toPgvectorString(embeddings[i]));
      }
      const sql = `
        UPDATE opinion_paragraphs AS op
        SET embedding = v.embedding::vector
        FROM (VALUES ${valuesClause}) AS v(id, embedding)
        WHERE op.id = v.id::integer
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
      processed += paragraphBatch.length;
      bar.update(processed);
      paragraphBatch = [];
    }
  }

  // Handle any remaining paragraphs in the last batch
  if (paragraphBatch.length > 0) {
    const embeddings = await fetchEmbeddings(paragraphBatch.map(p => p.raw_text));
    const valuesClause = paragraphBatch.map((p, i) => `($${i*2+1}, $${i*2+2})`).join(', ');
    const params = [];
    for (let i = 0; i < paragraphBatch.length; i++) {
      params.push(paragraphBatch[i].id, toPgvectorString(embeddings[i]));
    }
    const sql = `
      UPDATE opinion_paragraphs AS op
      SET embedding = v.embedding::vector
      FROM (VALUES ${valuesClause}) AS v(id, embedding)
      WHERE op.id = v.id::integer
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
    processed += paragraphBatch.length;
    bar.update(processed);
  }

  bar.stop();
  await pg.end();
}

await embedOpinionParagraphs();

