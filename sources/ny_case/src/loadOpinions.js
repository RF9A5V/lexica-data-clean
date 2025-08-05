// 1. Imports and Config
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { DOMParser } from "xmldom-qsa";

// 2. Script-wide constants and variables
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

const NY_STATE_APPEALS_DB = process.env.NY_STATE_APPEALS_DB;
const COURT_LISTENER_API_KEY = process.env.COURT_LISTENER_API_KEY;
const pg = new Client({ connectionString: NY_STATE_APPEALS_DB });
const courtListenerHeaders = {
  "Authorization": `Token ${COURT_LISTENER_API_KEY}`,
  "Content-Type": "application/json"
};
const targetReporters = ['N.Y.', 'N.Y.2d', 'N.Y.3d', 'N.E.', 'N.E.2d', 'N.E.3d'];
// const bindingTypes = ["015unamimous", "020lead"];
const countFilePath = path.join(__dirname, 'count.txt');

let count = 0;
let insertOpinionsBatch = [];
let missingBindingOpinions = [];

// 3. Utility functions
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

async function getOpinionWithExponentialBackoff(url) {
  await sleep(1500);
  const maxRetries = 5;
  let attempt = 0;
  let data;
  console.log(`[DEBUG] Fetching opinion at ${url}`);
  while (attempt < maxRetries) {
    try {
      const response = await fetch(url, {
        method: "GET",
        headers: courtListenerHeaders
      });
      if (!response.ok) {
        throw new Error(`[HTTP ${response.status}] ${response.statusText}`);
      }
      data = await response.json();
      break; // Success
    } catch (err) {
      attempt++;
      const delay = Math.min(16000, 1000 * Math.pow(2, attempt - 1)); // 1s, 2s, 4s, 8s, 16s
      console.error(`[RETRY] Attempt ${attempt} failed for ${url}:`, err.message || err);
      if (attempt >= maxRetries) {
        console.error(`[ERROR] Giving up after ${maxRetries} attempts:`, url);
        await exitWithLog();
      }
      console.log(`[RETRY] Waiting ${delay / 1000}s before next attempt...`);
      await sleep(delay);
    }
  }

  return data;
}

async function clearOpinionBuffer() {
  const rowSize = insertOpinionsBatch[0].length;
  const valueStrings = insertOpinionsBatch.map(
    (row, i) => `(${row.map((_, j) => `$${i * rowSize + j + 1}`).join(',')})`
  );
  const insertQuery = `
    INSERT INTO opinion_paragraphs (case_id, raw_text)
    VALUES ${valueStrings.join(',')}
    ON CONFLICT DO NOTHING;
    `

  const flatValues = insertOpinionsBatch.flat();
  await pg.query(insertQuery, flatValues);

  insertOpinionsBatch = [];

  console.log(`[DEBUG] Inserted ${insertOpinionsBatch.length} opinions.`);
}

function writeCountToFile(count) {
  fs.writeFileSync(countFilePath, String(count));
}

// 4. Cleanup/exit functions
async function exitWithLog() {
  await flushParagraphInserts(insertParagraphsBatch);
  await flushBindingUpdates(updateBindingBatch);
  writeCountToFile(count);
  const missingBindingOpinionsFilePath = path.join(__dirname, 'missing_binding_opinions.txt');
  fs.writeFileSync(missingBindingOpinionsFilePath, missingBindingOpinions.join('\n'));
  if (missingBindingOpinions.length > 0) {
    console.log(`[DEBUG] Missing binding opinions: ${missingBindingOpinions.join(', ')}`);
  }
  console.log(`[DEBUG] Processed ${count} cases.`);
  pg.end();
  process.exit(0);
}

// 5. Main processing function
async function processOpinions() {
  await pg.connect();
  try {
    let localCount = 0;
    let updateBindingBatch = [];
    let insertParagraphsBatch = [];
    const BATCH_SIZE = 100;
    // Query all opinions (with their parent case) where the case has a target reporter
    const query = `
      SELECT opinions.id AS opinion_id, opinions.url, opinions.binding_type, cases.id AS case_id, cases.citations
      FROM opinions
      JOIN cases ON opinions.case_id = cases.id
      WHERE EXISTS (
        SELECT 1
        FROM jsonb_array_elements(cases.citations) AS elem
        WHERE elem->>'reporter' = ANY($1)
      ) AND binding_type IS NULL
    `;
    const { rows: opinions } = await pg.query(query, [targetReporters]);
    console.log(`[DEBUG] Found ${opinions.length} opinions.`);

    // Progress bar setup
    const cliProgress = await import('cli-progress');
    const bar = new cliProgress.SingleBar({
      format: '[{bar}] {percentage}% | {value}/{total} | ETA: {eta}s',
      hideCursor: true,
      clearOnComplete: true,
    }, cliProgress.Presets.shades_classic);
    bar.start(opinions.length, 0);

    for (const [idx, opinionRow] of opinions.entries()) {
      const { opinion_id, url } = opinionRow;
      let newBindingType = null;
      bar.update(idx + 1);
      let paragraphs = [];
      try {
        const data = await getOpinionWithExponentialBackoff(url);
        const { plain_text, html, html_with_citations, type } = data;
        newBindingType = type || null;
        if (html_with_citations) {
          const doc = new DOMParser().parseFromString(html_with_citations, 'text/xml');
          const paraNodes = doc.querySelectorAll('p');
          for (let paragraph of paraNodes) {
            paragraphs.push(paragraph.textContent);
          }
          console.log(`[DEBUG] Found ${paragraphs.length} paragraphs for opinion ${opinion_id}.`);
        } else if (html || plain_text) {
          // fallback: treat as a single paragraph, but exit for manual inspection
          console.log(`[DEBUG] Found HTML or Text but no html_with_citations for opinion ${opinion_id}, exiting for manual inspection.`);
          await exitWithLog();
        } else {
          console.log(`[DEBUG] No usable content for opinion ${opinion_id}`);
          await exitWithLog();
        }
      } catch (e) {
        console.error(`[ERROR] Failed to fetch/parse opinion at ${url}:`, e);
        missingBindingOpinions.push(opinion_id);
        continue;
      }
      // Batch update binding_type
      updateBindingBatch.push({ id: opinion_id, binding_type: newBindingType });
      // Batch insert opinion_paragraphs
      for (const raw_text of paragraphs) {
        if (raw_text && raw_text.trim().length > 0) {
          insertParagraphsBatch.push({ opinion_id, raw_text });
        }
      }
      localCount++;
      // Flush batches
      if (updateBindingBatch.length >= BATCH_SIZE) {
        await flushBindingUpdates(updateBindingBatch);
        updateBindingBatch = [];
      }
      if (insertParagraphsBatch.length >= BATCH_SIZE) {
        await flushParagraphInserts(insertParagraphsBatch);
        insertParagraphsBatch = [];
      }
      if (localCount % 100 === 0) {
        console.log(`[DEBUG] Processed ${localCount} opinions.`);
      }
    }
    // Final flush
    if (updateBindingBatch.length > 0) await flushBindingUpdates(updateBindingBatch);
    if (insertParagraphsBatch.length > 0) await flushParagraphInserts(insertParagraphsBatch);
    bar.stop();
  } finally {
    pg.end();
  }
}

// Helper to batch update binding_type
async function flushBindingUpdates(batch) {
  if (batch.length === 0) return;
  const query = `UPDATE opinions SET binding_type = data.binding_type FROM (VALUES ${batch.map((_,i)=>`($${i*2+1}::INTEGER,$${i*2+2})`).join(',')}) AS data(id, binding_type) WHERE opinions.id = data.id`;
  const values = batch.flatMap(row => [row.id, row.binding_type]);
  await pg.query(query, values);
}
// Helper to batch insert paragraphs
async function flushParagraphInserts(batch) {
  if (batch.length === 0) return;
  const query = `INSERT INTO opinion_paragraphs (opinion_id, raw_text) VALUES ${batch.map((_,i)=>`($${i*2+1},$${i*2+2})`).join(',')}`;
  const values = batch.flatMap(row => [row.opinion_id, row.raw_text]);
  await pg.query(query, values);
}


// 6. Script entry point
processOpinions();