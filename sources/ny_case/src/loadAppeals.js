// Was used for populating the initial case dataset for NY Court of Appeals
// Should refactor to generalize

import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import ndjson from "ndjson";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

const NY_STATE_APPEALS_DB = process.env.NY_STATE_APPEALS_DB;
if (!NY_STATE_APPEALS_DB) {
  throw new Error('NY_STATE_APPEALS_DB is not set in environment variables. Please set it in your .env file.');
}
console.log(`[DEBUG] Using NY_STATE_APPEALS_DB: ${NY_STATE_APPEALS_DB}`);

const pg = new Client({ connectionString: NY_STATE_APPEALS_DB });
await pg.connect();

const filePath = path.join(__dirname, 'clusters.ndjson');
const fileStream = fs.createReadStream(filePath);
const stream = fileStream.pipe(ndjson.parse());

let count = 0;
let insertCasesBatch = [];
let insertOpinionsBatch = [];

// --- BEGIN TRANSACTION ---
await pg.query('BEGIN');

for await (const cluster of stream) {
  count++;

  let { 
    case_name, 
    case_name_full, 
    citations, 
    date_created, 
    date_filed, 
    date_filed_is_approximate, 
    date_modified,
    sub_opinions,
    citation_count,
    opinions // <-- If you have opinions in the cluster, batch them here
  } = cluster;

  citations = JSON.stringify(citations);
  sub_opinions = JSON.stringify(sub_opinions);

  insertCasesBatch.push([
    case_name,
    case_name_full,
    citations,
    date_created,
    date_filed,
    date_filed_is_approximate,
    date_modified,
    sub_opinions,
    citation_count
  ]);

  // Extract and batch insert opinions from sub_opinions array
  let parsedSubOpinions = [];
  try {
    parsedSubOpinions = Array.isArray(sub_opinions) ? sub_opinions : JSON.parse(sub_opinions);
  } catch (e) {
    parsedSubOpinions = [];
  }
  for (const opinionUrl of parsedSubOpinions) {
    insertOpinionsBatch.push([
      count, // Uses the current case's serial id if inserting serially, otherwise you may need to fetch the id after insert
      opinionUrl,
      null // binding_type is null for now
    ]);
  }


  if (insertCasesBatch.length >= 1000) {
    const rowSize = insertCasesBatch[0].length;
    const valueStrings = insertCasesBatch.map(
      (row, i) => `(${row.map((_, j) => `$${i * rowSize + j + 1}`).join(',')})`
    );
    const query = `INSERT INTO cases (case_name, case_name_full, citations, date_created, date_filed, date_filed_is_approximate, date_modified, sub_opinions, citation_count) VALUES ${valueStrings.join(',')}`;
    const flatValues = insertCasesBatch.flat();
    await pg.query(query, flatValues);

    console.log(`[DEBUG] Inserted ${insertCasesBatch.length} cases.`);
    insertCasesBatch = [];
  }

  if (insertOpinionsBatch.length >= 1000) {
    const rowSize = insertOpinionsBatch[0].length;
    const valueStrings = insertOpinionsBatch.map(
      (row, i) => `(${row.map((_, j) => `$${i * rowSize + j + 1}`).join(',')})`
    );
    const query = `INSERT INTO opinions (case_id, url, binding_type) VALUES ${valueStrings.join(',')}`;
    const flatValues = insertOpinionsBatch.flat();
    await pg.query(query, flatValues);
    console.log(`[DEBUG] Inserted ${insertOpinionsBatch.length} opinions.`);
    insertOpinionsBatch = [];
  }

  if(count % 1000 === 0) {
    console.log(`[DEBUG] Processed ${count} clusters.`);
  }
}

if(insertCasesBatch.length > 0) {
  const rowSize = insertCasesBatch[0].length;
  const valueStrings = insertCasesBatch.map(
    (row, i) => `(${row.map((_, j) => `$${i * rowSize + j + 1}`).join(',')})`
  );
  const query = `INSERT INTO cases (case_name, case_name_full, citations, date_created, date_filed, date_filed_is_approximate, date_modified, sub_opinions, citation_count) VALUES ${valueStrings.join(',')}`;
  const flatValues = insertCasesBatch.flat();
  await pg.query(query, flatValues);
  console.log(`[DEBUG] Inserted ${insertCasesBatch.length} cases.`);
}

if(insertOpinionsBatch.length > 0) {
  const rowSize = insertOpinionsBatch[0].length;
  const valueStrings = insertOpinionsBatch.map(
    (row, i) => `(${row.map((_, j) => `$${i * rowSize + j + 1}`).join(',')})`
  );
  const query = `INSERT INTO opinions (case_id, url, binding_type) VALUES ${valueStrings.join(',')}`;
  const flatValues = insertOpinionsBatch.flat();
  await pg.query(query, flatValues);
  console.log(`[DEBUG] Inserted ${insertOpinionsBatch.length} opinions.`);
}

// --- COMMIT TRANSACTION ---
await pg.query('COMMIT');
await pg.end();

