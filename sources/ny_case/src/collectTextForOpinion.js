import { Client } from "pg";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

async function pullTextForOpinion(opinionId) {
  const pg = new Client({ connectionString: 'postgresql://localhost/ny_court_of_appeals' });
  await pg.connect();

  const res = await pg.query('SELECT raw_text FROM opinion_paragraphs WHERE opinion_id = $1 ORDER BY id ASC', [opinionId]);
  
  await pg.end();

  return res.rows.map(row => cleanText(row.raw_text));
}

function cleanText(text) {

  // Remove everything contained in parentheses
  return text
    .replace(/\n/g, ' ')
    .replace(/\*\d+/g, '')
    .replace(/\*/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

async function main() {
  // Get opinion ID from command line argument
  const opinionId = process.argv[2];

  if (!opinionId) {
    console.error('Please provide an opinion ID as a command line argument.');
    process.exit(1);
  }

  const opinionText = await pullTextForOpinion(opinionId);
  console.log(opinionText.join('\n'));
}

// main();

export {
  pullTextForOpinion,
  cleanText
};