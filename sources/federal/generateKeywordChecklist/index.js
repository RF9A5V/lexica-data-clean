import dotenv from "dotenv";
import { Client } from "pg";
import { setupChecklist } from "./setupChecklist.js";
import { generateKeywords } from "./generateKeywords.js";

dotenv.config();

// Get --title=num flag, extract num
const title = process.argv.find(arg => arg.startsWith('--title='));
const titleNum = title ? title.split('=')[1] : null;

if(title && !titleNum) {
  throw new Error('Invalid --title flag. Please provide a number after the flag.');
}

const EMBEDDING_DB_URL = process.env.EMBEDDING_DB_URL;
if (!EMBEDDING_DB_URL) {
  throw new Error('EMBEDDING_DB_URL is not set in environment variables. Please set it in your .env file.');
}
console.log(`[DEBUG] Using EMBEDDING_DB_URL: ${EMBEDDING_DB_URL}`);

const pg = new Client({ connectionString: EMBEDDING_DB_URL });
await pg.connect();

await setupChecklist(pg);
await generateKeywords(pg, titleNum);

await pg.end();
