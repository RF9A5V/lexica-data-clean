import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { pool } from '../src/db/connection.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const ddlPath = path.resolve(__dirname, '../src/db/ddl.sql');
  const sql = fs.readFileSync(ddlPath, 'utf8');
  await pool.query(sql);
  console.log('DDL applied successfully.');
  await pool.end();
}

main().catch(async (err) => {
  console.error('init-db failed:', err);
  await pool.end();
  process.exit(1);
});
