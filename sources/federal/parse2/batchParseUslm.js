// batchParseUslm.js
// Batch USLM XML parser: loops over all XML files in data/xml and outputs NDJSON to parsed/title_XX/section_text.ndjson

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { exec as _exec } from 'child_process';
import { promisify } from 'util';
const exec = promisify(_exec);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const XML_DIR = path.resolve(__dirname, '../../data/xml');
const PARSED_DIR = path.resolve(__dirname, '../../data/parsed');
const PARSER_SCRIPT = path.resolve(__dirname, 'parseUslm.js');

function padTitle(num) {
  return num.length === 1 ? '0' + num : num;
}

async function main() {
  const entries = await fs.readdir(XML_DIR);
  const xmlFiles = entries.filter(f => f.endsWith('.xml'));
  for (const xmlFile of xmlFiles) {
    // Extract title number (uscXX.xml or uscXXa.xml)
    const match = xmlFile.match(/^usc(\d{1,2})([a-zA-Z]*)\.xml$/);
    if (!match) continue;
    const titleNum = padTitle(match[1]);
    let titleDir = `title_${titleNum}`;
    if (match[2]) titleDir += match[2].toLowerCase(); // handle e.g. usc05A.xml -> title_05a
    const outDir = path.join(PARSED_DIR, titleDir);
    const outPath = path.join(outDir, 'section_text.ndjson');
    await fs.mkdir(outDir, { recursive: true });
    console.log(`Parsing ${xmlFile} -> ${outPath}`);
    try {
      await exec(`node "${PARSER_SCRIPT}" "${path.join(XML_DIR, xmlFile)}" "${outPath}"`);
    } catch (err) {
      console.error(`Error parsing ${xmlFile}:`, err.stderr || err.message);
    }
  }
}

if (import.meta && import.meta.url && process.argv[1] === new URL(import.meta.url).pathname) {
  main().catch(e => { console.error(e); process.exit(1); });
}
