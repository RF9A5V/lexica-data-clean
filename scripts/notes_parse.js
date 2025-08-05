import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { DOMParser } from "xmldom-qsa";
import { extractNoteData } from "../utils/extractNoteData.js";
import { normalizeNoteRefs } from "../utils/normalizeNoteRefs.js";
import { writeNotesJson } from "../utils/writeNotesJson.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const uscDir = path.join(__dirname, "../data/xml");
  let files = [];
  const argFile = process.argv[2];
  if (argFile) {
    files = [argFile];
    console.log(`Processing only file: ${argFile}`);
  } else {
    files = await fs.readdir(uscDir);
  }
  for (const xmlFile of files) {
    if (!xmlFile.endsWith('.xml')) continue;
    if (/usc\d+A\.xml$/i.test(xmlFile)) continue; // skip appendix files
    const xmlPath = path.join(uscDir, xmlFile);
    const fileContent = await fs.readFile(xmlPath, "utf8");
    const doc = new DOMParser().parseFromString(fileContent, "text/xml");
    const notes = Array.from(doc.getElementsByTagName('note'));
    let titleNum = "unknown";
    const docNumberElem = doc.querySelector("docNumber");
    if (docNumberElem && docNumberElem.textContent) {
      titleNum = docNumberElem.textContent.trim().padStart(2, "0");
    } else {
      // Fallback: extract from filename (e.g., usc01.)
      const m = xmlFile.match(/^usc(\d+)/i);
      if (m) titleNum = m[1].padStart(2, "0");
    }
    const notesArr = notes.map(note => extractNoteData(note));
    normalizeNoteRefs(notesArr, titleNum);
    // Write to content/parsed/title_XX/notes_text.json
    const parsedDir = path.join(__dirname, '../data/parsed', `title_${titleNum}`);
    await fs.mkdir(parsedDir, { recursive: true });
    const outPath = path.join(parsedDir, 'notes_text.json');
    await fs.writeFile(outPath, JSON.stringify(notesArr, null, 2), 'utf8');
    console.log(`Wrote ${notesArr.length} notes to ${outPath}`);
  }
}

main().catch(e => { console.error(e); process.exit(1); });