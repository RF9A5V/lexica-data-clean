import path from "path";
import { parseValidationLogs } from "./log_parser.js";
import { getNdjsonEntry } from "./ndjson_lookup.js";
import { getXmlElementByIdentifier } from "./xml_lookup.js";
import { estimateTokens } from "./token_estimator.js";
import { renderProgressBar } from "./progress_bar.js";
import fs from "fs/promises";
import fsSync from "fs"; // for createWriteStream

const PARSED_DIR = "/Users/byuugulbary/Projects/lexica/lexica-data/data/parsed";
const XML_DIR = "/Users/byuugulbary/Projects/lexica/lexica-data/data/xml";
const LOG_PATH = path.join(PARSED_DIR, "all_validation_logs.txt");

async function main() {
  // First, count total entries for progress bar
  let total = 0;
  for await (const _ of parseValidationLogs(LOG_PATH)) total++;
  let processed = 0;
  let currentTitle = null;
  const outputPath = path.join(PARSED_DIR, "diagnostic_logs.txt");
  const writeStream = fsSync.createWriteStream(outputPath, { encoding: "utf8" });
  for await (const { title, lineNum, logLine } of parseValidationLogs(LOG_PATH)) {
    processed++;
    renderProgressBar(processed, total);
    let entryLines = [];
    if (title !== currentTitle) {
      entryLines.push(`\n# ${title}\n`);
      currentTitle = title;
    }
    entryLines.push(logLine);
    // NDJSON lookup
    const ndjsonPath = path.join(PARSED_DIR, title, "section_text.ndjson");
    const ndjsonResult = await getNdjsonEntry(ndjsonPath, lineNum);
    if (ndjsonResult.error) {
      entryLines.push(`NDJSON entry: ${ndjsonResult.error}`);
      writeStream.write(entryLines.join("\n") + "\n\n");
      continue;
    }
    const entryStr = JSON.stringify(ndjsonResult.entry, null, 2);
    entryLines.push(`NDJSON entry:\n${entryStr}`);
    // XML lookup
    const identifier = ndjsonResult.entry.identifier;
    let xmlStr = "";
    if (identifier) {
      const titleNum = title.match(/title_(\d+)/)[1].padStart(2, "0");
      const xmlPath = path.join(XML_DIR, `usc${titleNum}.xml`);
      const xmlResult = await getXmlElementByIdentifier(xmlPath, identifier);
      if (xmlResult.error) {
        xmlStr = xmlResult.error;
      } else {
        xmlStr = xmlResult.xml;
      }
    } else {
      xmlStr = "No identifier in NDJSON entry.";
    }
    entryLines.push(`XML element:\n${xmlStr}`);
    // Token estimation
    const tokenCount = estimateTokens([logLine, entryStr, xmlStr].join("\n"));
    entryLines.push(`Estimated tokens: ${tokenCount}`);
    writeStream.write(entryLines.join("\n") + "\n\n");
  }
  writeStream.end();
  console.log(`\nDiagnostic results written to ${outputPath}`);
}

main();
