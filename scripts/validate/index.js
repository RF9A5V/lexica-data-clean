import fs from "fs/promises";
import path from "path";
import { markerCheck } from "./checks/marker_check.js";
import { identifierFormatCheck } from "./checks/identifier_format_check.js";
import { typeCheck } from "./checks/type_check.js";
import { contentTypeCheck } from "./checks/content_type_check.js";
import { contentCheck } from "./checks/content_check.js";
import { duplicateCheck } from "./checks/duplicate_check.js";
import { makeContentHashCheck } from "./checks/content_hash_check.js";
import { Logger } from "./utils/logger.js";

// Checks for new NDJSON schema: identifier, type, contentType, content
const checks = [markerCheck, identifierFormatCheck, typeCheck, contentTypeCheck, contentCheck]; // duplicateCheck is run after all lines read

async function validateFile(ndjsonPath, logPath) {
  const logger = new Logger(logPath);
  const seenIds = new Set();
  const contentHashCheck = makeContentHashCheck();
  const lines = (await fs.readFile(ndjsonPath, "utf8")).split("\n");
  // Parse all lines and attach lineNum for adjacency check
  const allData = [];
  let lineNum = 0;
  for (const line of lines) {
    lineNum++;
    if (!line.trim()) continue;
    let data;
    try {
      data = JSON.parse(line);
      data.lineNum = lineNum;
      allData.push(data);
    } catch (e) {
      logger.log(`[Line ${lineNum}] [parseError] Invalid JSON: ${e.message}`);
    }
  }
  // Now run checks for each element
  for (let idx = 0; idx < allData.length; idx++) {
    const data = allData[idx];
    for (const check of checks) {
      const result = check(data);
      if (result) {
        const idPart = data.identifier ? `[id: ${data.identifier}] ` : '';
        logger.log(`[Line ${data.lineNum}] ${idPart}[${check.name}] ${result}`);
      }
    }
    // Adjacency-based duplicate content check
    const hashResult = contentHashCheck(allData, idx);
    if (hashResult) {
      logger.log(`[Line ${data.lineNum}] [contentHashCheck] ${hashResult}`);
    }
    // For duplicate check (by identifier+type+contentType)
    if (data.identifier && data.type && data.contentType) {
      const key = `${data.identifier}|${data.type}|${data.contentType}`;
      if (seenIds.has(key)) {
        logger.log(`[Line ${data.lineNum}] [duplicateCheck] Duplicate identifier+type+contentType: ${key}`);
      } else {
        seenIds.add(key);
      }
    }
  }
  await logger.close();
}

import process from 'process';

async function main() {
  const parsedDir = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../../data/parsed");
  // CLI arg parsing: --title <n>
  let onlyTitle = null;
  const argv = process.argv.slice(2);
  const titleFlagIdx = argv.indexOf('--title');
  if (titleFlagIdx !== -1 && argv[titleFlagIdx + 1]) {
    onlyTitle = `title_${argv[titleFlagIdx + 1].padStart(2, '0')}`;
  }
  const titles = await fs.readdir(parsedDir);
  const allLogs = [];
  for (const titleDir of titles) {
    if (!/^title_\d+$/.test(titleDir)) continue;
    if (onlyTitle && titleDir !== onlyTitle) continue;
    const ndjsonPath = path.join(parsedDir, titleDir, "section_text.ndjson");
    const logPath = path.join(parsedDir, titleDir, "validation_log.txt");
    try {
      await validateFile(ndjsonPath, logPath);
      console.log(`Validated ${ndjsonPath}, log written to ${logPath}`);
    } catch (e) {
      console.error(`Error validating ${ndjsonPath}:`, e);
    }
  }
  // After all validations, combine all validation logs
  const combinedPath = path.join(parsedDir, "all_validation_logs.txt");
  const fsSync = await import('fs');
  const writeStream = fsSync.createWriteStream(combinedPath, { flags: 'w', encoding: 'utf8' });
  for (const titleDir of titles) {
    if (!/^title_\d+$/.test(titleDir)) continue;
    const logPath = path.join(parsedDir, titleDir, "validation_log.txt");
    try {
      const logContent = await fs.readFile(logPath, "utf8");
      if (logContent.trim()) {
        writeStream.write(`# ${titleDir}\n`);
        writeStream.write(logContent.trim() + '\n\n');
      }
    } catch (e) {
      // Ignore missing logs
    }
  }
  writeStream.end();
  await new Promise(resolve => writeStream.on('finish', resolve));
  console.log(`Combined validation log written to ${combinedPath}`);
}

if (import.meta.url.endsWith("index.js")) {
  main();
}
