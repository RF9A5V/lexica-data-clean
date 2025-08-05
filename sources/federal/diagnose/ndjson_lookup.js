import fs from "fs/promises";

// Retrieves NDJSON entry by 1-based line number
export async function getNdjsonEntry(ndjsonPath, lineNum) {
  const file = await fs.readFile(ndjsonPath, "utf8");
  const lines = file.split("\n");
  if (lineNum < 1 || lineNum > lines.length) {
    return { error: `Line ${lineNum} out of range in ${ndjsonPath}` };
  }
  const entry = lines[lineNum - 1];
  if (!entry.trim()) {
    return { error: `Line ${lineNum} is empty in ${ndjsonPath}` };
  }
  try {
    return { entry: JSON.parse(entry) };
  } catch (e) {
    return { error: `Invalid JSON at line ${lineNum}: ${e.message}` };
  }
}
