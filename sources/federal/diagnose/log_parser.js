import fs from "fs/promises";

// Parses all_validation_logs.txt and yields { title, lineNum, logLine }
export async function* parseValidationLogs(logFilePath) {
  const file = await fs.readFile(logFilePath, "utf8");
  const lines = file.split("\n");
  let currentTitle = null;
  for (const line of lines) {
    const titleMatch = line.match(/^# (title_\d+)/);
    if (titleMatch) {
      currentTitle = titleMatch[1];
      continue;
    }
    const logMatch = line.match(/^\[Line (\d+)\](.*)$/);
    if (logMatch && currentTitle) {
      yield {
        title: currentTitle,
        lineNum: parseInt(logMatch[1], 10),
        logLine: line.trim(),
      };
    }
  }
}
