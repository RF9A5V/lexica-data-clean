import fs from "fs/promises";

export class Logger {
  constructor(filePath) {
    this.filePath = filePath;
    this.lines = [];
  }
  log(line) {
    this.lines.push(line);
  }
  async close() {
    await fs.writeFile(this.filePath, this.lines.join("\n"), "utf8");
  }
}
