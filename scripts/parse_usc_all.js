import { spawn } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

function runScript(script, args = []) {
  return new Promise((resolve, reject) => {
    const proc = spawn("node", [script, ...args], { stdio: "inherit" });
    proc.on("exit", code => {
      if (code === 0) resolve();
      else reject(new Error(`${script} exited with code ${code}`));
    });
  });
}

async function main() {
  const args = process.argv.slice(2);
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const baseDir = __dirname;

  const sectionScript = path.join(baseDir, "section_parse.js");
  const notesScript = path.join(baseDir, "notes_parse.js");

  console.log("=== Parsing sections ===");
  await runScript(sectionScript, args);

  console.log("=== Parsing notes ===");
  await runScript(notesScript, args);

  console.log("=== All parsing done ===");
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
