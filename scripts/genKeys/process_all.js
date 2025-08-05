import { spawn } from 'child_process';
import path from 'path';

// Helper to run a script and return a promise
function runScript(command, args, cwd = process.cwd()) {
  return new Promise((resolve, reject) => {
    const proc = spawn(command, args, { stdio: 'inherit', cwd });
    proc.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with code ${code}`));
    });
  });
}

// Main function: process all section_text.ndjson files in data/parsed/title_{titleNum}/
import fs from 'fs';

async function main() {
  const parsedDir = path.join('data', 'parsed');
  const entries = await fs.promises.readdir(parsedDir, { withFileTypes: true });
  for (const entry of entries) {
    if (!entry.isDirectory() || !entry.name.startsWith('title_')) continue;
    const ndjsonPath = path.join(parsedDir, entry.name, 'section_text.ndjson');
    if (fs.existsSync(ndjsonPath)) {
      console.log(`[INFO] Processing ${ndjsonPath}`);
      await runScript('node', [path.join('scripts', 'genKeys', 'generateKeys.js'), ndjsonPath]);
    }
  }
  console.log('All processing done!');
}

main();
