import { spawn } from 'child_process';
import { createWriteStream } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PY_SCRIPT = path.join(__dirname, 'pdf_parse.py');

/**
 * Spawn the Python pdfplumber extractor against `pdfPath` and stream NDJSON
 * records (one JSON object per line). Each record is parsed and yielded.
 * Optionally tees raw NDJSON output to `rawOutputPath` for later replay.
 */
export async function* extractPdfPages(pdfPath, { rawOutputPath = null, pythonBin = 'python3' } = {}) {
  const proc = spawn(pythonBin, [PY_SCRIPT, pdfPath], { stdio: ['ignore', 'pipe', 'pipe'] });

  const rawStream = rawOutputPath ? createWriteStream(rawOutputPath) : null;
  let buffer = '';
  let stderr = '';

  proc.stderr.on('data', d => { stderr += d.toString(); });

  const lines = [];
  let finished = false;
  let waiter = null;

  proc.stdout.on('data', chunk => {
    if (rawStream) rawStream.write(chunk);
    buffer += chunk.toString();
    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      if (line.trim()) lines.push(line);
    }
    if (waiter) { const w = waiter; waiter = null; w(); }
  });

  proc.stdout.on('end', () => {
    if (rawStream) rawStream.end();
    finished = true;
    if (waiter) { const w = waiter; waiter = null; w(); }
  });

  while (true) {
    while (lines.length) {
      const line = lines.shift();
      try {
        yield JSON.parse(line);
      } catch (err) {
        throw new Error(`bad JSON from pdf_parse.py: ${err.message}\nline: ${line.slice(0, 200)}`);
      }
    }
    if (finished) break;
    await new Promise(resolve => { waiter = resolve; });
  }

  const exitCode = await new Promise(resolve => proc.on('close', resolve));
  if (exitCode !== 0) {
    throw new Error(`pdf_parse.py exited ${exitCode}\nstderr:\n${stderr}`);
  }
}
