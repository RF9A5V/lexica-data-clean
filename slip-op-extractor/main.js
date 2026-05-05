#!/usr/bin/env node
/**
 * Slip-Op Extractor — parse a single NY slip-opinion HTML or PDF file into
 * the bulk-ingest JSON contract.
 *
 * Usage:
 *   node main.js parse <file>             # parse one file → stdout JSON
 *   node main.js parse <file> --out=<dir> # write to <dir>/<basename>.json
 *   node main.js parse-dir <dir>          # parse every .html/.pdf in <dir>
 *
 * The output JSON is what you'd POST to co-collection's
 * `/admin/api/bulk-ingest/upload` endpoint. The `target_source_ref` field
 * indicates which source DB the case is destined for (ny_supreme /
 * ny_appellate / ny_trial), derived from the document's court line.
 */

import { readFile, readdir, mkdir, writeFile } from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

import { detectFormat } from './src/detect.js';
import { parseHtml } from './src/parser_html.js';
import { parsePdf } from './src/parser_pdf.js';
import { buildPayload } from './src/output.js';
import { sha256OfBuffer, PARSER_VERSION } from './src/shared.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function usage() {
  console.log(`
Usage:
  node main.js parse <file> [--out=<dir>]
  node main.js parse-dir <dir> [--out=<dir>]

Options:
  --out=<dir>    Write <basename>.json into <dir> (default: print to stdout
                 for single-file parse; ./output/ for parse-dir)
`.trim());
}

async function parseOne(filePath) {
  const buf = await readFile(filePath);
  const sourceSha256 = sha256OfBuffer(buf);
  const format = detectFormat(filePath, buf);

  let caseObj = null;
  if (format === 'html-modern' || format === 'html-legacy') {
    caseObj = parseHtml(buf.toString('utf8'), format, {});
  } else if (format === 'pdf') {
    caseObj = parsePdf(filePath);
  } else {
    throw new Error(`unrecognised input format for ${path.basename(filePath)}`);
  }

  if (!caseObj) {
    return { format, payload: buildPayload({ caseObj: null, sourceSha256 }), error: 'parse-returned-null' };
  }

  const payload = buildPayload({ caseObj, sourceSha256 });
  return { format, payload };
}

async function cmdParseOne(args) {
  const filePath = args._[1];
  if (!filePath) { usage(); process.exit(2); }
  const { format, payload, error } = await parseOne(path.resolve(filePath));
  if (error) {
    console.error(`ERROR (${format}): ${error}`);
    process.exit(1);
  }
  if (args.out) {
    await mkdir(args.out, { recursive: true });
    const outPath = path.join(args.out, basenameNoExt(filePath) + '.json');
    await writeFile(outPath, JSON.stringify(payload, null, 2));
    console.error(`wrote ${outPath} (format=${format}, target=${payload.target_source_db})`);
  } else {
    process.stdout.write(JSON.stringify(payload, null, 2) + '\n');
    console.error(`format=${format} target=${payload.target_source_db}`);
  }
}

async function cmdParseDir(args) {
  const dir = args._[1];
  if (!dir) { usage(); process.exit(2); }
  const outDir = args.out || path.join(process.cwd(), 'output');
  await mkdir(outDir, { recursive: true });

  const entries = await readdir(dir);
  const files = entries
    .filter(f => /\.(html?|pdf)$/i.test(f))
    .map(f => path.join(dir, f));

  let ok = 0, fail = 0;
  for (const f of files) {
    try {
      const { format, payload, error } = await parseOne(f);
      if (error) {
        console.error(`SKIP  ${path.basename(f)} (${format}) — ${error}`);
        fail++;
        continue;
      }
      const outPath = path.join(outDir, basenameNoExt(f) + '.json');
      await writeFile(outPath, JSON.stringify(payload, null, 2));
      console.error(`OK    ${path.basename(f)} → ${path.basename(outPath)} (${format}, target=${payload.target_source_db})`);
      ok++;
    } catch (e) {
      console.error(`FAIL  ${path.basename(f)} — ${e.message}`);
      fail++;
    }
  }
  console.error(`\nparsed ${ok} ok, ${fail} fail (parser ${PARSER_VERSION})`);
  if (fail > 0) process.exit(1);
}

function basenameNoExt(f) {
  return path.basename(f).replace(/\.(html?|pdf)$/i, '');
}

function parseArgs(argv) {
  const out = { _: [], out: null };
  for (const a of argv.slice(2)) {
    if (a.startsWith('--out=')) out.out = a.split('=')[1];
    else if (a === '-h' || a === '--help') out._.push('help');
    else out._.push(a);
  }
  return out;
}

const args = parseArgs(process.argv);
const cmd = args._[0];
if (cmd === 'parse') {
  cmdParseOne(args).catch(e => { console.error(e.stack || e.message); process.exit(1); });
} else if (cmd === 'parse-dir') {
  cmdParseDir(args).catch(e => { console.error(e.stack || e.message); process.exit(1); });
} else {
  usage();
  process.exit(args._[0] === 'help' ? 0 : 2);
}
