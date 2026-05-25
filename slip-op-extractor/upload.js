#!/usr/bin/env node
/**
 * Slip-Op Bulk-Upload — push compiled batches from
 * ./compiled/<source>-<window>.json into co-collection's bulk-ingest API.
 *
 * Each compiled artifact is a single multi-case payload (built by
 * `node main.js compile`). One file → one HTTP POST → many cases ingested
 * in a single transaction-per-case loop on the server.
 *
 * Usage:
 *   node upload.js list                              # list compiled artifacts + ingestion state
 *   node upload.js upload <file|stem> [<file>...]    # upload specific compiled artifact(s)
 *   node upload.js upload-all                        # upload every compiled/<source>-<window>.json
 *
 * Identification: pass either the full filename (ny_appellate-2026-05-05.json)
 * or the stem without extension (ny_appellate-2026-05-05).
 *
 * Auth (one of):
 *   --email=<addr> --password=<pw>     (or env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
 *   --token=<jwt>                       (or env CO_ADMIN_TOKEN — skips login)
 *
 * Flags:
 *   --target=<env>        local | staging | prod  (resolves base-url)
 *   --base-url=<url>      Collection base URL (overrides --target)
 *   --source=<ref>        Filter to one source (e.g. ny_appellate)
 *   --window=<label>      Filter to one window (e.g. 2026-05-05)
 *   --confirm             Confirm (queue worker) when validation has no errors
 *   --overwrite           Pass overwrite=true on confirm
 *   --wait                Poll each confirmed ingestion to terminal state
 *   --skip-existing       Skip artifacts whose source_pdf_sha256 is already ingested
 *   --dry-run             Resolve & report what would happen, upload nothing
 *   --poll-interval=<ms>  Wait-mode poll cadence (default 3000)
 *   --poll-timeout=<sec>  Per-ingestion wait cap (default 600)
 *
 * Examples:
 *   # Smoke-test a compile artifact against local
 *   node upload.js upload ny_appellate-2026-05-05 --target=local --email=admin@local --password=changeme --dry-run
 *
 *   # Push everything to staging, auto-confirm and wait
 *   node upload.js upload-all --target=staging --confirm --wait \
 *     --email=… --password=…
 */

import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Blob } from 'node:buffer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const COMPILED_DIR = path.join(__dirname, 'compiled');

// --- arg parsing ---

function parseArgs(argv) {
  const positional = [];
  const flags = {};
  for (const arg of argv) {
    if (arg.startsWith('--')) {
      const eq = arg.indexOf('=');
      if (eq === -1) flags[arg.slice(2)] = true;
      else flags[arg.slice(2, eq)] = arg.slice(eq + 1);
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function bool(v) { return v === true || v === 'true' || v === '1'; }

// --- target resolution (mirror of bound-volume/upload.js) ---

const TARGET_URLS = {
  local:   'http://localhost:3001',
  staging: 'https://co-collection-staging.fly.dev',
  prod:    'https://co-collection.fly.dev',
};
function resolveTargetUrl(target) {
  if (!target) return null;
  return TARGET_URLS[String(target).toLowerCase()] || null;
}

// --- artifact discovery ---

async function listCompiled() {
  let entries;
  try {
    entries = await readdir(COMPILED_DIR, { withFileTypes: true });
  } catch (err) {
    if (err.code === 'ENOENT') return [];
    throw err;
  }
  const out = [];
  for (const e of entries) {
    if (!e.isFile()) continue;
    if (!e.name.endsWith('.json')) continue;
    if (e.name.endsWith('.manifest.json')) continue;  // skip manifests
    const fullPath = path.join(COMPILED_DIR, e.name);
    const st = await stat(fullPath);
    out.push({
      name: e.name,
      stem: e.name.replace(/\.json$/, ''),
      path: fullPath,
      size: st.size,
    });
  }
  out.sort((a, b) => a.name.localeCompare(b.name));
  return out;
}

function selectArtifacts(all, requested, flags) {
  let out = requested.length > 0
    ? all.filter(a => requested.includes(a.name) || requested.includes(a.stem))
    : all.slice();

  if (flags.source) {
    out = out.filter(a => a.stem.startsWith(`${flags.source}-`));
  }
  if (flags.window) {
    out = out.filter(a => a.stem.endsWith(`-${flags.window}`));
  }
  return out;
}

async function readMeta(artifactPath) {
  const raw = await readFile(artifactPath, 'utf8');
  const obj = JSON.parse(raw);
  return {
    sha256: obj.source_pdf_sha256 || null,
    batchId: obj.batch_id || null,
    targetSourceDb: obj.target_source_db || null,
    schemaVersion: obj.schema_version || null,
    parserVersion: obj.parser_version || null,
    cases: Array.isArray(obj.cases) ? obj.cases.length : 0,
  };
}

// --- HTTP client (mirrors bound-volume/upload.js's Client class) ---

class Client {
  constructor({ baseUrl, token }) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.token = token || null;
  }

  authHeader() {
    return this.token ? { Authorization: `Bearer ${this.token}` } : {};
  }

  async login(email, password) {
    const url = `${this.baseUrl}/admin/api/login`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`Admin login failed (HTTP ${res.status}): ${text}`);
    }
    const setCookie = res.headers.get('set-cookie') || '';
    const m = setCookie.match(/admin_token=([^;]+)/);
    if (!m) throw new Error('Admin login succeeded but no admin_token cookie returned');
    this.token = m[1];
  }

  async request(method, p, { json, query } = {}) {
    const qs = query ? '?' + new URLSearchParams(query).toString() : '';
    const url = `${this.baseUrl}${p}${qs}`;
    const headers = { ...this.authHeader() };
    let body;
    if (json !== undefined) {
      headers['content-type'] = 'application/json';
      body = JSON.stringify(json);
    }
    const res = await fetch(url, { method, headers, body });
    const ct = res.headers.get('content-type') || '';
    const payload = ct.includes('application/json')
      ? await res.json().catch(() => null)
      : await res.text();
    if (!res.ok) {
      const detail = typeof payload === 'object' && payload
        ? (payload.error || payload.message || JSON.stringify(payload))
        : payload;
      const err = new Error(`${method} ${p} → HTTP ${res.status}: ${detail}`);
      err.status = res.status;
      err.payload = payload;
      throw err;
    }
    return payload;
  }

  async upload(artifactPath, fileName, sourceOverride, { mergeTarget = false } = {}) {
    const buf = await readFile(artifactPath);
    const fd = new FormData();
    fd.append('file', new Blob([buf], { type: 'application/json' }), fileName);
    const params = new URLSearchParams();
    if (sourceOverride) params.set('source', sourceOverride);
    // Track B / B3 §1: see bound-volume-extractor/upload.js for the full
    // explanation. Server persists the flag but REJECTS confirmed merge_target
    // jobs until the merged-aware matcher/inserter ships.
    if (mergeTarget) params.set('merge_target', 'true');
    const qs = params.toString() ? `?${params.toString()}` : '';
    const url = `${this.baseUrl}/admin/api/bulk-ingest/upload${qs}`;
    const res = await fetch(url, {
      method: 'POST',
      headers: this.authHeader(),
      body: fd,
    });
    const ct = res.headers.get('content-type') || '';
    const payload = ct.includes('application/json')
      ? await res.json().catch(() => null)
      : await res.text();
    if (!res.ok) {
      const detail = typeof payload === 'object' && payload
        ? (payload.error || payload.message || JSON.stringify(payload))
        : payload;
      const err = new Error(`upload ${fileName} → HTTP ${res.status}: ${detail}`);
      err.status = res.status;
      err.payload = payload;
      throw err;
    }
    return payload.ingestion;
  }

  confirm(id, { overwrite = false } = {}) {
    return this.request('POST', `/admin/api/bulk-ingest/${id}/confirm`, {
      json: { overwrite },
    });
  }

  get(id) {
    return this.request('GET', `/admin/api/bulk-ingest/${id}`);
  }

  list({ source, status, limit = 200 } = {}) {
    const query = { limit: String(limit) };
    if (source) query.source = source;
    if (status) query.status = status;
    return this.request('GET', '/admin/api/bulk-ingest', { query });
  }
}

const TERMINAL_BLOCKING_STATUSES = new Set([
  'pending_review', 'confirmed', 'running', 'completed',
  'pending_match_review',  // Bucket A: counts as in-flight for skip-existing
  'reverting',
]);

const TERMINAL_FINAL_STATUSES = new Set([
  'completed', 'failed', 'cancelled', 'reverted', 'pending_match_review',
]);

async function buildExistingIndex(client, sourceRefs) {
  const index = new Map(); // sha256 -> ingestion row
  for (const ref of sourceRefs) {
    let res;
    try {
      res = await client.list({ source: ref, limit: 200 });
    } catch (err) {
      console.warn(`[skip-existing] could not list ${ref}: ${err.message}`);
      continue;
    }
    for (const row of res.ingestions || []) {
      if (!row.source_pdf_sha256) continue;
      if (!TERMINAL_BLOCKING_STATUSES.has(row.status)) continue;
      if (!index.has(row.source_pdf_sha256)) {
        index.set(row.source_pdf_sha256, row);
      }
    }
  }
  return index;
}

async function waitForTerminal(client, id, { intervalMs, timeoutMs }) {
  const started = Date.now();
  while (true) {
    const row = await client.get(id);
    if (TERMINAL_FINAL_STATUSES.has(row.status)) return row;
    if (Date.now() - started > timeoutMs) {
      const err = new Error(`Wait timed out after ${Math.round(timeoutMs / 1000)}s (status=${row.status})`);
      err.lastRow = row;
      throw err;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
}

// --- commands ---

async function cmdList(client, flags) {
  const all = await listCompiled();
  const artifacts = selectArtifacts(all, [], flags);
  if (!client) {
    for (const a of artifacts) {
      console.log(`${a.name.padEnd(40)}  ${(a.size / 1024).toFixed(0)} KiB`);
    }
    return;
  }
  // Index existing by gathering source refs from artifact metas.
  const refs = new Set();
  for (const a of artifacts) {
    try {
      const meta = await readMeta(a.path);
      if (meta.targetSourceDb) refs.add(meta.targetSourceDb);
    } catch { /* ignore */ }
  }
  const idx = await buildExistingIndex(client, [...refs]);
  console.log(`compiled artifacts: ${artifacts.length}    existing ingestions indexed: ${idx.size}`);
  console.log('artifact                                         size   status                 ingestion');
  for (const a of artifacts) {
    let meta;
    try { meta = await readMeta(a.path); } catch { meta = {}; }
    const hit = meta.sha256 ? idx.get(meta.sha256) : null;
    const status = hit ? hit.status : '—';
    const idCol = hit ? `id=${hit.id}` : '';
    console.log(
      `${a.name.padEnd(45)}  ${String(Math.round(a.size / 1024)).padStart(6)} KiB  ` +
      `${status.padEnd(20)}  ${idCol}`
    );
  }
}

async function cmdUpload(client, flags, requested) {
  const all = await listCompiled();
  const artifacts = selectArtifacts(all, requested, flags);

  if (artifacts.length === 0) {
    console.log('No compiled artifacts to upload (after filters).');
    return;
  }

  const dryRun = bool(flags['dry-run']);
  const confirm = bool(flags.confirm);
  const overwrite = bool(flags.overwrite);
  const wait = bool(flags.wait);
  const skipExisting = bool(flags['skip-existing']);
  const sourceOverride = flags.source || null;
  const intervalMs = parseInt(flags['poll-interval'] || '3000', 10);
  const timeoutMs = parseInt(flags['poll-timeout'] || '600', 10) * 1000;

  let existingIndex = null;
  if (skipExisting && client) {
    const refs = new Set();
    for (const a of artifacts) {
      try {
        const meta = await readMeta(a.path);
        if (meta.targetSourceDb) refs.add(meta.targetSourceDb);
      } catch { /* ignore */ }
    }
    existingIndex = await buildExistingIndex(client, [...refs]);
    console.log(`[skip-existing] indexed ${existingIndex.size} prior ingestions across ${refs.size} source(s)`);
  }

  const summary = { uploaded: 0, skipped: 0, failed: 0, validationErrors: 0, confirmed: 0, completed: 0, queued_for_review: 0 };
  let i = 0;
  for (const a of artifacts) {
    i++;
    const tag = `[${i}/${artifacts.length}] ${a.stem}`;
    let meta;
    try {
      meta = await readMeta(a.path);
    } catch (err) {
      console.error(`${tag} failed to read artifact: ${err.message}`);
      summary.failed++;
      continue;
    }

    if (existingIndex && meta.sha256) {
      const hit = existingIndex.get(meta.sha256);
      if (hit) {
        console.log(`${tag} skip — already ingested (id=${hit.id}, status=${hit.status})`);
        summary.skipped++;
        continue;
      }
    }

    const mergeTarget = bool(flags['merge-target']);

    if (dryRun) {
      const mergeNote = mergeTarget ? ' [merge_target=true — server will REJECT confirms]' : '';
      console.log(`${tag} dry-run — would upload ${meta.cases} cases (${(a.size / 1024 / 1024).toFixed(1)} MiB), source=${sourceOverride || meta.targetSourceDb}, schema=${meta.schemaVersion}${mergeNote}`);
      continue;
    }

    let ingestion;
    try {
      ingestion = await client.upload(a.path, a.name, sourceOverride, { mergeTarget });
    } catch (err) {
      console.error(`${tag} upload failed: ${err.message}`);
      summary.failed++;
      continue;
    }
    summary.uploaded++;
    const errCount = (ingestion.validation_errors || []).length;
    const warnCount = (ingestion.validation_warnings || []).length;
    const counts = ingestion.metrics?.counts || {};
    console.log(
      `${tag} uploaded id=${ingestion.id} source=${ingestion.source_ref} ` +
      `cases=${counts.cases ?? '?'} ops=${counts.opinions ?? '?'} ` +
      `cites=${counts.citations ?? '?'} errs=${errCount} warns=${warnCount}`
    );

    if (errCount > 0) {
      summary.validationErrors++;
      console.warn(`${tag}   ⚠ has ${errCount} validation error(s) — not eligible for confirm`);
      for (const e of (ingestion.validation_errors || []).slice(0, 3)) {
        console.warn(`${tag}     ${e.path}: ${e.message}`);
      }
      continue;
    }

    if (!confirm) continue;

    let confirmed;
    try {
      confirmed = await client.confirm(ingestion.id, { overwrite });
    } catch (err) {
      console.error(`${tag} confirm failed: ${err.message}`);
      summary.failed++;
      continue;
    }
    summary.confirmed++;
    console.log(`${tag}   confirmed (status=${confirmed.ingestion?.status || 'confirmed'})`);

    if (!wait) continue;

    try {
      const final = await waitForTerminal(client, ingestion.id, { intervalMs, timeoutMs });
      const dur = final.completed_at && final.started_at
        ? `${Math.round((new Date(final.completed_at) - new Date(final.started_at)) / 1000)}s`
        : '?';
      if (final.status === 'completed') {
        summary.completed++;
        console.log(`${tag}   ✓ completed in ${dur}`);
      } else if (final.status === 'pending_match_review') {
        summary.queued_for_review++;
        const queued = final.result_json?.cases_queued ?? '?';
        console.log(`${tag}   ⌛ pending_match_review in ${dur} — ${queued} fuzzy candidate(s) await review at /admin/case-matches`);
      } else {
        summary.failed++;
        console.error(`${tag}   ✗ ended status=${final.status}: ${final.error_message || '(no error message)'}`);
      }
    } catch (err) {
      summary.failed++;
      console.error(`${tag}   wait error: ${err.message}`);
    }
  }

  console.log('');
  console.log('Summary:');
  console.log(`  uploaded:                ${summary.uploaded}`);
  console.log(`  skipped (existing):      ${summary.skipped}`);
  console.log(`  with validation errors:  ${summary.validationErrors}`);
  console.log(`  confirmed:               ${summary.confirmed}`);
  console.log(`  completed:               ${summary.completed}`);
  console.log(`  pending_match_review:    ${summary.queued_for_review}`);
  console.log(`  failed:                  ${summary.failed}`);
}

// --- entry ---

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const cmd = positional[0];
  const rest = positional.slice(1);

  if (!cmd || flags.help || cmd === 'help') {
    printUsage();
    return;
  }

  const baseUrl =
    flags['base-url']
    || process.env.CO_COLLECTION_URL
    || resolveTargetUrl(flags.target)
    || 'http://localhost:3001';

  let client = null;
  const haveAnyCred = !!(flags.email || flags.token || process.env.CO_ADMIN_TOKEN || process.env.CO_ADMIN_EMAIL);
  const isDryNoCheck = bool(flags['dry-run']) && !bool(flags['skip-existing']);
  const wantsAuth = !((cmd === 'list' && !haveAnyCred) || isDryNoCheck);
  if (wantsAuth) {
    const token = flags.token || process.env.CO_ADMIN_TOKEN;
    client = new Client({ baseUrl, token });
    if (!token) {
      const email = flags.email || process.env.CO_ADMIN_EMAIL;
      const password = flags.password || process.env.CO_ADMIN_PASSWORD;
      if (!email || !password) {
        throw new Error('Missing credentials: pass --token=… or --email=… --password=… (or set CO_ADMIN_TOKEN, or CO_ADMIN_EMAIL+CO_ADMIN_PASSWORD)');
      }
      await client.login(email, password);
    }
  }

  switch (cmd) {
    case 'list':
      await cmdList(client, flags);
      return;
    case 'upload':
      if (rest.length === 0) throw new Error('upload requires at least one <artifact> argument. Use upload-all for everything.');
      await cmdUpload(client, flags, rest);
      return;
    case 'upload-all':
      await cmdUpload(client, flags, []);
      return;
    default:
      console.error(`Unknown command: ${cmd}`);
      printUsage();
      process.exit(2);
  }
}

function printUsage() {
  console.log(`Slip-Op Bulk-Upload

Commands:
  list                              list compiled/<source>-<window>.json artifacts
  upload <artifact> [<artifact>...] upload specific artifact(s) by name or stem
  upload-all                        upload every artifact

Common flags:
  --target=<env>                    local | staging | prod (resolves base-url)
  --base-url=<url>                  collection base URL (env CO_COLLECTION_URL; overrides --target)
  --email=<addr> --password=<pw>    admin credentials (env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
  --token=<jwt>                     admin JWT (env CO_ADMIN_TOKEN; skips login)
  --source=<ref>                    filter to one source (also passed to upload as override)
  --merge-target                    upload with merge_target=true (Track B / B3 §1). Server persists
                                    the flag but REJECTS confirmed jobs until the merged-aware
                                    matcher/inserter ships — use for ahead-of-server upload staging only.
  --window=<label>                  filter to one window (e.g. 2026-05-05)
  --skip-existing                   skip artifacts whose source_pdf_sha256 is already ingested
  --confirm                         queue worker after each upload
  --overwrite                       confirm with overwrite=true
  --wait                            wait for each confirmed ingestion to reach terminal status
  --poll-interval=<ms>              wait poll cadence (default 3000)
  --poll-timeout=<sec>              per-ingestion wait cap (default 600)
  --dry-run                         report only; upload nothing
`);
}

main().catch(err => {
  console.error('Error:', err.message);
  if (process.env.DEBUG) console.error(err.stack);
  process.exit(1);
});
