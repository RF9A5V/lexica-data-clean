#!/usr/bin/env node
/**
 * Slip-Op Bulk-Upload — push compiled batches from
 * ./compiled/<source>-<window>.json into co-backend's bulk-ingest API.
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
 * Auth:
 *   --email=<addr> --password=<pw>     (or env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
 *
 *   co-backend uses Phoenix session + CSRF (the old co-collection bearer-token
 *   flow is gone). `login()` POSTs /api/auth/login, stashes the
 *   _curia_obscura_backend_key cookie from Set-Cookie, and stashes the
 *   csrf_token from the response body. Subsequent admin calls send both
 *   `Cookie:` and `X-CSRF-Token:` headers.
 *
 * Flags:
 *   --target=<env>        local | staging | prod  (resolves base-url)
 *   --base-url=<url>      Backend base URL (overrides --target)
 *   --source=<ref>        Filter to one source (e.g. ny_appellate); also used as
 *                         the upload source override when set.
 *   --window=<label>      Filter to one window (e.g. 2026-05-05)
 *   --wait                Poll each ingestion to terminal state
 *   --skip-existing       Skip artifacts whose source_pdf_sha256 is already ingested
 *   --dry-run             Resolve & report what would happen, upload nothing
 *   --poll-interval=<ms>  Wait-mode poll cadence (default 3000)
 *   --poll-timeout=<sec>  Per-ingestion wait cap (default 600)
 *
 * Status enum (new server, auto-apply on validation pass):
 *   uploaded → applying → applied | needs_review | failed
 *
 * Examples:
 *   # Smoke-test a compile artifact against local
 *   node upload.js upload ny_appellate-2026-05-05 --target=local --email=admin@local --password=changeme --dry-run
 *
 *   # Push everything to staging and wait
 *   node upload.js upload-all --target=staging --wait \
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
  local:   'http://localhost:4000',
  staging: 'https://curia-backend-staging.fly.dev',
  prod:    'https://curia-obscura-backend.fly.dev',
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

// Slip-op compiled artifact stems are `<source_ref>-<window>` (e.g.
// `ny_appellate-2026-05-05`). The source ref is everything before the
// trailing `-YYYY-MM-DD` window suffix.
function sourceRefFromStem(stem) {
  const m = stem.match(/^(.+)-\d{4}-\d{2}-\d{2}$/);
  return m ? m[1] : null;
}

// --- HTTP client (mirrors bound-volume/upload.js's Client class) ---

const SESSION_COOKIE_NAME = '_curia_obscura_backend_key';

class Client {
  constructor({ baseUrl }) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.sessionCookie = null;
    this.csrfToken = null;
  }

  authHeaders() {
    const h = {};
    if (this.sessionCookie) h['Cookie'] = `${SESSION_COOKIE_NAME}=${this.sessionCookie}`;
    if (this.csrfToken) h['X-CSRF-Token'] = this.csrfToken;
    return h;
  }

  async login(email, password) {
    const url = `${this.baseUrl}/api/auth/login`;
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
    const m = setCookie.match(new RegExp(`${SESSION_COOKIE_NAME}=([^;,\\s]+)`));
    if (!m) {
      throw new Error(`Admin login succeeded but no ${SESSION_COOKIE_NAME} cookie returned`);
    }
    this.sessionCookie = m[1];
    // Phoenix's auth/login doesn't persist `_csrf_token` into the session
    // cookie it sends back (the `get_csrf_token()` call mutates a local conn
    // that doesn't make it into the response pipeline). So the login-response
    // token is bound to nothing the server can verify. Fix: GET /api/csrf_token
    // with the login cookie. That call generates a token AND mutates the
    // session, producing a Set-Cookie whose new session payload carries the
    // matching `_csrf_token`. Both the new cookie AND the new token must be
    // captured for subsequent calls to validate.
    const csrfRes = await fetch(`${this.baseUrl}/api/csrf_token`, {
      headers: { Cookie: `${SESSION_COOKIE_NAME}=${this.sessionCookie}` },
    });
    if (!csrfRes.ok) {
      throw new Error(`Failed to fetch post-login CSRF token (HTTP ${csrfRes.status})`);
    }
    const csrfSetCookie = csrfRes.headers.get('set-cookie') || '';
    const updated = csrfSetCookie.match(new RegExp(`${SESSION_COOKIE_NAME}=([^;,\\s]+)`));
    if (updated) this.sessionCookie = updated[1];
    const csrfPayload = await csrfRes.json().catch(() => null);
    if (!csrfPayload || !csrfPayload.csrf_token) {
      throw new Error('CSRF token endpoint returned no csrf_token');
    }
    this.csrfToken = csrfPayload.csrf_token;
  }

  async request(method, p, { json, query } = {}) {
    const qs = query ? '?' + new URLSearchParams(query).toString() : '';
    const url = `${this.baseUrl}${p}${qs}`;
    const headers = { ...this.authHeaders() };
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

  async upload(artifactPath, fileName, sourceOverride) {
    if (!sourceOverride) {
      throw new Error(`upload(${fileName}) requires a source ref — the server now strictly requires ?source=<ref>`);
    }
    const buf = await readFile(artifactPath);
    const fd = new FormData();
    fd.append('file', new Blob([buf], { type: 'application/json' }), fileName);
    const params = new URLSearchParams();
    params.set('source', sourceOverride);
    const qs = `?${params.toString()}`;
    const url = `${this.baseUrl}/admin/api/bulk-ingest/upload${qs}`;
    const res = await fetch(url, {
      method: 'POST',
      headers: this.authHeaders(),
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

// New status enum on co-backend:
//   uploaded → applying → applied | needs_review | failed
// All non-failed rows block re-upload of the same SHA.
const TERMINAL_BLOCKING_STATUSES = new Set([
  'uploaded',
  'applying',
  'applied',
  'needs_review',
]);

const TERMINAL_FINAL_STATUSES = new Set([
  'applied', 'failed', 'needs_review',
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
  // Index existing by lifting source refs from the artifact stems.
  const refs = new Set();
  for (const a of artifacts) {
    const ref = sourceRefFromStem(a.stem);
    if (ref) refs.add(ref);
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
  const wait = bool(flags.wait);
  const skipExisting = bool(flags['skip-existing']);
  const sourceFlag = flags.source ? String(flags.source) : null;
  const intervalMs = parseInt(flags['poll-interval'] || '3000', 10);
  const timeoutMs = parseInt(flags['poll-timeout'] || '600', 10) * 1000;

  let existingIndex = null;
  if (skipExisting && client) {
    const refs = new Set();
    for (const a of artifacts) {
      const ref = sourceRefFromStem(a.stem);
      if (ref) refs.add(ref);
    }
    existingIndex = await buildExistingIndex(client, [...refs]);
    console.log(`[skip-existing] indexed ${existingIndex.size} prior ingestions across ${refs.size} source(s)`);
  }

  const summary = { uploaded: 0, skipped: 0, failed: 0, validationErrors: 0, applied: 0, needsReview: 0 };
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

    const sourceRef = sourceFlag || sourceRefFromStem(a.stem);
    if (!sourceRef) {
      console.error(`${tag} cannot determine source ref from stem "${a.stem}" (expected <source>-<YYYY-MM-DD>); pass --source=<ref> to override`);
      summary.failed++;
      continue;
    }

    if (dryRun) {
      console.log(`${tag} dry-run — would upload ${meta.cases} cases (${(a.size / 1024 / 1024).toFixed(1)} MiB), source=${sourceRef}, schema=${meta.schemaVersion}`);
      continue;
    }

    let ingestion;
    try {
      ingestion = await client.upload(a.path, a.name, sourceRef);
    } catch (err) {
      console.error(`${tag} upload failed: ${err.message}`);
      const vErrs = err.payload?.ingestion?.validation_errors || err.payload?.validation_errors;
      if (Array.isArray(vErrs) && vErrs.length) {
        for (const e of vErrs.slice(0, 3)) {
          console.error(`${tag}     ${e.path || e.field || ''}: ${e.message}`);
        }
        summary.validationErrors++;
      } else {
        summary.failed++;
      }
      continue;
    }
    summary.uploaded++;
    // Server wraps errors/warnings as { items: [...] } to keep JSON shape flat-ish.
    const errCount = (ingestion.validation_errors?.items || ingestion.validation_errors || []).length;
    const warnCount = (ingestion.validation_warnings?.items || ingestion.validation_warnings || []).length;
    const counts = ingestion.metrics?.counts || {};
    console.log(
      `${tag} uploaded id=${ingestion.id} source=${ingestion.source_ref} status=${ingestion.status} ` +
      `cases=${counts.cases ?? '?'} ops=${counts.opinions ?? '?'} ` +
      `cites=${counts.citations ?? '?'} errs=${errCount} warns=${warnCount}`
    );

    if (errCount > 0) {
      summary.validationErrors++;
      console.warn(`${tag}   has ${errCount} validation error(s) — left at status=uploaded, NOT enqueued`);
      for (const e of (ingestion.validation_errors?.items || ingestion.validation_errors || []).slice(0, 3)) {
        console.warn(`${tag}     ${e.path || e.field || ''}: ${e.message}`);
      }
      continue;
    }

    if (!wait) continue;

    try {
      const final = await waitForTerminal(client, ingestion.id, { intervalMs, timeoutMs });
      const dur = final.completed_at && final.started_at
        ? `${Math.round((new Date(final.completed_at) - new Date(final.started_at)) / 1000)}s`
        : '?';
      if (final.status === 'applied') {
        summary.applied++;
        console.log(`${tag}   ✓ applied in ${dur}`);
      } else if (final.status === 'needs_review') {
        summary.needsReview++;
        const queued = final.result_json?.cases_queued ?? '?';
        console.log(`${tag}   ⌛ needs_review in ${dur} — ${queued} fuzzy candidate(s) await review at /admin/case-matches`);
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
  console.log(`  applied:                 ${summary.applied}`);
  console.log(`  needs_review:            ${summary.needsReview}`);
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
    || process.env.CO_BACKEND_URL
    || resolveTargetUrl(flags.target)
    || 'http://localhost:4000';

  let client = null;
  const haveAnyCred = !!(flags.email || process.env.CO_ADMIN_EMAIL);
  const isDryNoCheck = bool(flags['dry-run']) && !bool(flags['skip-existing']);
  const wantsAuth = !((cmd === 'list' && !haveAnyCred) || isDryNoCheck);
  if (wantsAuth) {
    client = new Client({ baseUrl });
    const email = flags.email || process.env.CO_ADMIN_EMAIL;
    const password = flags.password || process.env.CO_ADMIN_PASSWORD;
    if (!email || !password) {
      throw new Error('Missing credentials: pass --email=… --password=… (or set CO_ADMIN_EMAIL + CO_ADMIN_PASSWORD)');
    }
    await client.login(email, password);
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
  --base-url=<url>                  backend base URL (env CO_BACKEND_URL; overrides --target)
  --email=<addr> --password=<pw>    admin credentials (env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
  --source=<ref>                    filter to one source (also passed to upload as override)
  --window=<label>                  filter to one window (e.g. 2026-05-05)
  --skip-existing                   skip artifacts whose source_pdf_sha256 is already ingested
  --wait                            wait for each ingestion to reach a terminal status
                                    (applied | failed | needs_review)
  --poll-interval=<ms>              wait poll cadence (default 3000)
  --poll-timeout=<sec>              per-ingestion wait cap (default 600)
  --dry-run                         report only; upload nothing

Note: the server auto-applies when validation passes — no separate confirm
step. Status flow: uploaded → applying → applied | needs_review | failed.
`);
}

main().catch(err => {
  console.error('Error:', err.message);
  if (process.env.DEBUG) console.error(err.stack);
  process.exit(1);
});
