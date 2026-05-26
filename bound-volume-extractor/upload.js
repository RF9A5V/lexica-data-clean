#!/usr/bin/env node
/**
 * Bound-Volume Bulk-Upload — push parsed cases.json files from
 * ./out/<volume>/cases.json into co-backend's bulk-ingest API.
 *
 * Usage:
 *   node upload.js list                          # list local volumes + their ingestion state
 *   node upload.js upload <vol> [<vol>...]       # upload specific volume(s)
 *   node upload.js upload-all                    # upload every out/<vol>/cases.json
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
 *   --target=<env>        local | staging | prod (resolves base-url)
 *   --base-url=<url>      Backend base URL (default: env CO_BACKEND_URL or http://localhost:4000)
 *   --source=<ref>        Force source override for every volume in the run
 *                         (escape hatch — only useful with --reporter or a
 *                         single explicit volume). The server now REQUIRES
 *                         ?source=<ref> on every upload, so we always send it.
 *   --reporter=<name>     Filter by reporter suffix: AD3d | Misc3d | NY3d
 *   --wait                Poll each ingestion to terminal state
 *   --skip-existing       Skip volumes whose source_pdf_sha256 is already ingested
 *   --dry-run             Resolve & report what would happen, upload nothing
 *   --limit=<N>           Only process first N volumes (after filtering)
 *   --start-from=<vol>    Skip volumes whose dir name sorts before this one
 *   --poll-interval=<ms>  Wait-mode poll cadence (default 3000)
 *   --poll-timeout=<sec>  Per-ingestion wait cap (default 600)
 *
 * Status enum (new server, auto-apply on validation pass):
 *   uploaded → applying → applied | needs_review | failed
 *
 * Examples:
 *   # Smoke-test one volume against local
 *   node upload.js upload 157AD3d --email=admin@local --password=changeme --dry-run
 *
 *   # Upload everything to staging, wait for the worker
 *   CO_BACKEND_URL=https://curia-backend-staging.fly.dev \
 *   CO_ADMIN_EMAIL=… CO_ADMIN_PASSWORD=… \
 *     node upload.js upload-all --wait --skip-existing
 */

import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Blob } from 'node:buffer';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = path.join(__dirname, 'out');

// -------- args --------

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

// -------- volume discovery --------

async function listLocalVolumes() {
  const entries = await readdir(OUTPUT_DIR, { withFileTypes: true });
  const dirs = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const cases = path.join(OUTPUT_DIR, e.name, 'cases.json');
    try {
      const st = await stat(cases);
      if (st.isFile()) dirs.push({ name: e.name, casesPath: cases, size: st.size });
    } catch {
      // no cases.json — skip
    }
  }
  // Volume-natural sort: leading number then suffix.
  dirs.sort((a, b) => {
    const na = parseInt(a.name, 10) || 0;
    const nb = parseInt(b.name, 10) || 0;
    if (na !== nb) return na - nb;
    return a.name.localeCompare(b.name);
  });
  return dirs;
}

function reporterOf(volumeName) {
  const m = volumeName.match(/^\d+(.+)$/);
  return m ? m[1] : null;
}

// -------- HTTP --------

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
    // Phoenix session lives in the Set-Cookie header; CSRF token lives in body.
    // Set-Cookie may concatenate multiple cookies — pull the session one by name.
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

  async upload(casesPath, fileName, sourceOverride) {
    if (!sourceOverride) {
      throw new Error(`upload(${fileName}) requires a source ref — the server now strictly requires ?source=<ref>`);
    }
    const buf = await readFile(casesPath);
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

  async get(id) {
    // Server wraps as { ingestion: {...} }. Unwrap for caller convenience —
    // mirrors `client.upload()` which already returns `payload.ingestion`.
    const payload = await this.request('GET', `/admin/api/bulk-ingest/${id}`);
    return payload?.ingestion ?? payload;
  }

  list({ source, status, limit = 200 } = {}) {
    const query = { limit: String(limit) };
    if (source) query.source = source;
    if (status) query.status = status;
    return this.request('GET', '/admin/api/bulk-ingest', { query });
  }
}

// -------- existing-ingestion lookup (for --skip-existing) --------

// New status enum on co-backend:
//   uploaded → applying → applied | needs_review | failed
// All non-failed rows block re-upload of the same SHA.
const TERMINAL_BLOCKING_STATUSES = new Set([
  'uploaded',
  'applying',
  'applied',
  'needs_review',
]);

async function buildExistingIndex(client, sourceRefs) {
  // We index by source_pdf_sha256 across the union of recent ingestions for
  // each distinct source ref we'll touch. The list endpoint caps at 200 per
  // call, which is plenty for a single source under our current cadence.
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
      // Keep newest (list returns DESC by created_at — first wins).
      if (!index.has(row.source_pdf_sha256)) {
        index.set(row.source_pdf_sha256, row);
      }
    }
  }
  return index;
}

// -------- waiter --------

const TERMINAL_FINAL_STATUSES = new Set([
  'applied', 'failed', 'needs_review',
]);

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

// -------- main commands --------

async function cmdList(client, flags) {
  const volumes = await selectVolumes(flags);
  if (!client) {
    for (const v of volumes) {
      console.log(`${v.name.padEnd(12)}  ${(v.size / 1024).toFixed(0)} KiB`);
    }
    return;
  }
  // Need to know what's already ingested. Lift target sources from the volume names.
  const refs = collectSourceRefs(volumes, flags);
  const idx = await buildExistingIndex(client, refs);
  console.log(`local volumes: ${volumes.length}    existing ingestions indexed: ${idx.size}`);
  console.log('volume       size   status         ingestion');
  for (const v of volumes) {
    const meta = await readMeta(v.casesPath);
    const hit = meta.sha256 ? idx.get(meta.sha256) : null;
    const status = hit ? hit.status : '—';
    const idCol = hit ? `id=${hit.id}` : '';
    console.log(
      `${v.name.padEnd(12)} ${String(Math.round(v.size / 1024)).padStart(6)} KiB ` +
      `${status.padEnd(14)} ${idCol}`
    );
  }
}

async function cmdUpload(client, flags, requestedVolumes) {
  const all = await listLocalVolumes();
  const byName = new Map(all.map(v => [v.name, v]));

  let volumes;
  if (requestedVolumes.length > 0) {
    const missing = requestedVolumes.filter(n => !byName.has(n));
    if (missing.length) {
      throw new Error(`Unknown volume(s): ${missing.join(', ')}`);
    }
    volumes = requestedVolumes.map(n => byName.get(n));
  } else {
    volumes = all;
  }

  volumes = filterAndSlice(volumes, flags);

  if (volumes.length === 0) {
    console.log('No volumes to upload (after filters).');
    return;
  }

  const dryRun = bool(flags['dry-run']);
  const wait = bool(flags.wait);
  const skipExisting = bool(flags['skip-existing']);
  const intervalMs = parseInt(flags['poll-interval'] || '3000', 10);
  const timeoutMs = parseInt(flags['poll-timeout'] || '600', 10) * 1000;

  let existingIndex = null;
  if (skipExisting) {
    const refs = collectSourceRefs(volumes, flags);
    existingIndex = await buildExistingIndex(client, refs);
    console.log(`[skip-existing] indexed ${existingIndex.size} prior ingestions across ${refs.length} source(s)`);
  }

  const summary = { uploaded: 0, skipped: 0, failed: 0, validationErrors: 0, applied: 0, needsReview: 0 };
  let i = 0;
  for (const v of volumes) {
    i++;
    const tag = `[${i}/${volumes.length}] ${v.name}`;
    let meta;
    try {
      meta = await readMeta(v.casesPath);
    } catch (err) {
      console.error(`${tag} failed to read cases.json: ${err.message}`);
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

    const sourceRef = sourceRefFor(v.name, flags);
    if (!sourceRef) {
      console.error(`${tag} cannot resolve source ref (unknown reporter "${reporterOf(v.name)}"; pass --source=<ref> to override)`);
      summary.failed++;
      continue;
    }

    if (dryRun) {
      console.log(`${tag} dry-run — would upload ${meta.cases} cases (${(v.size / 1024 / 1024).toFixed(1)} MiB), source=${sourceRef}`);
      continue;
    }

    let ingestion;
    try {
      ingestion = await client.upload(v.casesPath, `${v.name}.cases.json`, sourceRef);
    } catch (err) {
      console.error(`${tag} upload failed: ${err.message}`);
      // Surface validation errors when the server returned them on a 422.
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
      `errs=${errCount} warns=${warnCount}`
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
        console.log(`${tag}   ⌛ needs_review in ${dur} — fuzzy match candidates await operator review`);
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
  console.log(`  uploaded:           ${summary.uploaded}`);
  console.log(`  skipped (existing): ${summary.skipped}`);
  console.log(`  with validation errors: ${summary.validationErrors}`);
  console.log(`  applied:            ${summary.applied}`);
  console.log(`  needs_review:       ${summary.needsReview}`);
  console.log(`  failed:             ${summary.failed}`);
}

async function selectVolumes(flags) {
  const all = await listLocalVolumes();
  return filterAndSlice(all, flags);
}

function filterAndSlice(volumes, flags) {
  let out = volumes.slice();
  if (flags.reporter) {
    const want = String(flags.reporter);
    out = out.filter(v => reporterOf(v.name) === want);
  }
  if (flags['start-from']) {
    const from = String(flags['start-from']);
    const idx = out.findIndex(v => v.name === from);
    if (idx >= 0) out = out.slice(idx);
  }
  if (flags.limit) {
    const n = parseInt(flags.limit, 10);
    if (Number.isFinite(n) && n > 0) out = out.slice(0, n);
  }
  return out;
}

async function readMeta(casesPath) {
  // We only need the first ~few KB of the JSON to read header fields, but the
  // schema doesn't guarantee header-first ordering. Easiest correct path is a
  // full parse (cases.json files are 0.2–6 MB; trivial for Node).
  const raw = await readFile(casesPath, 'utf8');
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

function collectSourceRefs(volumes, flags) {
  if (flags.source) return [String(flags.source)];
  const set = new Set();
  for (const v of volumes) {
    const ref = sourceRefFor(v.name, flags);
    if (ref) set.add(ref);
  }
  return [...set];
}

// Reporter suffix → source ref. The merged co-backend uses a single canonical
// source ref per source across every environment (local / staging / prod) —
// the per-env physical-DB drift that the old co-collection world had is gone.
const SOURCE_REFS = {
  NY3d:   'ny_supreme',
  AD3d:   'ny_appellate',
  Misc3d: 'ny_trial',
};

function sourceRefFor(volumeName, flags) {
  // --source=<ref> is a global escape hatch (applies to every volume).
  if (flags.source) return String(flags.source);
  const reporter = reporterOf(volumeName);
  return reporter ? (SOURCE_REFS[reporter] || null) : null;
}

// --target=… → base URL. Hostnames match the co-backend Fly app names;
// bump if those ever change. Returns null for unknown targets so resolution
// falls through to the local default.
const TARGET_URLS = {
  local:   'http://localhost:4000',
  staging: 'https://curia-backend-staging.fly.dev',
  prod:    'https://curia-obscura-backend.fly.dev',
};
function resolveTargetUrl(target) {
  if (!target) return null;
  return TARGET_URLS[String(target).toLowerCase()] || null;
}

// -------- entry --------

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const cmd = positional[0];
  const rest = positional.slice(1);

  if (!cmd || flags.help || cmd === 'help') {
    printUsage();
    return;
  }

  // --target=local|staging|prod is a convenience over --base-url. Explicit
  // --base-url and CO_BACKEND_URL still win so existing CI scripts don't
  // change behavior.
  const baseUrl =
    flags['base-url']
    || process.env.CO_BACKEND_URL
    || resolveTargetUrl(flags.target)
    || 'http://localhost:4000';

  // Auth is required when we'll talk to the API. `list` without creds, and
  // `--dry-run` without `--skip-existing`, both stay local.
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
      if (rest.length === 0) throw new Error('upload requires at least one <volume> argument (e.g. 157AD3d). Use upload-all for everything.');
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
  console.log(`Bound-Volume Bulk-Upload

Commands:
  list                              list local out/<vol> dirs (with backend state if creds given)
  upload <vol> [<vol>...]           upload specific volume(s)
  upload-all                        upload every out/<vol>/cases.json

Common flags:
  --target=<env>                    local | staging | prod (resolves base-url)
  --base-url=<url>                  backend base URL (env CO_BACKEND_URL, default http://localhost:4000)
  --email=<addr> --password=<pw>    admin credentials (env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
  --source=<ref>                    force source override for every volume in the run
                                    (escape hatch; reporter suffix mapping is otherwise automatic)
  --reporter=<AD3d|Misc3d|NY3d>     filter to one reporter
  --start-from=<vol>                resume from a specific volume (sorted natural order)
  --limit=<N>                       process at most N volumes after filtering
  --skip-existing                   skip volumes whose source_pdf_sha256 is already ingested
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
