#!/usr/bin/env node
/**
 * Bound-Volume Bulk-Upload — push parsed cases.json files from
 * ./out/<volume>/cases.json into co-collection's bulk-ingest API.
 *
 * Usage:
 *   node upload.js list                          # list local volumes + their ingestion state
 *   node upload.js upload <vol> [<vol>...]       # upload specific volume(s)
 *   node upload.js upload-all                    # upload every out/<vol>/cases.json
 *
 * Auth (one of):
 *   --email=<addr> --password=<pw>     (or env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
 *   --token=<jwt>                       (or env CO_ADMIN_TOKEN — skips login)
 *
 * Flags:
 *   --target=<env>        Resolve base-url and per-volume source ref by env:
 *                         local | staging | prod. On staging/prod the upload
 *                         passes ?source=<ref> per volume (since the parser-
 *                         emitted target_source_db only resolves on local).
 *                         Overridden by --base-url= or env CO_COLLECTION_URL.
 *   --base-url=<url>      Collection base URL (default: env CO_COLLECTION_URL or http://localhost:3001)
 *   --source=<ref>        Force target_source_db override for every volume in
 *                         the run (escape hatch — only useful with --reporter
 *                         or a single explicit volume).
 *   --reporter=<name>     Filter by reporter suffix: AD3d | Misc3d | NY3d
 *   --confirm             Confirm (queue worker) when validation has no errors
 *   --overwrite           Pass overwrite=true on confirm (re-ingest into existing rows)
 *   --wait                Poll each confirmed ingestion to terminal state
 *   --skip-existing       Skip volumes whose source_pdf_sha256 is already ingested
 *                         (non-failed, non-cancelled)
 *   --dry-run             Resolve & report what would happen, upload nothing
 *   --limit=<N>           Only process first N volumes (after filtering)
 *   --start-from=<vol>    Skip volumes whose dir name sorts before this one
 *   --poll-interval=<ms>  Wait-mode poll cadence (default 3000)
 *   --poll-timeout=<sec>  Per-ingestion wait cap (default 600)
 *
 * Examples:
 *   # Smoke-test one volume against local
 *   node upload.js upload 157AD3d --email=admin@local --password=changeme --dry-run
 *
 *   # Upload everything to staging, auto-confirm and wait for the worker
 *   CO_COLLECTION_URL=https://co-collection-staging.fly.dev \
 *   CO_ADMIN_EMAIL=… CO_ADMIN_PASSWORD=… \
 *     node upload.js upload-all --confirm --wait --skip-existing
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
    // The login endpoint returns the JWT only via Set-Cookie — pull it out.
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

  async upload(casesPath, fileName, sourceOverride, { mergeTarget = false } = {}) {
    const buf = await readFile(casesPath);
    const fd = new FormData();
    fd.append('file', new Blob([buf], { type: 'application/json' }), fileName);
    const params = new URLSearchParams();
    if (sourceOverride) params.set('source', sourceOverride);
    // Track B / B3 §1: signal that the upload should be applied against the
    // merged ny_caselaw DB. Server persists the flag but currently REJECTS
    // confirmed merge_target jobs (the merged-aware matcher/inserter aren't
    // implemented yet) — surface that explicitly so callers know the upload
    // lands as pending_review but won't apply.
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

  cancel(id) {
    return this.request('POST', `/admin/api/bulk-ingest/${id}/cancel`);
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

// -------- existing-ingestion lookup (for --skip-existing) --------

const TERMINAL_BLOCKING_STATUSES = new Set([
  'pending_review',
  'confirmed',
  'running',
  'completed',
  'reverting', // mid-revert; once reverted the user can re-upload
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
  'completed', 'failed', 'cancelled', 'reverted',
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
  const confirm = bool(flags.confirm);
  const overwrite = bool(flags.overwrite);
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

  const summary = { uploaded: 0, skipped: 0, failed: 0, validationErrors: 0, confirmed: 0, completed: 0 };
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

    const uploadOverride = uploadOverrideFor(v.name, flags);
    const reportedSource = uploadOverride || sourceRefFor(v.name, flags) || meta.targetSourceDb;
    const mergeTarget = bool(flags['merge-target']);

    if (dryRun) {
      const mergeNote = mergeTarget ? ' [merge_target=true — server will REJECT confirms]' : '';
      console.log(`${tag} dry-run — would upload ${meta.cases} cases (${(v.size / 1024 / 1024).toFixed(1)} MiB), source=${reportedSource}${uploadOverride ? ' (override)' : ''}${mergeNote}`);
      continue;
    }

    let ingestion;
    try {
      ingestion = await client.upload(v.casesPath, `${v.name}.cases.json`, uploadOverride, { mergeTarget });
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
      `errs=${errCount} warns=${warnCount}`
    );

    if (errCount > 0) {
      summary.validationErrors++;
      console.warn(`${tag}   ⚠ has ${errCount} validation error(s) — not eligible for confirm`);
      // Surface first few so the operator knows why
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
    console.log(`${tag}   confirmed (worker will pick up; status=${confirmed.ingestion?.status || 'confirmed'})`);

    if (!wait) continue;

    try {
      const final = await waitForTerminal(client, ingestion.id, { intervalMs, timeoutMs });
      const dur = final.completed_at && final.started_at
        ? `${Math.round((new Date(final.completed_at) - new Date(final.started_at)) / 1000)}s`
        : '?';
      if (final.status === 'completed') {
        summary.completed++;
        console.log(`${tag}   ✓ completed in ${dur}`);
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
  console.log(`  confirmed:          ${summary.confirmed}`);
  console.log(`  completed:          ${summary.completed}`);
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

// Per-target source identifiers, keyed by reporter suffix. Source refs are
// the same on every environment (ny_supreme / ny_appellate / ny_trial); what
// differs is the physical DB name. The parser bakes the *dev* physical
// names (ny_reporter / ny_appellate_division / ny_trial_courts) into each
// cases.json's `target_source_db`, and on local the sources table maps
// those names back to source refs. Staging and prod don't carry that drift —
// the DB names match the source refs — so the parser-emitted names won't
// resolve there. uploadOverrideFor() detects that mismatch and tells the
// upload call to pass `?source=<ref>` instead.
const SOURCE_TARGETS = {
  local: {
    NY3d:   { source_ref: 'ny_supreme',   db_name: 'ny_reporter' },
    AD3d:   { source_ref: 'ny_appellate', db_name: 'ny_appellate_division' },
    Misc3d: { source_ref: 'ny_trial',     db_name: 'ny_trial_courts' },
  },
  staging: {
    NY3d:   { source_ref: 'ny_supreme',   db_name: 'ny_supreme' },
    AD3d:   { source_ref: 'ny_appellate', db_name: 'ny_appellate' },
    Misc3d: { source_ref: 'ny_trial',     db_name: 'ny_trial' },
  },
  prod: {
    NY3d:   { source_ref: 'ny_supreme',   db_name: 'ny_supreme' },
    AD3d:   { source_ref: 'ny_appellate', db_name: 'ny_appellate' },
    Misc3d: { source_ref: 'ny_trial',     db_name: 'ny_trial' },
  },
};

function targetEnv(flags) {
  return String(flags.target || 'local').toLowerCase();
}

function targetTable(flags) {
  return SOURCE_TARGETS[targetEnv(flags)] || SOURCE_TARGETS.local;
}

function sourceRefFor(volumeName, flags) {
  return targetTable(flags)[reporterOf(volumeName)]?.source_ref || null;
}

function uploadOverrideFor(volumeName, flags) {
  // --source=<ref> is a global escape hatch (applies to every volume).
  if (flags.source) return String(flags.source);
  const reporter = reporterOf(volumeName);
  const entry = targetTable(flags)[reporter];
  if (!entry) return null;
  // If the target's db_name matches the parser-emitted dev physical name,
  // the server can resolve it on its own — leave the override unset.
  // Otherwise pass source_ref so the lookup bypasses the database_url scan.
  const devDbName = SOURCE_TARGETS.local[reporter]?.db_name;
  return entry.db_name === devDbName ? null : entry.source_ref;
}

// --target=… → base URL. Hostnames match the Fly app names; bump if those
// ever change. Returns null for unknown targets so resolution falls
// through to the local default.
const TARGET_URLS = {
  local:   'http://localhost:3001',
  staging: 'https://curia-collection-staging.fly.dev',
  prod:    'https://co-collection.fly.dev',
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
  // --base-url and CO_COLLECTION_URL still win so existing CI scripts don't
  // change behavior.
  const baseUrl =
    flags['base-url']
    || process.env.CO_COLLECTION_URL
    || resolveTargetUrl(flags.target)
    || 'http://localhost:3001';

  // Auth is required when we'll talk to the API. `list` without creds, and
  // `--dry-run` without `--skip-existing`, both stay local.
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
  list                              list local output/<vol> dirs (with collection state if creds given)
  upload <vol> [<vol>...]           upload specific volume(s)
  upload-all                        upload every output/<vol>/cases.json

Common flags:
  --base-url=<url>                  collection base URL (env CO_COLLECTION_URL, default http://localhost:3001)
  --email=<addr> --password=<pw>    admin credentials (env CO_ADMIN_EMAIL / CO_ADMIN_PASSWORD)
  --token=<jwt>                     admin JWT (env CO_ADMIN_TOKEN; skips login)
  --source=<ref>                    force target_source_db override for every volume in the run
  --merge-target                    upload with merge_target=true (Track B / B3 §1). Server persists
                                    the flag but REJECTS confirmed jobs until the merged-aware
                                    matcher/inserter ships — use for ahead-of-server upload staging only.
  --reporter=<AD3d|Misc3d|NY3d>     filter to one reporter
  --start-from=<vol>                resume from a specific volume (sorted natural order)
  --limit=<N>                       process at most N volumes after filtering
  --skip-existing                   skip volumes whose source_pdf_sha256 is already ingested
  --confirm                         queue worker after each upload (only when no validation errors)
  --overwrite                       confirm with overwrite=true (re-ingest)
  --wait                            wait for each confirmed ingestion to reach a terminal status
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
