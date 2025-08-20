const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const cliProgress = require('cli-progress');
const pLimit = require('p-limit');

// Configuration constants
const DEFAULT_CONCURRENCY = 6;
const DEFAULT_WRITE_CONCURRENCY = 1;
const DEFAULT_POOL_SIZE = 10;
const MAX_CONCURRENCY = 50;
const MAX_WRITE_CONCURRENCY = 10;
const RETRY_ATTEMPTS = 3;
const RETRY_BASE_DELAY = 1000;

// Global cleanup state
let shutdownRequested = false;
const pendingOperations = new Set();

function parseCommonArgs(argv, additionalOptions = {}) {
  const args = {
    resume: true // Default to resume behavior
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--db') { args.db = argv[++i]; }
    else if (a === '--limit') { args.limit = parseInt(argv[++i], 10); }
    else if (a === '--case-ids') { args.caseIds = argv[++i].split(',').map(s => s.trim()); }
    else if (a === '--no-llm') { args.noLlm = true; }
    else if (a === '--samples-dir') { args.samplesDir = argv[++i]; }
    else if (a === '--dry-run') { args.dryRun = true; }
    else if (a === '--debug') { args.debug = true; }
    else if (a === '--concurrency') { args.concurrency = parseInt(argv[++i], 10); }
    else if (a === '--no-resume') { args.resume = false; }
    else if (a === '--write-concurrency') { args.writeConcurrency = parseInt(argv[++i], 10); }
    else if (a === '--pool-size') { args.poolSize = parseInt(argv[++i], 10); }
    else if (a === '--verbose-fields') { args.verboseFields = true; }
    else if (a === '--submit-only') { args.submitOnly = true; }
    else if (a === '--retrieve-only') { args.retrieveOnly = argv[++i]; }
    else if (a === '--list-batches') { args.listBatches = true; }
    else if (a === '--cancel-batch') { args.cancelBatch = argv[++i]; }
    else if (a === '--cleanup-batches') { args.cleanupBatches = true; }
    else if (a === '--cleanup-test-only') { args.cleanupTestOnly = true; }
    else if (a === '--cleanup-cancelled') { args.cleanupCancelled = true; }
    else if (a === '--cleanup-failed') { args.cleanupFailed = true; }
    else if (a === '--cleanup-older-than') { args.cleanupOlderThan = argv[++i]; }
    else if (a === '--help') { args.help = true; }
    else if (a === '--batch-id') { args.batchId = argv[++i]; }
    else if (a === '--scenario') { args.scenario = argv[++i]; }
    else if (additionalOptions[a]) {
      additionalOptions[a](args, argv, i);
      if (additionalOptions[a].incrementIndex) i++;
    }
  }
  
  // Validate and clamp arguments
  if (args.limit && (!Number.isFinite(args.limit) || args.limit < 1)) {
    throw new Error(`Invalid --limit: ${args.limit}. Must be a positive integer.`);
  }
  if (args.concurrency && (!Number.isFinite(args.concurrency) || args.concurrency < 1 || args.concurrency > MAX_CONCURRENCY)) {
    throw new Error(`Invalid --concurrency: ${args.concurrency}. Must be 1-${MAX_CONCURRENCY}.`);
  }
  if (args.writeConcurrency && (!Number.isFinite(args.writeConcurrency) || args.writeConcurrency < 1 || args.writeConcurrency > MAX_WRITE_CONCURRENCY)) {
    throw new Error(`Invalid --write-concurrency: ${args.writeConcurrency}. Must be 1-${MAX_WRITE_CONCURRENCY}.`);
  }
  if (args.poolSize && (!Number.isFinite(args.poolSize) || args.poolSize < 1 || args.poolSize > 100)) {
    throw new Error(`Invalid --pool-size: ${args.poolSize}. Must be 1-100.`);
  }
  
  return args;
}

function printCommonUsage(scriptName, specificOptions = '') {
  console.log(`Usage: ${scriptName} --db <DATABASE_URL|DB_NAME> [OPTIONS]
  
  Common Options:
    --limit N                 Process at most N opinions
    --case-ids id1,id2        Process specific opinion IDs
    --concurrency N           LLM concurrency (1-${MAX_CONCURRENCY}, default: ${DEFAULT_CONCURRENCY})
    --write-concurrency N     DB write concurrency (1-${MAX_WRITE_CONCURRENCY}, default: ${DEFAULT_WRITE_CONCURRENCY})
    --pool-size N             DB connection pool size (1-100, default: auto)
    --no-resume               Process all opinions (default: skip already processed)
    --no-llm                  Use sample payloads instead of LLM
    --samples-dir path        Directory for sample payloads
    --verbose-fields          Use verbose field names in LLM output (default: minimal fields for efficiency)
    --with-evidence           Include evidence character indices in LLM output (default: evidence-free for max efficiency)
    --dry-run                 No DB commits (rollback transactions)
    --debug                   Verbose logging
${specificOptions}`);
}

function makePgClient(db, poolSize) {
  const maxConnections = poolSize || DEFAULT_POOL_SIZE;
  if (db && db.includes('://')) return new Pool({ connectionString: db, max: maxConnections });
  // Fallback: treat as local database name (relies on env PGUSER/PGHOST)
  return new Pool({ database: db, max: maxConnections });
}

async function flagValuelessOpinion(client, opinionId, reason) {
  const res = await client.query(
    'UPDATE opinions SET is_valueless = true, valueless_reason = COALESCE(valueless_reason, $2) WHERE id = $1 AND is_valueless IS DISTINCT FROM true RETURNING id',
    [opinionId, reason || 'pass1 valueless']
  );
  if (res.rowCount > 0) {
    console.log(`[flagged] opinion ${opinionId} marked valueless (${reason || 'pass1 valueless'})`);
  }
}

function loadSamplePayload(samplesDir, name) {
  const dir = samplesDir || path.join(__dirname, '..', 'samples');
  const p = path.join(dir, `${name}.json`);
  const raw = fs.readFileSync(p, 'utf8');
  return JSON.parse(raw);
}

// Utility functions
function normalizeValuelessReason(reason) {
  if (reason === null || reason === undefined) return null;
  if (typeof reason === 'string') return reason;
  return String(reason);
}

async function retryDbOperation(operation, maxAttempts = RETRY_ATTEMPTS) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      const isRetryable = error.message.includes('deadlock') || 
                         error.message.includes('connection') ||
                         error.code === 'ECONNRESET' ||
                         error.code === 'ENOTFOUND';
      
      if (!isRetryable || attempt === maxAttempts) {
        throw error;
      }
      
      const delay = RETRY_BASE_DELAY * Math.pow(2, attempt - 1);
      console.warn(`[retry] Attempt ${attempt}/${maxAttempts} failed: ${error.message}. Retrying in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

function setupGracefulShutdown() {
  const shutdown = async (signal) => {
    if (shutdownRequested) return;
    shutdownRequested = true;
    console.log(`\n[shutdown] Received ${signal}. Waiting for ${pendingOperations.size} pending operations...`);
    
    // Wait for pending operations with timeout
    const timeout = setTimeout(() => {
      console.log('[shutdown] Timeout reached. Forcing exit.');
      process.exit(1);
    }, 30000);
    
    await Promise.allSettled(Array.from(pendingOperations));
    clearTimeout(timeout);
    console.log('[shutdown] Graceful shutdown complete.');
    process.exit(0);
  };
  
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

function renderProgress(prefix, done, total) {
  if (!total || total <= 0) return;
  const isTTY = process.stdout.isTTY;
  const pct = Math.max(0, Math.min(1, done / total));
  const percent = Math.round(pct * 100);
  if (!isTTY) {
    if (done === total) {
      console.log(`[${prefix}] ${done}/${total} (${percent}%)`);
    }
    return;
  }
  const cols = process.stdout.columns || 80;
  const barWidth = Math.max(10, Math.min(30, cols - (prefix.length + 20)));
  const filled = Math.round(barWidth * pct);
  const bar = `[${'#'.repeat(filled)}${'-'.repeat(barWidth - filled)}] ${percent}% ${done}/${total}`;
  process.stdout.write(`\r${prefix} ${bar}`);
  if (done >= total) process.stdout.write('\n');
}

async function getOpinionCandidates(client, args, whereClause = '', additionalParams = []) {
  let query = `
    SELECT o.id, o.text, c.jurisdiction_name, c.court_name, c.decision_date
    FROM opinions o
    INNER JOIN cases c ON o.case_id = c.id
    WHERE o.is_valueless = false
  `;
  
  let params = [];
  let paramIndex = 1;
  
  if (whereClause) {
    query += ` AND (${whereClause})`;
    params.push(...additionalParams);
    paramIndex += additionalParams.length;
  }
  
  if (args.caseIds && args.caseIds.length > 0) {
    query += ` AND o.id = ANY($${paramIndex}::int[])`;
    params.push(args.caseIds);
    paramIndex++;
  }
  
  // Order by citation_count DESC to prioritize highly-cited cases, then by opinion ID for consistency
  query += ' ORDER BY c.citation_count DESC, o.id';
  
  if (args.limit && args.limit > 0) {
    query += ` LIMIT $${paramIndex}`;
    params.push(args.limit);
  }
  
  return await client.query(query, params);
}

async function reportOpinionStats(client, passName) {
  const totalRow = await client.query('SELECT count(*)::int AS total FROM opinions');
  const valuelessRow = await client.query('SELECT count(*)::int AS valueless FROM opinions WHERE is_valueless = true');
  const total = totalRow.rows[0]?.total ?? 0;
  const valueless = valuelessRow.rows[0]?.valueless ?? 0;
  const eligible = total - valueless;
  console.log(`[pre] opinions total=${total} valueless=${valueless} eligible=${eligible}`);
  return { total, valueless, eligible };
}

module.exports = {
  // Constants
  DEFAULT_CONCURRENCY,
  DEFAULT_WRITE_CONCURRENCY,
  DEFAULT_POOL_SIZE,
  MAX_CONCURRENCY,
  MAX_WRITE_CONCURRENCY,
  RETRY_ATTEMPTS,
  RETRY_BASE_DELAY,
  
  // Global state
  pendingOperations,
  
  // Functions
  parseCommonArgs,
  printCommonUsage,
  makePgClient,
  flagValuelessOpinion,
  loadSamplePayload,
  normalizeValuelessReason,
  retryDbOperation,
  setupGracefulShutdown,
  renderProgress,
  getOpinionCandidates,
  reportOpinionStats,
  
  // External dependencies re-exported for convenience
  cliProgress,
  pLimit
};
