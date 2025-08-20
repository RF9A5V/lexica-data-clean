/**
 * Config file loader for multi-source pipeline
 */

import fs from 'fs/promises';
import path from 'path';
import { DATABASE_CONFIG, EXTRACTION_CONFIG, PATHS } from './config.js';

function normalizeSource(source, index) {
  let id = source.id || source.key || source.name || source.label || `source_${index + 1}`;
  id = String(id)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '') || `source_${index + 1}`;

  const label = source.label || source.name || id;
  const scrape_urls = Array.isArray(source.scrape_urls) ? source.scrape_urls.filter(Boolean) : [];
  const zip_urls = Array.isArray(source.zip_urls) ? source.zip_urls.filter(Boolean) : [];
  const urls_file = source.urls_file || null; // optional explicit path to save/load scraped URLs

  return { id, label, scrape_urls, zip_urls, urls_file };
}

export async function loadConfig(configPath) {
  const resolvedPath = path.resolve(configPath);
  const raw = await fs.readFile(resolvedPath, 'utf8');
  let cfg;
  try {
    cfg = JSON.parse(raw);
  } catch (e) {
    throw new Error(`Failed to parse config JSON at ${resolvedPath}: ${e.message}`);
  }

  const database = { ...DATABASE_CONFIG, ...(cfg.database || {}) };
  const options = {
    maxConcurrency: EXTRACTION_CONFIG.maxConcurrency,
    batchSize: EXTRACTION_CONFIG.batchSize,
    retryAttempts: EXTRACTION_CONFIG.retryAttempts,
    downloadDelayMs: EXTRACTION_CONFIG.downloadDelayMs,
    ...(cfg.options || {})
  };

  const sources = Array.isArray(cfg.sources) ? cfg.sources.map((s, i) => normalizeSource(s, i)) : [];
  if (sources.length === 0) {
    throw new Error('Config must include a non-empty sources[] array.');
  }

  const paths = { ...PATHS, ...(cfg.paths || {}) };
  const metadata = (cfg.metadata && typeof cfg.metadata === 'object' && !Array.isArray(cfg.metadata)) ? cfg.metadata : {};

  return { database, options, sources, paths, metadata };
}
