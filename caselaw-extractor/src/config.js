/**
 * Configuration for case.law data extraction
 */

import dotenv from 'dotenv';

// Load environment variables
dotenv.config();

export const JURISDICTIONS = {
  ny: {
    name: 'New York',
    url: 'https://static.case.law/ny/',
    abbreviation: 'NY'
  },
  ny2: {
    name: 'New York',
    url: 'https://static.case.law/ny-2d/',
    abbreviation: 'NY2D'
  },
  ny3: {
    name: 'New York',
    url: 'https://static.case.law/ny3d/',
    abbreviation: 'NY3D'
  },
  ca: {
    name: 'California',
    url: 'https://static.case.law/ca/',
    abbreviation: 'CA'
  },
  us: {
    name: 'United States',
    url: 'https://static.case.law/us/',
    abbreviation: 'US'
  }
};

export const DEFAULT_JURISDICTION = 'ny';

export const PATHS = {
  zips: './data/zips',
  extracted: './data/extracted',
  processed: './data/processed'
};

export const DATABASE_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  database: process.env.DB_NAME || 'caselaw',
  user: process.env.DB_USER || 'postgres',
  password: process.env.DB_PASSWORD || 'postgres'
};

export const SCRAPER_CONFIG = {
  headless: true,
  timeout: 30000,
  userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
};

export const EXTRACTION_CONFIG = {
  batchSize: 100,
  maxConcurrency: 5,
  retryAttempts: 3,
  // Delay between starting downloads (ms) to avoid hammering servers
  downloadDelayMs: 1000
};

/**
 * Get jurisdiction config by key
 */
export function getJurisdiction(key = DEFAULT_JURISDICTION) {
  const jurisdiction = JURISDICTIONS[key.toLowerCase()];
  if (!jurisdiction) {
    throw new Error(`Unknown jurisdiction: ${key}. Available: ${Object.keys(JURISDICTIONS).join(', ')}`);
  }
  return jurisdiction;
}

/**
 * Get command line arguments
 */
export function getCliArgs() {
  const args = process.argv.slice(2);
  const jurisdiction = args.find(arg => arg.startsWith('--jurisdiction='))?.split('=')[1] || DEFAULT_JURISDICTION;
  const verbose = args.includes('--verbose') || args.includes('-v');
  const dryRun = args.includes('--dry-run');
  const limit = args.find(arg => arg.startsWith('--limit='))?.split('=')[1];
  const offset = args.find(arg => arg.startsWith('--offset='))?.split('=')[1];
  const sample = args.find(arg => arg.startsWith('--sample='))?.split('=')[1];
  const stats = args.includes('--stats');
  const help = args.includes('--help') || args.includes('-h');
  const config = args.find(arg => arg.startsWith('--config='))?.split('=')[1];
  
  return {
    jurisdiction,
    verbose,
    dryRun,
    limit,
    offset,
    sample,
    stats,
    help,
    config
  };
}
