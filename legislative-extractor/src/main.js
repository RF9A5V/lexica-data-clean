#!/usr/bin/env node

/**
 * Main orchestrator script for legislative data extraction
 *
 * Usage:
 *   node src/main.js [--config=../configs/rcny.json] [--step=all|fetch|extract|parse|load] [--verbose] [--dry-run]
 */

import dotenv from 'dotenv';
import { parseArgs } from 'node:util';
import { loadConfig } from './config_file.js';
import { fetchZipFiles, extractZipContents } from './zip_processor.js';
import { parseXmlToNdjson } from './xml_statute_parser.js';
import { loadNdjsonToDatabase } from './statute_loader.js';
import fs from 'fs/promises';
import path from 'path';

// Load environment variables
dotenv.config();

const VALID_STEPS = ['all', 'fetch', 'extract', 'parse', 'load'];

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const configPath = args.find(arg => arg.startsWith('--config='))?.split('=')[1];
  const step = args.find(arg => arg.startsWith('--step='))?.split('=')[1];
  const verbose = args.includes('--verbose');
  const dryRun = args.includes('--dry-run');
  const errorOnly = args.includes('--error-only');
  const help = args.includes('--help') || args.includes('-h');

  return { configPath, step, verbose, dryRun, errorOnly, help };
}

/**
 * Display help information
 */
function showHelp() {
  console.log(`
Legislative Data Extraction Tool

Usage:
  node src/main.js [options]

Options:
  --config=<path>         Configuration file path (required)
  --step=<step>           Step to run [default: all]
                          Steps: fetch, extract, parse, load, all
  --verbose, -v           Verbose output
  --dry-run              Show what would be done without executing
  --help, -h             Show this help message

Examples:
  node src/main.js --config=../configs/rcny.json
  node src/main.js --config=../configs/rcny.json --step=fetch --verbose
  node src/main.js --config=../configs/rcny.json --step=parse --dry-run
`);
}

/**
 * Step 1: Fetch ZIP files from configured URLs
 */
async function runFetchStep(config, options) {
  const { verbose, dryRun } = options;

  console.log('\n⬇️  Step 1: Fetching ZIP files...');

  for (const source of config.sources) {
    console.log(`\n=== Processing source: ${source.label} (${source.id}) ===`);

    if (!source.zip_urls || source.zip_urls.length === 0) {
      console.warn(`No ZIP URLs configured for source ${source.id}`);
      continue;
    }

    const results = await fetchZipFiles(source, config, { verbose, dryRun });

    console.log(`✅ Source ${source.id}: ${results.downloaded} downloaded, ${results.skipped} skipped, ${results.failed} failed`);
  }
}

/**
 * Step 2: Extract ZIP contents to staging directories
 */
async function runExtractStep(config, options) {
  const { verbose, dryRun } = options;

  console.log('\n📦 Step 2: Extracting ZIP contents...');

  for (const source of config.sources) {
    console.log(`\n=== Processing source: ${source.label} (${source.id}) ===`);

    const stagingDir = source.staging_dir || `./data/staging/${source.id}`;
    const zipDir = source.zip_dir || `./data/zips/${source.id}`;

    const results = await extractZipContents(source, zipDir, stagingDir, { verbose, dryRun });

    console.log(`✅ Source ${source.id}: ${results.processedZips} ZIPs processed, ${results.extractedFiles} files extracted`);
  }
}

/**
 * Step 3: Parse XML files to NDJSON format
 */
async function runParseStep(config, options) {
  const { verbose, dryRun, errorOnly } = options;

  console.log('\n🔍 Step 3: Parsing XML to NDJSON...');

  for (const source of config.sources) {
    console.log(`\n=== Processing source: ${source.label} (${source.id}) ===`);

    const stagingDir = source.staging_dir || `./data/staging/${source.id}`;
    const ndjsonOutput = source.ndjson_output || `./data/processed/${source.id}-statutes.ndjson`;

    const results = await parseXmlToNdjson(source, stagingDir, ndjsonOutput, { verbose, dryRun, errorOnly });

    console.log(`✅ Source ${source.id}: ${results.processedFiles} XML files processed, ${results.ndjsonLines} NDJSON lines generated`);
  }
}

/**
 * Step 4: Load NDJSON data into legislative database
 */
async function runLoadStep(config, options) {
  const { verbose, dryRun } = options;

  console.log('\n🗄️  Step 4: Loading data into database...');

  for (const source of config.sources) {
    console.log(`\n=== Processing source: ${source.label} (${source.id}) ===`);

    const ndjsonFile = source.ndjson_output || `./data/processed/${source.id}-statutes.ndjson`;

    const results = await loadNdjsonToDatabase(source, ndjsonFile, config.database, { verbose, dryRun });

    console.log(`✅ Source ${source.id}: ${results.insertedUnits} units inserted, ${results.insertedVersions} text versions created`);
  }
}

/**
 * Main execution function
 */
async function main() {
  const startTime = Date.now();

  try {
    const { configPath, step, verbose, dryRun, errorOnly, help } = parseCliArgs();

    if (help) {
      showHelp();
      return;
    }

    if (!configPath) {
      console.error('❌ Error: --config=<path> is required');
      showHelp();
      process.exit(1);
    }

    if (!VALID_STEPS.includes(step)) {
      console.error(`❌ Error: Invalid step '${step}'. Valid steps: ${VALID_STEPS.join(', ')}`);
      process.exit(1);
    }

    // Load configuration
    console.log(`📋 Loading configuration from: ${configPath}`);
    const config = await loadConfig(configPath);

    console.log(`🚀 Legislative Data Extraction Tool`);
    console.log(`Config: ${configPath}`);
    console.log(`Sources: ${config.sources.map(s => s.id).join(', ')}`);
    console.log(`Step: ${step}`);
    console.log(`Mode: ${verbose ? 'VERBOSE' : 'NORMAL'}${errorOnly ? ' (ERRORS ONLY)' : ''}`);

    const options = { verbose, dryRun, errorOnly };

    // Execute the specified step
    switch (step) {
      case 'all':
        await runFetchStep(config, options);
        await runExtractStep(config, options);
        await runParseStep(config, options);
        await runLoadStep(config, options);
        break;
      case 'fetch':
        await runFetchStep(config, options);
        break;
      case 'extract':
        await runExtractStep(config, options);
        break;
      case 'parse':
        await runParseStep(config, options);
        break;
      case 'load':
        await runLoadStep(config, options);
        break;
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n🎉 Completed successfully in ${duration}s!`);

    if (step === 'all') {
      console.log('\n📋 Next steps:');
      console.log('  • Review the legislative database schema and data');
      console.log('  • Run citation extraction on the loaded legislative texts');
      console.log('  • Register the legislative source in co-collection');
      console.log('  • Test cross-database citation resolution');
    }

  } catch (error) {
    console.error('\n❌ Execution failed:', error.message);

    if (process.env.NODE_ENV === 'development') {
      console.error('\nStack trace:');
      console.error(error.stack);
    }

    console.log('\n💡 Troubleshooting tips:');
    console.log('  • Check your configuration file syntax');
    console.log('  • Verify database connection settings');
    console.log('  • Run with --verbose for more details');
    console.log('  • Try running individual steps with --step=<step>');

    process.exit(1);
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
