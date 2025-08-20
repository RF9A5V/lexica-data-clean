#!/usr/bin/env node

/**
 * Main orchestrator script for case.law data extraction
 * 
 * Usage:
 *   node main.js [--jurisdiction=ny] [--verbose] [--dry-run] [--step=all|scrape|download|extract|load]
 */

import dotenv from 'dotenv';
import { getJurisdiction, getCliArgs, EXTRACTION_CONFIG } from './src/config.js';
import { loadConfig } from './src/config_file.js';
import { extractCitationsForSource, extractCitationsFromSourceRef } from './src/citation_extractor.js';

// Load environment variables
dotenv.config();
import { scrapeZipUrls, saveUrls } from './src/scraper.js';
import { loadUrls, downloadFiles } from './src/downloader.js';
import { extractAllZips, combineExtractedData } from './src/extractor.js';
import { createPool, createSchema, loadCasesFromFile, getDatabaseStats, insertMetadata } from './src/database.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Parse command line arguments for step selection
 */
function parseStepArg() {
  const args = process.argv.slice(2);
  const stepArg = args.find(arg => arg.startsWith('--step='))?.split('=')[1];
  
  const validSteps = ['all', 'scrape', 'download', 'extract', 'extract-citations', 'load'];
  const step = stepArg || 'all';
  
  if (!validSteps.includes(step)) {
    throw new Error(`Invalid step: ${step}. Valid steps: ${validSteps.join(', ')}`);
  }
  
  return step;
}

/**
 * Display help information
 */
function showHelp() {
  console.log(`
Case.law Data Extraction Tool

Usage:
  node main.js [options]

Options:
  --jurisdiction=<code>    Jurisdiction to process (ny, ca, us, etc.) [default: ny]
  --step=<step>           Step to run [default: all]
                          Steps: scrape, download, extract, extract-citations, load, all
  --verbose, -v           Verbose output
  --dry-run              Show what would be done without executing
  --config=<path>        Use JSON config with multiple sources (overrides --jurisdiction)
  --help, -h             Show this help message

Examples:
  node main.js                                    # Process NY with all steps
  node main.js --jurisdiction=ca --verbose        # Process CA with verbose output
  node main.js --step=scrape --dry-run           # Just scrape URLs (dry run)
  node main.js --step=load                       # Only load data to database
  node main.js --config=./configs/ny_coa.json --step=extract-citations --verbose # Extract citations into app DB
  node main.js --source=nycoa --step=extract-citations --verbose --dry-run --limit=200 # Use app DB source ref

Available Jurisdictions:
  ny  - New York
  ca  - California  
  us  - United States
`);
}

/**
 * Step 1: Scrape ZIP URLs
 */
async function runScrapeStep(jurisdiction, options) {
  const { verbose, dryRun } = options;
  
  console.log('\n🔍 Step 1: Scraping ZIP URLs...');
  
  const jurisdictionConfig = getJurisdiction(jurisdiction);
  const urls = await scrapeZipUrls(jurisdictionConfig.url, { verbose });
  
  if (urls.length === 0) {
    throw new Error('No ZIP files found!');
  }
  
  if (!dryRun) {
    await saveUrls(urls, jurisdiction);
  }
  
  console.log(`✅ Found ${urls.length} ZIP files`);
  return urls;
}

/**
 * Step 2: Download ZIP files
 */
async function runDownloadStep(jurisdiction, options) {
  const { verbose, dryRun } = options;
  
  console.log('\n⬇️  Step 2: Downloading ZIP files...');
  
  const urls = await loadUrls(jurisdiction);
  const outputDir = path.resolve(`./data/zips/${jurisdiction}`);
  
  if (dryRun) {
    console.log(`Would download ${urls.length} files to: ${outputDir}`);
    return { downloaded: 0, skipped: urls.length, failed: 0 };
  }
  
  const results = await downloadFiles(urls, outputDir, {
    verbose,
    maxConcurrency: EXTRACTION_CONFIG.maxConcurrency,
    retryAttempts: EXTRACTION_CONFIG.retryAttempts,
    downloadDelayMs: EXTRACTION_CONFIG.downloadDelayMs
  });
  
  console.log(`✅ Downloaded ${results.downloaded} files (${results.skipped} skipped, ${results.failed} failed)`);
  return results;
}

/**
 * Step 3: Extract and process JSON files
 */
async function runExtractStep(jurisdiction, options) {
  const { verbose, dryRun } = options;
  
  console.log('\n📦 Step 3: Extracting and processing JSON files...');
  
  if (dryRun) {
    console.log('Would extract ZIP files and process JSON data');
    return { totalCases: 0 };
  }
  
  const zipDir = path.resolve(`./data/zips/${jurisdiction}`);
  const extractedDir = path.resolve(`./data/extracted/${jurisdiction}`);
  const combinedFile = path.resolve(`./data/processed/${jurisdiction}-cases.json`);
  
  const results = await extractAllZips(zipDir, extractedDir, { verbose });
  const totalCases = await combineExtractedData(extractedDir, combinedFile, { verbose });
  
  console.log(`✅ Extracted ${results.totalCases} cases from ${results.processedZips} ZIP files`);
  return { ...results, totalCases };
}

/**
 * Config-driven multi-source pipeline
 */
async function runConfigPipeline(configPath, step, options) {
  const { verbose, dryRun, limit, offset } = options;
  const cfg = await loadConfig(configPath);

  console.log(`\n🧩 Config-driven mode: ${configPath}`);
  if (verbose) {
    console.log(`Sources: ${cfg.sources.map(s => s.id).join(', ')}`);
  }

  for (const source of cfg.sources) {
    const sourceId = source.id;
    const label = source.label || sourceId;
    console.log(`\n=== Source: ${label} (${sourceId}) ===`);

    let urls = [];

    // SCRAPE
    if (step === 'all' || step === 'scrape') {
      console.log('\n🔍 Step 1: Scraping ZIP URLs...');
      const scraped = [];
      for (const baseUrl of source.scrape_urls || []) {
        const found = await scrapeZipUrls(baseUrl, { verbose });
        scraped.push(...found);
      }
      const explicit = source.zip_urls || [];
      // Dedupe
      urls = Array.from(new Set([...(explicit || []), ...scraped]));

      if (urls.length === 0) {
        console.warn('No ZIP files found for this source.');
      }

      if (!dryRun) {
        const out = source.urls_file || `./data/processed/${sourceId}-zip-urls.json`;
        await saveUrls(urls, sourceId, out);
      } else if (verbose) {
        console.log('Dry run - URLs not saved');
      }
    }

    // DOWNLOAD
    if (step === 'all' || step === 'download') {
      console.log('\n⬇️  Step 2: Downloading ZIP files...');
      if (!urls.length) {
        if (source.zip_urls && source.zip_urls.length) {
          urls = source.zip_urls;
        } else if (source.urls_file) {
          try {
            const data = JSON.parse(await fs.readFile(path.resolve(source.urls_file), 'utf8'));
            urls = Array.isArray(data?.urls) ? data.urls : (Array.isArray(data) ? data : []);
          } catch {
            urls = await loadUrls(sourceId);
          }
        } else {
          urls = await loadUrls(sourceId);
        }
      }

      const outputDir = path.resolve(`./data/zips/${sourceId}`);
      if (dryRun) {
        console.log(`Would download ${urls.length} files to: ${outputDir}`);
      } else {
        await downloadFiles(urls, outputDir, {
          verbose,
          maxConcurrency: cfg.options.maxConcurrency,
          retryAttempts: cfg.options.retryAttempts,
          downloadDelayMs: cfg.options.downloadDelayMs
        });
      }
    }

    // EXTRACT
    if (step === 'all' || step === 'extract') {
      console.log('\n📦 Step 3: Extracting and processing JSON files...');
      if (dryRun) {
        console.log('Would extract ZIP files and process JSON data');
      } else {
        const zipDir = path.resolve(`./data/zips/${sourceId}`);
        const extractedDir = path.resolve(`./data/extracted/${sourceId}`);
        const combinedFile = path.resolve(`./data/processed/${sourceId}-cases.json`);
        const results = await extractAllZips(zipDir, extractedDir, { verbose });
        const totalCases = await combineExtractedData(extractedDir, combinedFile, { verbose });
        if (verbose) console.log(`  Extracted ${results.totalCases} cases; combined total: ${totalCases}`);
      }
    }

    // EXTRACT-CITATIONS
    if (step === 'extract-citations') {
      console.log('\n🔎 Step 3b: Extracting citations from opinions...');
      if (dryRun) {
        console.log('Would parse opinions, extract citations, and upsert into app DB extracted_citations');
      } else {
        const sourceRef = source.source_ref || process.env.EXTRACT_CITATIONS_SOURCE_REF || sourceId;
        const res = await extractCitationsForSource({
          configPath,
          sourceId,
          sourceRef,
          verbose,
          dryRun,
          limit,
          offset
        });
        console.log(`  ✅ Citations extracted: ${res.totalExtracted} (from ${res.processedCases} cases)`);
      }
    }

    // LOAD
    if (step === 'all' || step === 'load') {
      console.log('\n🗄️  Step 4: Loading data into database...');
      if (dryRun) {
        console.log('Would create database schema and load case data');
      } else {
        const pool = createPool(cfg.database);
        try {
          const client = await pool.connect();
          await client.query('SELECT NOW()');
          client.release();
          await createSchema(pool, { verbose });
          // Write config metadata (jurisdiction, court_level, reporter, etc.)
          await insertMetadata(pool, cfg.metadata, { verbose });
          const dataFile = path.resolve(`./data/processed/${sourceId}-cases.json`);
          const result = await loadCasesFromFile(pool, dataFile, { verbose });
          const stats = await getDatabaseStats(pool);
          console.log(`  ✅ Loaded ${result.inserted} cases (${result.skipped} skipped)`);
          console.log(`  📊 Database now contains ${stats.cases} total cases`);
        } finally {
          await pool.end();
        }
      }
    }
  }
}

/**
 * Step 4: Load data into database
 */
async function runLoadStep(jurisdiction, options) {
  const { verbose, dryRun } = options;
  
  console.log('\n🗄️  Step 4: Loading data into database...');
  
  if (dryRun) {
    console.log('Would create database schema and load case data');
    return { inserted: 0, skipped: 0 };
  }
  
  const pool = createPool();
  
  try {
    // Test connection
    const client = await pool.connect();
    await client.query('SELECT NOW()');
    client.release();
    
    // Create schema
    await createSchema(pool, { verbose });
    
    // Load data
    const dataFile = path.resolve(`./data/processed/${jurisdiction}-cases.json`);
    const result = await loadCasesFromFile(pool, dataFile, { verbose });
    
    // Get statistics
    const stats = await getDatabaseStats(pool);
    
    console.log(`✅ Loaded ${result.inserted} cases (${result.skipped} skipped)`);
    console.log(`📊 Database now contains ${stats.cases} total cases`);
    
    return result;
    
  } finally {
    await pool.end();
  }
}

/**
 * Main execution function
 */
async function main() {
  try {
    const args = process.argv.slice(2);
    
    // Check for help
    if (args.includes('--help') || args.includes('-h')) {
      showHelp();
      return;
    }
    
    const { jurisdiction, verbose, dryRun, config, limit, offset } = getCliArgs();
    const step = parseStepArg();
    
    console.log(`🚀 Case.law Data Extraction Tool`);
    if (config) {
      console.log(`Mode: CONFIG (${config})`);
    } else {
      const jurisdictionConfig = getJurisdiction(jurisdiction);
      console.log(`Jurisdiction: ${jurisdictionConfig.name} (${jurisdiction.toUpperCase()})`);
      console.log(`URL: ${jurisdictionConfig.url}`);
    }
    console.log(`Step: ${step}`);
    if (dryRun) console.log('Mode: DRY RUN');
    if (verbose) console.log('Mode: VERBOSE');
    
    const startTime = Date.now();
    const options = { verbose, dryRun, limit, offset };
    
    if (config) {
      // Config-driven multi-source pipeline
      await runConfigPipeline(config, step, options);
    } else {
      // Execute requested steps (single jurisdiction mode)
      // Pass through the jurisdiction key from CLI
      const jurisdictionKey = jurisdiction;
      if (step === 'all' || step === 'scrape') {
        await runScrapeStep(jurisdictionKey, options);
      }
      
      if (step === 'all' || step === 'download') {
        await runDownloadStep(jurisdictionKey, options);
      }
      
      if (step === 'all' || step === 'extract') {
        await runExtractStep(jurisdictionKey, options);
      }
      
      if (step === 'extract-citations') {
        const args = process.argv.slice(2);
        const sourceRef = args.find(a => a.startsWith('--source='))?.split('=')[1];
        if (!sourceRef) {
          console.log('\nℹ️  Provide either --config with --source=<id> (config mode) or --source=<reference> (app DB mode).');
          console.log('    Example: node main.js --source=nycoa --step=extract-citations --verbose --dry-run');
          process.exit(1);
        }
        console.log('\n🔎 Step 3b: Extracting citations from opinions (app DB mode)...');
        if (options.dryRun) {
          console.log(`Would parse opinions for source_ref='${sourceRef}', extract citations, and (if not dry-run) upsert into app DB extracted_citations`);
        } else {
          const res = await extractCitationsFromSourceRef({
            sourceRef,
            limit: options.limit,
            offset: options.offset,
            verbose: options.verbose,
            dryRun: options.dryRun
          });
          console.log(`  ✅ Citations extracted: ${res.totalExtracted} (from ${res.processedCases} cases)`);
        }
      }
      
      if (step === 'all' || step === 'load') {
        await runLoadStep(jurisdictionKey, options);
      }
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log(`\n🎉 Completed successfully in ${duration}s!`);
    
    if (step === 'all') {
      console.log('\n📋 Next steps:');
      console.log('  • Review the database schema and data');
      console.log('  • Run queries to explore the case law data');
      console.log('  • Consider adding indexes for your specific use cases');
    }
    
  } catch (error) {
    console.error('\n❌ Execution failed:', error.message);
    
    if (process.env.NODE_ENV === 'development') {
      console.error('\nStack trace:');
      console.error(error.stack);
    }
    
    console.log('\n💡 Troubleshooting tips:');
    console.log('  • Check your internet connection');
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
