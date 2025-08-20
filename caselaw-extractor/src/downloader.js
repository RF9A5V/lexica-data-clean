/**
 * Download ZIP files from case.law URLs
 */

import fs from 'fs/promises';
import { createWriteStream } from 'fs';
import path from 'path';
import https from 'https';
import http from 'http';
import { getJurisdiction, getCliArgs, PATHS, EXTRACTION_CONFIG } from './config.js';

/**
 * Download a single file with progress tracking
 */
async function downloadFile(url, outputPath, options = {}) {
  const { verbose = false, retryAttempts = EXTRACTION_CONFIG.retryAttempts } = options;
  
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  
  for (let attempt = 1; attempt <= retryAttempts; attempt++) {
    try {
      if (verbose) console.log(`  Downloading: ${path.basename(outputPath)} (attempt ${attempt}/${retryAttempts})`);
      
      await new Promise((resolve, reject) => {
        const protocol = url.startsWith('https:') ? https : http;
        const file = createWriteStream(outputPath);
        
        const request = protocol.get(url, (response) => {
          if (response.statusCode === 302 || response.statusCode === 301) {
            // Handle redirects
            file.close();
            return downloadFile(response.headers.location, outputPath, options)
              .then(resolve)
              .catch(reject);
          }
          
          if (response.statusCode !== 200) {
            file.close();
            reject(new Error(`HTTP ${response.statusCode}: ${response.statusMessage}`));
            return;
          }
          
          const totalSize = parseInt(response.headers['content-length'], 10);
          let downloadedSize = 0;
          
          response.on('data', (chunk) => {
            downloadedSize += chunk.length;
            if (verbose && totalSize) {
              const percent = ((downloadedSize / totalSize) * 100).toFixed(1);
              process.stdout.write(`\r    Progress: ${percent}% (${downloadedSize}/${totalSize} bytes)`);
            }
          });
          
          response.pipe(file);
          
          file.on('finish', async () => {
            file.close();
            if (verbose && totalSize) process.stdout.write('\n');
            
            // Validate the downloaded file is a ZIP
            try {
              const fd = await fs.open(outputPath, 'r');
              const buffer = Buffer.alloc(4);
              await fd.read(buffer, 0, 4, 0);
              await fd.close();
              
              const isZip = buffer.slice(0, 2).toString() === 'PK';
              if (!isZip) {
                throw new Error('Downloaded file is not a valid ZIP archive');
              }
              
              resolve();
            } catch (validationError) {
              reject(new Error(`File validation failed: ${validationError.message}`));
            }
          });
          
          file.on('error', (err) => {
            file.close();
            fs.unlink(outputPath).catch(() => {}); // Clean up on error
            reject(err);
          });
        });
        
        request.on('error', (err) => {
          reject(err);
        });
        
        request.setTimeout(60000, () => {
          request.destroy();
          reject(new Error('Download timeout'));
        });
      });
      
      // Success - break out of retry loop
      break;
      
    } catch (error) {
      if (attempt === retryAttempts) {
        throw new Error(`Failed to download ${url} after ${retryAttempts} attempts: ${error.message}`);
      }
      
      if (verbose) console.log(`    Attempt ${attempt} failed: ${error.message}`);
      
      // Wait before retrying
      await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
    }
  }
}

/**
 * Download multiple files with concurrency control
 */
async function downloadFiles(urls, outputDir, options = {}) {
  const {
    verbose = false,
    maxConcurrency = EXTRACTION_CONFIG.maxConcurrency,
    skipExisting = true,
    retryAttempts = EXTRACTION_CONFIG.retryAttempts,
    downloadDelayMs = EXTRACTION_CONFIG.downloadDelayMs
  } = options;

  await fs.mkdir(outputDir, { recursive: true });

  const results = {
    total: urls.length,
    downloaded: 0,
    skipped: 0,
    failed: 0,
    errors: []
  };

  // Simple global rate limiter to space out download starts
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let nextStartAt = Date.now();
  let gate = Promise.resolve();
  async function scheduleStart() {
    // Ensure calls enter one-by-one
    const prev = gate;
    let release;
    gate = new Promise((r) => (release = r));
    await prev.catch(() => {});
    if (downloadDelayMs > 0) {
      const now = Date.now();
      const wait = Math.max(0, nextStartAt - now);
      if (wait > 0) await sleep(wait);
      nextStartAt = Date.now() + downloadDelayMs;
    }
    release();
  }

  // Worker pool for concurrency control
  let index = 0;
  const workerCount = Math.max(1, Math.min(maxConcurrency, urls.length));

  async function worker() {
    while (true) {
      const i = index++;
      if (i >= urls.length) break;
      const url = urls[i];
      const filename = path.basename(new URL(url).pathname);
      const outputPath = path.join(outputDir, filename);

      // Skip if already exists
      if (skipExisting) {
        try {
          await fs.access(outputPath);
          if (verbose) console.log(`  Skipping existing file: ${filename}`);
          results.skipped++;
          continue;
        } catch {}
      }

      // Rate limit the start of this download
      await scheduleStart();

      try {
        await downloadFile(url, outputPath, { verbose, retryAttempts });
        results.downloaded++;
        if (verbose) {
          console.log(`  ✅ Downloaded: ${filename} (${results.downloaded + results.skipped}/${results.total})`);
        }
      } catch (error) {
        results.failed++;
        results.errors.push({ url, error: error.message });
        if (verbose) {
          console.error(`  ❌ Failed: ${filename} - ${error.message}`);
        }
      }
    }
  }

  await Promise.all(Array.from({ length: workerCount }, () => worker()));

  return results;
}

/**
 * Load URLs from a saved JSON file
 */
async function loadUrls(jurisdiction) {
  const urlsFile = `./data/processed/${jurisdiction}-zip-urls.json`;
  
  try {
    const data = JSON.parse(await fs.readFile(urlsFile, 'utf8'));
    return data.urls;
  } catch (error) {
    throw new Error(`Could not load URLs file: ${urlsFile}. Run scraper first.`);
  }
}

/**
 * Main downloader function
 */
async function main() {
  try {
    const { jurisdiction, verbose, dryRun } = getCliArgs();
    const jurisdictionConfig = getJurisdiction(jurisdiction);
    
    console.log(`Downloading ZIP files for ${jurisdictionConfig.name} (${jurisdiction.toUpperCase()})`);
    
    const urls = await loadUrls(jurisdiction);
    console.log(`Found ${urls.length} ZIP files to download`);
    
    if (dryRun) {
      console.log('Dry run - files not downloaded');
      urls.forEach((url, index) => {
        console.log(`  ${index + 1}. ${path.basename(new URL(url).pathname)}`);
      });
      return;
    }
    
    const outputDir = path.join(PATHS.zips, jurisdiction);
    const startTime = Date.now();
    
    const results = await downloadFiles(urls, outputDir, {
      verbose,
      maxConcurrency: EXTRACTION_CONFIG.maxConcurrency,
      retryAttempts: EXTRACTION_CONFIG.retryAttempts,
      downloadDelayMs: EXTRACTION_CONFIG.downloadDelayMs
    });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('\n📊 Download Summary:');
    console.log(`  Total files: ${results.total}`);
    console.log(`  Downloaded: ${results.downloaded}`);
    console.log(`  Skipped: ${results.skipped}`);
    console.log(`  Failed: ${results.failed}`);
    console.log(`  Duration: ${duration}s`);
    
    if (results.errors.length > 0) {
      console.log('\n❌ Errors:');
      results.errors.forEach(({ url, error }) => {
        console.log(`  ${path.basename(new URL(url).pathname)}: ${error}`);
      });
    }
    
    if (results.failed === 0) {
      console.log('✅ All downloads completed successfully!');
    } else {
      console.log(`⚠️  ${results.failed} downloads failed`);
      process.exit(1);
    }
    
  } catch (error) {
    console.error('❌ Download failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// Export functions for use in other modules
export { downloadFile, downloadFiles, loadUrls };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
