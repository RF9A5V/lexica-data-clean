/**
 * Extract ZIP files and process JSON case data
 */

import fs from 'fs/promises';
import { createWriteStream } from 'fs';
import path from 'path';
import yauzl from 'yauzl';
import { promisify } from 'util';
import { getJurisdiction, getCliArgs, PATHS, EXTRACTION_CONFIG } from './config.js';

const openZip = promisify(yauzl.open);

/**
 * Extract required fields from a case JSON object.
 *
 * `provenance` attaches source-volume context so the loader can rebuild the
 * official citation from the ZIP archive's folder+filename instead of trusting
 * CAP's free-text `citations[].cite` string, which is often wrong (e.g. a 2004
 * case in the ny3d ZIP mislabeled as "N.Y." 1st series).
 */
function extractCaseFields(caseData, provenance = {}) {
  try {
    const extracted = {
      // Basic case information
      id: caseData.id,
      name: caseData.name,
      name_abbreviation: caseData.name_abbreviation,
      decision_date: caseData.decision_date,

      // Citations
      citations: caseData.citations || [],

      // Court information
      court_name: caseData.court?.name || null,
      court_name_abbreviation: caseData.court?.name_abbreviation || null,
      court_id: caseData.court?.id || null,

      // Jurisdiction
      jurisdiction_name: caseData.jurisdiction?.name_long || null,
      jurisdiction_abbreviation: caseData.jurisdiction?.name || null,
      jurisdiction_id: caseData.jurisdiction?.id || null,

      // Citations to other cases
      cites_to: caseData.cites_to || [],

      // Opinions
      opinions: caseData.casebody?.opinions || [],

      // Additional metadata that might be useful
      docket_number: caseData.docket_number || null,
      first_page: caseData.first_page || null,
      last_page: caseData.last_page || null,
      file_name: caseData.file_name || null,

      // Archive provenance — not sourced from CAP, populated by the extractor.
      _source_id: provenance.source_id ?? null,
      _file_volume: provenance.file_volume ?? null
    };

    return extracted;
  } catch (error) {
    throw new Error(`Failed to extract fields from case ${caseData?.id || 'unknown'}: ${error.message}`);
  }
}

/**
 * Extract a single ZIP file and process JSON files
 */
async function extractZipFile(zipPath, outputDir, options = {}) {
  const { verbose = false } = options;
  
  if (verbose) console.log(`  Extracting: ${path.basename(zipPath)}`);
  
  const results = {
    zipFile: path.basename(zipPath),
    jsonFiles: 0,
    casesProcessed: 0,
    errors: []
  };
  
  // Volume is encoded in the ZIP filename (e.g. "2.zip" → volume 2).
  // Source id is the immediate parent directory of outputDir
  // (e.g. data/extracted/ny3d → "ny3d").
  const volumeRaw = path.basename(zipPath, '.zip');
  const fileVolume = Number.isFinite(parseInt(volumeRaw, 10)) ? parseInt(volumeRaw, 10) : null;
  const sourceId = path.basename(outputDir) || null;
  const provenance = { source_id: sourceId, file_volume: fileVolume };

  try {
    const zipFile = await openZip(zipPath, { lazyEntries: true });
    const processedCases = [];

    await new Promise((resolve, reject) => {
      zipFile.readEntry();

      zipFile.on('entry', async (entry) => {
        try {
          // Only process JSON files in 'json' directories
          if ((entry.fileName.includes('/json/') || entry.fileName.startsWith('json/')) && entry.fileName.endsWith('.json')) {
            results.jsonFiles++;

            // Extract the JSON content
            const jsonContent = await new Promise((resolveJson, rejectJson) => {
              zipFile.openReadStream(entry, (err, readStream) => {
                if (err) return rejectJson(err);

                const chunks = [];
                readStream.on('data', chunk => chunks.push(chunk));
                readStream.on('end', () => {
                  try {
                    const jsonString = Buffer.concat(chunks).toString('utf8');
                    const jsonData = JSON.parse(jsonString);
                    resolveJson(jsonData);
                  } catch (parseError) {
                    rejectJson(new Error(`JSON parse error in ${entry.fileName}: ${parseError.message}`));
                  }
                });
                readStream.on('error', rejectJson);
              });
            });

            // Extract required fields
            const extractedCase = extractCaseFields(jsonContent, provenance);
            processedCases.push(extractedCase);
            results.casesProcessed++;
            
            if (verbose && results.casesProcessed % 100 === 0) {
              console.log(`    Processed ${results.casesProcessed} cases...`);
            }
          }
          
          zipFile.readEntry();
          
        } catch (error) {
          results.errors.push({
            file: entry.fileName,
            error: error.message
          });
          zipFile.readEntry();
        }
      });
      
      zipFile.on('end', () => {
        resolve(processedCases);
      });
      
      zipFile.on('error', reject);
    });
    
    // Save processed cases to JSON file
    if (processedCases.length > 0) {
      const outputFile = path.join(outputDir, `${path.basename(zipPath, '.zip')}.json`);
      await fs.mkdir(path.dirname(outputFile), { recursive: true });
      await fs.writeFile(outputFile, JSON.stringify(processedCases, null, 2));
      
      if (verbose) {
        console.log(`    Saved ${processedCases.length} cases to: ${path.basename(outputFile)}`);
      }
    }
    
    zipFile.close();
    
  } catch (error) {
    results.errors.push({
      file: 'ZIP_EXTRACTION',
      error: error.message
    });
  }
  
  return results;
}

/**
 * Process multiple ZIP files with concurrency control
 */
async function extractAllZips(zipDir, outputDir, options = {}) {
  const { 
    verbose = false, 
    maxConcurrency = EXTRACTION_CONFIG.maxConcurrency 
  } = options;
  
  // Find all ZIP files
  const zipFiles = [];
  try {
    const files = await fs.readdir(zipDir);
    for (const file of files) {
      if (file.endsWith('.zip')) {
        zipFiles.push(path.join(zipDir, file));
      }
    }
  } catch (error) {
    throw new Error(`Could not read ZIP directory: ${zipDir}`);
  }
  
  if (zipFiles.length === 0) {
    throw new Error(`No ZIP files found in: ${zipDir}`);
  }
  
  console.log(`Found ${zipFiles.length} ZIP files to process`);
  
  const overallResults = {
    totalZips: zipFiles.length,
    processedZips: 0,
    totalJsonFiles: 0,
    totalCases: 0,
    errors: []
  };
  
  // Create a semaphore for concurrency control
  const semaphore = Array(maxConcurrency).fill(null).map(() => Promise.resolve());
  let semaphoreIndex = 0;
  
  const extractPromises = zipFiles.map(async (zipPath, index) => {
    // Wait for an available slot
    await semaphore[semaphoreIndex];
    const currentSlot = semaphoreIndex;
    semaphoreIndex = (semaphoreIndex + 1) % maxConcurrency;
    
    try {
      const result = await extractZipFile(zipPath, outputDir, { verbose });
      
      overallResults.processedZips++;
      overallResults.totalJsonFiles += result.jsonFiles;
      overallResults.totalCases += result.casesProcessed;
      overallResults.errors.push(...result.errors);
      
      if (verbose) {
        console.log(`  ✅ Processed: ${result.zipFile} (${result.casesProcessed} cases, ${overallResults.processedZips}/${overallResults.totalZips})`);
      }
      
    } catch (error) {
      overallResults.errors.push({
        file: path.basename(zipPath),
        error: error.message
      });
      
      if (verbose) {
        console.error(`  ❌ Failed: ${path.basename(zipPath)} - ${error.message}`);
      }
    } finally {
      // Release the semaphore slot
      semaphore[currentSlot] = Promise.resolve();
    }
  });
  
  await Promise.all(extractPromises);
  
  return overallResults;
}

/**
 * Combine all extracted JSON files into a single dataset
 */
async function combineExtractedData(extractedDir, outputFile, options = {}) {
  const { verbose = false } = options;
  
  if (verbose) console.log('Combining extracted data...');
  
  // Ensure directory exists
  try {
    await fs.access(extractedDir);
  } catch {
    await fs.mkdir(extractedDir, { recursive: true });
    if (verbose) console.log(`  Created directory: ${extractedDir}`);
  }
  
  const files = await fs.readdir(extractedDir);
  const out = createWriteStream(outputFile, { flags: 'w' });
  let total = 0;
  
  // Helper to throttle writes if internal buffer is full
  const writeLine = (line) => new Promise((resolve, reject) => {
    const ok = out.write(line);
    if (!ok) {
      out.once('drain', resolve);
    } else {
      resolve();
    }
  });
  
  try {
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      const filePath = path.join(extractedDir, file);
      const content = JSON.parse(await fs.readFile(filePath, 'utf8'));
      
      if (Array.isArray(content)) {
        for (const obj of content) {
          await writeLine(`${JSON.stringify(obj)}\n`);
          total++;
        }
        if (verbose) console.log(`  Added ${content.length} cases from ${file}`);
      } else {
        await writeLine(`${JSON.stringify(content)}\n`);
        total++;
        if (verbose) console.log(`  Added 1 case from ${file}`);
      }
    }
  } finally {
    await new Promise((resolve) => {
      out.end(resolve);
    });
  }
  
  if (verbose) {
    console.log(`Combined ${total} cases into: ${path.basename(outputFile)} (NDJSON)`);
  }
  
  return total;
}

/**
 * Main extractor function
 */
async function main() {
  try {
    const { jurisdiction, verbose, dryRun } = getCliArgs();
    const jurisdictionConfig = getJurisdiction(jurisdiction);
    
    console.log(`Extracting case data for ${jurisdictionConfig.name} (${jurisdiction.toUpperCase()})`);
    
    const zipDir = path.join(PATHS.zips, jurisdiction);
    const extractedDir = path.join(PATHS.extracted, jurisdiction);
    const combinedFile = path.join(PATHS.processed, `${jurisdiction}-cases.json`);
    
    if (dryRun) {
      console.log('Dry run - files not extracted');
      return;
    }
    
    const startTime = Date.now();
    
    // Extract all ZIP files
    const results = await extractAllZips(zipDir, extractedDir, { 
      verbose,
      maxConcurrency: EXTRACTION_CONFIG.maxConcurrency 
    });
    
    // Combine all extracted data
    const totalCases = await combineExtractedData(extractedDir, combinedFile, { verbose });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('\n📊 Extraction Summary:');
    console.log(`  ZIP files processed: ${results.processedZips}/${results.totalZips}`);
    console.log(`  JSON files found: ${results.totalJsonFiles}`);
    console.log(`  Cases extracted: ${results.totalCases}`);
    console.log(`  Combined cases: ${totalCases}`);
    console.log(`  Duration: ${duration}s`);
    
    if (results.errors.length > 0) {
      console.log(`\n⚠️  ${results.errors.length} errors occurred:`);
      results.errors.slice(0, 10).forEach(({ file, error }) => {
        console.log(`  ${file}: ${error}`);
      });
      if (results.errors.length > 10) {
        console.log(`  ... and ${results.errors.length - 10} more errors`);
      }
    }
    
    console.log(`✅ Extraction completed! Combined data saved to: ${combinedFile}`);
    
  } catch (error) {
    console.error('❌ Extraction failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// Export functions for use in other modules
export { extractCaseFields, extractZipFile, extractAllZips, combineExtractedData };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
