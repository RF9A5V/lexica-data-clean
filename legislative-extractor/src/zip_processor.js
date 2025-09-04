/**
 * ZIP file processing utilities
 * Handles downloading and extracting ZIP files containing legislative XML data
 */

import fs from 'fs/promises';
import path from 'path';
import { createWriteStream } from 'fs';
import https from 'https';
import http from 'http';
import yauzl from 'yauzl';
import { ZIP_CONFIG } from './config.js';

/**
 * Download ZIP files from configured URLs
 */
export async function fetchZipFiles(source, config, options = {}) {
  const { verbose = false, dryRun = false } = options;
  const results = { downloaded: 0, skipped: 0, failed: 0 };

  const zipDir = source.zip_dir || `./data/zips/${source.id}`;

  // Create ZIP directory if it doesn't exist
  if (!dryRun) {
    await fs.mkdir(zipDir, { recursive: true });
  }

  for (const url of source.zip_urls || []) {
    try {
      const filename = path.basename(new URL(url).pathname);
      const outputPath = path.join(zipDir, filename);

      // Check if file already exists
      if (!dryRun) {
        try {
          await fs.access(outputPath);
          if (verbose) console.log(`  Skipping existing file: ${filename}`);
          results.skipped++;
          continue;
        } catch {
          // File doesn't exist, proceed with download
        }
      }

      if (dryRun) {
        console.log(`  Would download: ${filename} from ${url}`);
        results.downloaded++;
        continue;
      }

      if (verbose) console.log(`  Downloading: ${filename}`);

      await downloadFile(url, outputPath, {
        verbose,
        retryAttempts: config.zip?.retryAttempts || ZIP_CONFIG.retryAttempts
      });

      results.downloaded++;

      if (verbose) {
        console.log(`  ✅ Downloaded: ${filename}`);
      }

      // Add delay between downloads
      const delay = config.zip?.downloadDelayMs || ZIP_CONFIG.downloadDelayMs;
      if (delay > 0) {
        await new Promise(resolve => setTimeout(resolve, delay));
      }

    } catch (error) {
      console.error(`  ❌ Failed to download from ${url}: ${error.message}`);
      results.failed++;
    }
  }

  return results;
}

/**
 * Extract ZIP contents to staging directory
 */
export async function extractZipContents(source, zipDir, stagingDir, options = {}) {
  const { verbose = false, dryRun = false } = options;
  const results = { processedZips: 0, extractedFiles: 0 };

  if (dryRun) {
    console.log(`  Would extract XML files from ${zipDir} to ${stagingDir} (flattened structure)`);
    return results;
  }

  // Create staging directory
  await fs.mkdir(stagingDir, { recursive: true });

  if (verbose) {
    console.log(`  Extracting XML files from ${zipDir} to ${stagingDir}`);
  }

  try {
    // Get list of ZIP files
    const files = await fs.readdir(zipDir);
    const zipFiles = files.filter(file => file.endsWith('.zip'));

    if (verbose) {
      console.log(`    Found ${zipFiles.length} ZIP files to extract`);
    }

    for (const zipFile of zipFiles) {
      const zipPath = path.join(zipDir, zipFile);

      if (verbose) {
        console.log(`  Extracting XML files from: ${zipFile}`);
      }

      const extractResults = await extractXmlFilesFromZip(zipPath, stagingDir, { verbose });
      results.processedZips++;
      results.extractedFiles += extractResults.extractedFiles;
    }

  } catch (error) {
    console.error(`  Error extracting ZIP contents: ${error.message}`);
    throw error;
  }

  return results;
}

/**
 * Download a single file with retry logic
 */
async function downloadFile(url, outputPath, options = {}) {
  const { verbose = false, retryAttempts = 3 } = options;

  await fs.mkdir(path.dirname(outputPath), { recursive: true });

  for (let attempt = 1; attempt <= retryAttempts; attempt++) {
    try {
      if (verbose) console.log(`    Attempt ${attempt}/${retryAttempts}: ${path.basename(outputPath)}`);

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

          response.pipe(file);

          file.on('finish', () => {
            file.close();
            resolve();
          });

          file.on('error', (err) => {
            file.close();
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
 * Extract a single ZIP file
 */
async function extractZip(zipPath, extractTo, options = {}) {
  const { verbose = false } = options;

  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) {
        reject(new Error(`Failed to open ZIP file: ${err.message}`));
        return;
      }

      zipfile.readEntry();

      zipfile.on('entry', (entry) => {
        const entryPath = path.join(extractTo, entry.fileName);

        if (/\/$/.test(entry.fileName)) {
          // Directory entry
          fs.mkdir(entryPath, { recursive: true })
            .then(() => zipfile.readEntry())
            .catch(reject);
          return;
        }

        // File entry
        fs.mkdir(path.dirname(entryPath), { recursive: true })
          .then(() => {
            zipfile.openReadStream(entry, (err, readStream) => {
              if (err) {
                reject(new Error(`Failed to read ZIP entry: ${err.message}`));
                return;
              }

              const writeStream = createWriteStream(entryPath);
              readStream.pipe(writeStream);

              writeStream.on('finish', () => {
                if (verbose) {
                  console.log(`    Extracted: ${entry.fileName}`);
                }
                zipfile.readEntry();
              });

              writeStream.on('error', reject);
            });
          })
          .catch(reject);
      });

      zipfile.on('end', resolve);
      zipfile.on('error', reject);
    });
  });
}

/**
 * Extract only XML files from ZIP to staging directory (flattened structure)
 */
async function extractXmlFilesFromZip(zipPath, stagingDir, options = {}) {
  const { verbose = false } = options;
  const results = { extractedFiles: 0 };

  return new Promise((resolve, reject) => {
    yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
      if (err) {
        reject(new Error(`Failed to open ZIP file: ${err.message}`));
        return;
      }

      let processedEntries = 0;

      zipfile.readEntry();

      zipfile.on('entry', async (entry) => {
        const fileName = entry.fileName;

        // Skip directory entries
        if (/\/$/.test(fileName)) {
          zipfile.readEntry();
          return;
        }

        // Only extract XML files
        if (!fileName.toLowerCase().endsWith('.xml')) {
          // if (verbose) {
          //   console.log(`    Skipping non-XML file: ${fileName}`);
          // }
          zipfile.readEntry();
          return;
        }

        // Create flattened filename (remove directory structure)
        const baseName = path.basename(fileName);
        const targetPath = path.join(stagingDir, baseName);

        // Handle duplicate filenames by adding numeric suffix
        let finalPath = targetPath;
        let counter = 1;
        while (true) {
          try {
            await fs.access(finalPath);
            // File exists, try next number
            const ext = path.extname(baseName);
            const nameWithoutExt = path.basename(baseName, ext);
            finalPath = path.join(stagingDir, `${nameWithoutExt}_${counter}${ext}`);
            counter++;
          } catch {
            // File doesn't exist, use this path
            break;
          }
        }

        zipfile.openReadStream(entry, async (err, readStream) => {
          if (err) {
            reject(new Error(`Failed to read ZIP entry: ${err.message}`));
            return;
          }

          try {
            const writeStream = createWriteStream(finalPath);
            readStream.pipe(writeStream);

            writeStream.on('finish', () => {
              results.extractedFiles++;
              if (verbose) {
                console.log(`    Extracted XML: ${baseName} → ${path.basename(finalPath)}`);
              }
              zipfile.readEntry();
            });

            writeStream.on('error', (writeErr) => {
              reject(new Error(`Failed to write extracted file: ${writeErr.message}`));
            });

          } catch (error) {
            reject(new Error(`Failed to process ZIP entry: ${error.message}`));
          }
        });
      });

      zipfile.on('end', () => {
        resolve(results);
      });

      zipfile.on('error', reject);
    });
  });
}

/**
 * Recursively get all files in a directory
 */
async function getFilesRecursive(dir) {
  const files = [];

  async function scan(currentDir) {
    const items = await fs.readdir(currentDir);

    for (const item of items) {
      const itemPath = path.join(currentDir, item);
      const stat = await fs.stat(itemPath);

      if (stat.isDirectory()) {
        await scan(itemPath);
      } else {
        files.push(itemPath);
      }
    }
  }

  await scan(dir);
  return files;
}
