import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { createWriteStream, createReadStream } from 'fs';
import puppeteer from 'puppeteer';
import https from 'https';
import unzipper from 'unzipper';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DOWNLOAD_PAGE_URL = 'https://uscode.house.gov/download/download.shtml';
const OUTPUT_DIR = path.resolve('data/raw');
const ZIP_OUTPUT_PATH = path.join(OUTPUT_DIR, 'usc.zip');

async function getBulkXmlUrlWithPuppeteer() {
  const browser = await puppeteer.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(DOWNLOAD_PAGE_URL, { waitUntil: 'domcontentloaded' });
  // Find the link in the page context
  const url = await page.evaluate(() => {
    const link = document.querySelector('a[title="All USC Titles in XML"]');
    return link ? link.href : null;
  });
  await browser.close();
  if (!url) throw new Error('Could not find XML ZIP link on download page');
  return url;
}

async function downloadZip(url, outputPath) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  return new Promise((resolve, reject) => {
    const file = createWriteStream(outputPath);
    https.get(url, response => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      response.pipe(file);
      file.on('finish', async () => {
        file.close();
        // Validate ZIP file
        try {
          const fd = await fs.open(outputPath, 'r');
          const buffer = Buffer.alloc(4);
          await fd.read(buffer, 0, 4, 0);
          await fd.close();
          const isZip = buffer.slice(0, 2).toString() === 'PK';
          if (!isZip) {
            const first200 = (await fs.readFile(outputPath)).toString('utf8', 0, 200);
            reject(new Error('Downloaded file is not a valid ZIP archive. First 200 bytes: ' + first200));
            return;
          }
          // Extract ZIP to data/xml
          const extractDir = path.resolve('data/xml');
          await fs.mkdir(extractDir, { recursive: true });
          await new Promise((resolveExtract, rejectExtract) => {
            createReadStream(outputPath)
              .pipe(unzipper.Extract({ path: extractDir }))
              .on('close', resolveExtract)
              .on('error', rejectExtract);
          });
          console.log('Extraction completed to', extractDir);
          resolve();
        } catch (err) {
          reject(new Error('Failed to validate ZIP file: ' + err));
        }
      });
    }).on('error', err => {
      fs.unlink(outputPath);
      reject(err);
    });
  });
}

async function main() {
  try {
    console.log('Launching headless browser to fetch download page...');
    const zipUrl = await getBulkXmlUrlWithPuppeteer();
    console.log('Found ZIP URL:', zipUrl);
    console.log(`Downloading ZIP to ${ZIP_OUTPUT_PATH}...`);
    await downloadZip(zipUrl, ZIP_OUTPUT_PATH);
    console.log('Download completed successfully.');
  } catch (err) {
    console.error('Error:', err);
    process.exit(1);
  }
}

main();
