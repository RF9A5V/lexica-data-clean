/**
 * Web scraper to find ZIP file URLs from static.case.law
 */

import puppeteer from 'puppeteer';
import fs from 'fs/promises';
import path from 'path';
import { getJurisdiction, getCliArgs, SCRAPER_CONFIG } from './config.js';

/**
 * Scrape ZIP file URLs from a static.case.law page
 */
async function scrapeZipUrls(baseUrl, options = {}) {
  const { verbose = false } = options;
  
  if (verbose) console.log(`Launching browser to scrape: ${baseUrl}`);
  
  const browser = await puppeteer.launch({
    headless: SCRAPER_CONFIG.headless,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });
  
  try {
    const page = await browser.newPage();
    await page.setUserAgent(SCRAPER_CONFIG.userAgent);
    
    if (verbose) console.log('Navigating to page...');
    await page.goto(baseUrl, { 
      waitUntil: 'networkidle2',
      timeout: SCRAPER_CONFIG.timeout 
    });
    
    if (verbose) console.log('Extracting ZIP URLs...');
    
    // Extract all href attributes that end with .zip
    const zipUrls = await page.evaluate((baseUrl) => {
      const links = Array.from(document.querySelectorAll('a[href$=".zip"]'));
      return links.map(link => {
        const href = link.getAttribute('href');
        // Convert relative URLs to absolute URLs
        if (href.startsWith('http')) {
          return href;
        } else if (href.startsWith('/')) {
          const url = new URL(baseUrl);
          return `${url.protocol}//${url.host}${href}`;
        } else {
          return new URL(href, baseUrl).toString();
        }
      }).filter(url => url.endsWith('.zip'));
    }, baseUrl);
    
    if (verbose) {
      console.log(`Found ${zipUrls.length} ZIP files:`);
      zipUrls.forEach((url, index) => {
        console.log(`  ${index + 1}. ${url}`);
      });
    }
    
    return zipUrls;
    
  } finally {
    await browser.close();
  }
}

/**
 * Save URLs to a JSON file
 */
async function saveUrls(urls, jurisdiction, outputPath = null) {
  if (!outputPath) {
    outputPath = `./data/processed/${jurisdiction}-zip-urls.json`;
  }
  
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  
  const data = {
    jurisdiction,
    scrapedAt: new Date().toISOString(),
    count: urls.length,
    urls
  };
  
  await fs.writeFile(outputPath, JSON.stringify(data, null, 2));
  console.log(`Saved ${urls.length} URLs to: ${outputPath}`);
  
  return outputPath;
}

/**
 * Main scraper function
 */
async function main() {
  try {
    const { jurisdiction, verbose, dryRun } = getCliArgs();
    const jurisdictionConfig = getJurisdiction(jurisdiction);
    
    console.log(`Scraping ZIP files for ${jurisdictionConfig.name} (${jurisdiction.toUpperCase()})`);
    console.log(`URL: ${jurisdictionConfig.url}`);
    
    const urls = await scrapeZipUrls(jurisdictionConfig.url, { verbose });
    
    if (urls.length === 0) {
      console.warn('No ZIP files found!');
      return;
    }
    
    if (!dryRun) {
      await saveUrls(urls, jurisdiction);
    } else {
      console.log('Dry run - URLs not saved');
    }
    
    console.log(`✅ Successfully found ${urls.length} ZIP files`);
    
  } catch (error) {
    console.error('❌ Scraping failed:', error.message);
    if (process.env.NODE_ENV === 'development') {
      console.error(error.stack);
    }
    process.exit(1);
  }
}

// Export functions for use in other modules
export { scrapeZipUrls, saveUrls };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
