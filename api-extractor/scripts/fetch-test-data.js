#!/usr/bin/env node
/**
 * Fetch Test Data Script
 * Pre-fetches NYSenate API data for testing without requiring API calls during tests
 */

const fetch = require('node-fetch');
const fs = require('fs/promises');
const path = require('path');

class TestDataFetcher {
  constructor() {
    this.baseUrl = 'https://legislation.nysenate.gov/api/3';
    this.apiKey = process.env.NYSENATE_API_KEY;
    this.outputDir = path.join(__dirname, '..', 'test-data', 'sections');
    this.requestDelay = 1000; // 1 second between requests
    this.lastRequestTime = 0;
  }

  async fetchSection(lawId, sectionNum) {
    console.log(`Fetching ${lawId} § ${sectionNum}...`);
    
    // Rate limiting
    await this.enforceRateLimit();
    
    try {
      const url = `${this.baseUrl}/laws/${lawId}/sections/${sectionNum}`;
      const response = await fetch(url, {
        headers: {
          'X-API-KEY': this.apiKey || '',
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`API request failed: ${response.status} ${response.statusText}`);
      }

      const data = await response.json();
      return this.extractSectionData(data);
    } catch (error) {
      console.error(`Failed to fetch ${lawId} § ${sectionNum}:`, error.message);
      return {
        lawId,
        sectionNum,
        text: '',
        title: '',
        success: false,
        error: error.message
      };
    }
  }

  extractSectionData(apiResponse) {
    const section = apiResponse.result || apiResponse;
    
    return {
      lawId: section.lawId,
      sectionNum: section.sectionNum,
      text: section.text || '',
      title: section.title || section.heading || '',
      activeDate: section.activeDate,
      success: true,
      fetchedAt: new Date().toISOString()
    };
  }

  async enforceRateLimit() {
    const timeSinceLastRequest = Date.now() - this.lastRequestTime;
    if (timeSinceLastRequest < this.requestDelay) {
      const waitTime = this.requestDelay - timeSinceLastRequest;
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    this.lastRequestTime = Date.now();
  }

  async fetchAllTestData() {
    const testCases = [
      { lawId: 'ABC', sectionNum: '3', description: 'Complex definitions section with many subsections' },
      { lawId: 'TAX', sectionNum: '606', description: 'Large section with 400+ child elements' },
      { lawId: 'PEN', sectionNum: '60.35', description: 'Decimal section numbering' },
      { lawId: 'CPL', sectionNum: '240.20', description: 'Criminal procedure with nested hierarchy' },
      { lawId: 'ABC', sectionNum: '12-aaaa', description: 'Special alphanumeric numbering' },
      { lawId: 'EDN', sectionNum: '2575', description: 'Education law with complex structure' },
      { lawId: 'PBH', sectionNum: '3306', description: 'Public health controlled substances' }
    ];

    const results = [];
    
    // Ensure output directory exists
    await fs.mkdir(this.outputDir, { recursive: true });
    
    for (const testCase of testCases) {
      console.log(`\nProcessing: ${testCase.description}`);
      const sectionData = await this.fetchSection(testCase.lawId, testCase.sectionNum);
      
      const result = {
        ...testCase,
        ...sectionData
      };
      
      results.push(result);
      
      // Save individual section file
      const filename = `${testCase.lawId.toLowerCase()}-${testCase.sectionNum.replace(/\./g, '_')}.json`;
      const filepath = path.join(this.outputDir, filename);
      await fs.writeFile(filepath, JSON.stringify(result, null, 2));
      
      console.log(`✓ Saved to ${filename}`);
    }

    // Save combined results file
    const combinedFile = path.join(this.outputDir, 'all-sections.json');
    await fs.writeFile(combinedFile, JSON.stringify(results, null, 2));
    
    console.log(`\n✓ All sections saved to ${combinedFile}`);
    console.log(`✓ Individual files saved to ${this.outputDir}`);
    
    // Generate summary
    const successful = results.filter(r => r.success).length;
    const failed = results.length - successful;
    
    console.log(`\nSummary:`);
    console.log(`  Total sections: ${results.length}`);
    console.log(`  Successful: ${successful}`);
    console.log(`  Failed: ${failed}`);
    
    if (failed > 0) {
      console.log(`\nFailed sections:`);
      results.filter(r => !r.success).forEach(r => {
        console.log(`  - ${r.lawId} § ${r.sectionNum}: ${r.error}`);
      });
    }
    
    return results;
  }
}

// Run the script if called directly
if (require.main === module) {
  const fetcher = new TestDataFetcher();
  
  fetcher.fetchAllTestData()
    .then(() => {
      console.log('\n✅ Test data fetching completed successfully!');
      process.exit(0);
    })
    .catch(error => {
      console.error('\n❌ Test data fetching failed:', error);
      process.exit(1);
    });
}

module.exports = { TestDataFetcher };
