/**
 * NYSenate OpenLeg API Test Client
 * Handles fetching and caching of legislative text for testing
 */

const fetch = require('node-fetch');
const fs = require('fs/promises');
const path = require('path');

class NYSenateTestClient {
  constructor(options = {}) {
    this.baseUrl = options.baseUrl || 'https://legislation.nysenate.gov/api/3';
    this.apiKey = options.apiKey || process.env.NYSENATE_API_KEY;
    this.cacheDir = options.cacheDir || path.join(process.cwd(), 'test-data', 'api-cache');
    this.requestDelay = options.requestDelay || 1000; // 1 second between requests
    this.lastRequestTime = 0;
  }

  /**
   * Fetch section data from NYSenate API with caching
   */
  async fetchSection(lawId, sectionNum) {
    const cacheKey = `${lawId.toLowerCase()}-section-${sectionNum}`;
    const cachedData = await this.getCachedData(cacheKey);
    
    if (cachedData) {
      console.log(`Using cached data for ${lawId} § ${sectionNum}`);
      return cachedData;
    }

    console.log(`Fetching ${lawId} § ${sectionNum} from NYSenate API...`);
    
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
      const sectionData = this.extractSectionData(data);
      
      // Cache the result
      await this.cacheData(cacheKey, sectionData);
      
      return sectionData;
    } catch (error) {
      console.error(`Failed to fetch ${lawId} § ${sectionNum}:`, error.message);
      throw error;
    }
  }

  /**
   * Extract relevant section data from API response
   */
  extractSectionData(apiResponse) {
    const section = apiResponse.result || apiResponse;
    
    return {
      lawId: section.lawId,
      sectionNum: section.sectionNum,
      text: section.text || '',
      title: section.title || section.heading || '',
      activeDate: section.activeDate,
      childCount: this.estimateChildCount(section.text || ''),
      rawApiData: section // Keep full API response for debugging
    };
  }

  /**
   * Estimate number of child elements in section text
   */
  estimateChildCount(text) {
    if (!text) return 0;
    
    // Count hierarchical markers
    const subsectionMatches = text.match(/^\s*\d+[a-z-]*\.\s/gm) || [];
    const paragraphMatches = text.match(/^\s*\([a-z]\)\s/gm) || [];
    const subparagraphMatches = text.match(/^\s*\([ivx]+\)\s/gm) || [];
    const clauseMatches = text.match(/^\s*\([A-Z]\)\s/gm) || [];
    const itemMatches = text.match(/^\s*\(\d+\)\s/gm) || [];
    
    return subsectionMatches.length + paragraphMatches.length + 
           subparagraphMatches.length + clauseMatches.length + itemMatches.length;
  }

  /**
   * Get cached data if available and not expired
   */
  async getCachedData(cacheKey) {
    try {
      const cacheFile = path.join(this.cacheDir, `${cacheKey}.json`);
      const stats = await fs.stat(cacheFile);
      
      // Cache expires after 24 hours
      const cacheAge = Date.now() - stats.mtime.getTime();
      if (cacheAge > 24 * 60 * 60 * 1000) {
        return null;
      }
      
      const cached = await fs.readFile(cacheFile, 'utf8');
      return JSON.parse(cached);
    } catch (error) {
      return null; // Cache miss
    }
  }

  /**
   * Cache API response data
   */
  async cacheData(cacheKey, data) {
    try {
      await fs.mkdir(this.cacheDir, { recursive: true });
      const cacheFile = path.join(this.cacheDir, `${cacheKey}.json`);
      await fs.writeFile(cacheFile, JSON.stringify(data, null, 2));
    } catch (error) {
      console.warn('Failed to cache data:', error.message);
    }
  }

  /**
   * Enforce rate limiting between API requests
   */
  async enforceRateLimit() {
    const timeSinceLastRequest = Date.now() - this.lastRequestTime;
    if (timeSinceLastRequest < this.requestDelay) {
      const waitTime = this.requestDelay - timeSinceLastRequest;
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    this.lastRequestTime = Date.now();
  }

  /**
   * Fetch multiple sections for comprehensive testing
   */
  async fetchTestSuite() {
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
    
    for (const testCase of testCases) {
      try {
        console.log(`Fetching test case: ${testCase.description}`);
        const sectionData = await this.fetchSection(testCase.lawId, testCase.sectionNum);
        results.push({
          ...testCase,
          ...sectionData,
          success: true
        });
      } catch (error) {
        console.error(`Failed to fetch ${testCase.lawId} § ${testCase.sectionNum}:`, error.message);
        results.push({
          ...testCase,
          success: false,
          error: error.message
        });
      }
    }

    return results;
  }

  /**
   * Clear all cached data
   */
  async clearCache() {
    try {
      await fs.rm(this.cacheDir, { recursive: true, force: true });
      console.log('Cache cleared successfully');
    } catch (error) {
      console.warn('Failed to clear cache:', error.message);
    }
  }

  /**
   * Get cache statistics
   */
  async getCacheStats() {
    try {
      const files = await fs.readdir(this.cacheDir);
      const stats = await Promise.all(
        files.map(async (file) => {
          const filePath = path.join(this.cacheDir, file);
          const stat = await fs.stat(filePath);
          return {
            file,
            size: stat.size,
            modified: stat.mtime
          };
        })
      );

      return {
        totalFiles: files.length,
        totalSize: stats.reduce((sum, s) => sum + s.size, 0),
        files: stats
      };
    } catch (error) {
      return { totalFiles: 0, totalSize: 0, files: [] };
    }
  }
}

module.exports = { NYSenateTestClient };
