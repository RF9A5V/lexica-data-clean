import { Client } from 'pg';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

class KeywordSearchEngine {
  constructor() {
    this.pg = null;
  }

  async connect() {
    this.pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
    await this.pg.connect();
  }

  async disconnect() {
    if (this.pg) {
      await this.pg.end();
    }
  }

  /**
   * Search for cases using keywords with various matching strategies
   * @param {string[]} keywords - Array of search keywords
   * @param {Object} options - Search options
   * @returns {Object[]} Array of matching cases with relevance scores
   */
  async searchByKeywords(keywords, options = {}) {
    const {
      matchStrategy = 'any', // 'any', 'all', 'phrase'
      minRelevance = 0.5,
      maxResults = 50,
      includeContext = true,
      firacCategories = null, // Filter by FIRAC categories
      sortBy = 'relevance' // 'relevance', 'citation_count', 'date'
    } = options;

    // Normalize keywords
    const normalizedKeywords = keywords.map(k => k.toLowerCase().trim()).filter(k => k.length > 0);
    
    if (normalizedKeywords.length === 0) {
      return [];
    }

    let query, params;

    switch (matchStrategy) {
      case 'all':
        ({ query, params } = this._buildAllKeywordsQuery(normalizedKeywords, options));
        break;
      case 'phrase':
        ({ query, params } = this._buildPhraseQuery(normalizedKeywords.join(' '), options));
        break;
      case 'any':
      default:
        ({ query, params } = this._buildAnyKeywordsQuery(normalizedKeywords, options));
        break;
    }

    const result = await this.pg.query(query, params);
    return this._processSearchResults(result.rows, options);
  }

  /**
   * Build query for matching ANY of the provided keywords
   */
  _buildAnyKeywordsQuery(keywords, options) {
    const { minRelevance, maxResults, firacCategories } = options;
    
    const keywordPlaceholders = keywords.map((_, i) => `$${i + 1}`).join(',');
    let paramIndex = keywords.length + 1;
    
    let query = `
      WITH keyword_matches AS (
        SELECT 
          o.id as opinion_id,
          c.id as case_id,
          c.case_name,
          c.citation_count,
          c.date_filed,
          k.keyword_text,
          ok.relevance_score,
          -- Calculate match score based on keyword frequency and relevance
          (ok.relevance_score * (1.0 / GREATEST(k.frequency, 1))) as match_score
        FROM opinions o
        JOIN cases c ON o.case_id = c.id
        JOIN opinion_keywords ok ON o.id = ok.opinion_id
        JOIN keywords k ON ok.keyword_id = k.id
        WHERE k.keyword_text = ANY($${keywords.length + 1})
          AND ok.relevance_score >= $${paramIndex++}
    `;
    
    // Add FIRAC category filter if specified
    if (firacCategories && firacCategories.length > 0) {
      const categoryPlaceholders = firacCategories.map((_, i) => `$${paramIndex + i}`).join(',');
      query += `
        AND EXISTS (
          SELECT 1 FROM opinion_sentences os
          JOIN firac_classifications fc ON os.classification_id = fc.id
          WHERE os.opinion_id = o.id
            AND fc.category = ANY(ARRAY[${categoryPlaceholders}])
        )
      `;
      paramIndex += firacCategories.length;
    }
    
    query += `
      ),
      aggregated_results AS (
        SELECT 
          case_id,
          opinion_id,
          case_name,
          citation_count,
          date_filed,
          -- Aggregate match information
          COUNT(DISTINCT keyword_text) as matched_keywords,
          AVG(match_score) as avg_match_score,
          MAX(match_score) as max_match_score,
          SUM(match_score) as total_match_score,
          ARRAY_AGG(DISTINCT keyword_text ORDER BY match_score DESC) as matching_keywords
        FROM keyword_matches
        GROUP BY case_id, opinion_id, case_name, citation_count, date_filed
      )
      SELECT *,
        -- Calculate final relevance score
        (total_match_score * LOG(matched_keywords + 1)) as final_relevance
      FROM aggregated_results
      ORDER BY final_relevance DESC, citation_count DESC
      LIMIT $${paramIndex}
    `;
    
    const params = [
      ...keywords,
      keywords, // For ANY clause
      minRelevance,
      ...(firacCategories || []),
      maxResults
    ];
    
    return { query, params };
  }

  /**
   * Build query for matching ALL of the provided keywords
   */
  _buildAllKeywordsQuery(keywords, options) {
    const { minRelevance, maxResults, firacCategories } = options;
    
    let paramIndex = 1;
    
    let query = `
      WITH keyword_matches AS (
        SELECT 
          o.id as opinion_id,
          c.id as case_id,
          c.case_name,
          c.citation_count,
          c.date_filed,
          k.keyword_text,
          ok.relevance_score
        FROM opinions o
        JOIN cases c ON o.case_id = c.id
        JOIN opinion_keywords ok ON o.id = ok.opinion_id
        JOIN keywords k ON ok.keyword_id = k.id
        WHERE k.keyword_text = ANY($${paramIndex++})
          AND ok.relevance_score >= $${paramIndex++}
    `;
    
    if (firacCategories && firacCategories.length > 0) {
      const categoryPlaceholders = firacCategories.map((_, i) => `$${paramIndex + i}`).join(',');
      query += `
        AND EXISTS (
          SELECT 1 FROM opinion_sentences os
          JOIN firac_classifications fc ON os.classification_id = fc.id
          WHERE os.opinion_id = o.id
            AND fc.category = ANY(ARRAY[${categoryPlaceholders}])
        )
      `;
      paramIndex += firacCategories.length;
    }
    
    query += `
      ),
      opinion_keyword_counts AS (
        SELECT 
          opinion_id,
          case_id,
          case_name,
          citation_count,
          date_filed,
          COUNT(DISTINCT keyword_text) as matched_keywords,
          AVG(relevance_score) as avg_relevance,
          ARRAY_AGG(DISTINCT keyword_text ORDER BY relevance_score DESC) as matching_keywords
        FROM keyword_matches
        GROUP BY opinion_id, case_id, case_name, citation_count, date_filed
        HAVING COUNT(DISTINCT keyword_text) = $${paramIndex++} -- Must match ALL keywords
      )
      SELECT *,
        avg_relevance as final_relevance
      FROM opinion_keyword_counts
      ORDER BY avg_relevance DESC, citation_count DESC
      LIMIT $${paramIndex}
    `;
    
    const params = [
      keywords,
      minRelevance,
      ...(firacCategories || []),
      keywords.length,
      maxResults
    ];
    
    return { query, params };
  }

  /**
   * Build query for phrase matching
   */
  _buildPhraseQuery(phrase, options) {
    const { minRelevance, maxResults } = options;
    
    const query = `
      SELECT 
        o.id as opinion_id,
        c.id as case_id,
        c.case_name,
        c.citation_count,
        c.date_filed,
        k.keyword_text,
        ok.relevance_score as final_relevance,
        1 as matched_keywords,
        ARRAY[k.keyword_text] as matching_keywords
      FROM opinions o
      JOIN cases c ON o.case_id = c.id
      JOIN opinion_keywords ok ON o.id = ok.opinion_id
      JOIN keywords k ON ok.keyword_id = k.id
      WHERE k.keyword_text ILIKE $1
        AND ok.relevance_score >= $2
      ORDER BY ok.relevance_score DESC, c.citation_count DESC
      LIMIT $3
    `;
    
    const params = [`%${phrase}%`, minRelevance, maxResults];
    return { query, params };
  }

  /**
   * Process and enrich search results
   */
  _processSearchResults(rows, options) {
    const { includeContext } = options;
    
    return rows.map(row => ({
      case_id: row.case_id,
      opinion_id: row.opinion_id,
      case_name: row.case_name,
      citation_count: row.citation_count,
      date_filed: row.date_filed,
      relevance_score: parseFloat(row.final_relevance),
      matched_keywords: row.matched_keywords || [row.keyword_text],
      match_summary: {
        total_matched: row.matched_keywords || 1,
        keywords: row.matching_keywords || [row.keyword_text]
      }
    }));
  }

  /**
   * Get related keywords for query expansion
   */
  async getRelatedKeywords(keywords, limit = 10) {
    const normalizedKeywords = keywords.map(k => k.toLowerCase().trim());
    
    const query = `
      WITH base_opinions AS (
        SELECT DISTINCT ok.opinion_id
        FROM opinion_keywords ok
        JOIN keywords k ON ok.keyword_id = k.id
        WHERE k.keyword_text = ANY($1)
      ),
      related_keywords AS (
        SELECT 
          k.keyword_text,
          COUNT(*) as co_occurrence_count,
          AVG(ok.relevance_score) as avg_relevance
        FROM base_opinions bo
        JOIN opinion_keywords ok ON bo.opinion_id = ok.opinion_id
        JOIN keywords k ON ok.keyword_id = k.id
        WHERE k.keyword_text != ALL($1) -- Exclude original keywords
        GROUP BY k.keyword_text
        HAVING COUNT(*) >= 2 -- Must appear with original keywords at least twice
      )
      SELECT 
        keyword_text,
        co_occurrence_count,
        avg_relevance,
        (co_occurrence_count * avg_relevance) as relevance_score
      FROM related_keywords
      ORDER BY relevance_score DESC
      LIMIT $2
    `;
    
    const result = await this.pg.query(query, [normalizedKeywords, limit]);
    return result.rows;
  }

  /**
   * Get keyword statistics for analysis
   */
  async getKeywordStats(keywords) {
    const normalizedKeywords = keywords.map(k => k.toLowerCase().trim());
    
    const query = `
      SELECT 
        k.keyword_text,
        k.frequency,
        COUNT(ok.opinion_id) as opinion_count,
        AVG(ok.relevance_score) as avg_relevance,
        MIN(ok.relevance_score) as min_relevance,
        MAX(ok.relevance_score) as max_relevance
      FROM keywords k
      LEFT JOIN opinion_keywords ok ON k.id = ok.keyword_id
      WHERE k.keyword_text = ANY($1)
      GROUP BY k.id, k.keyword_text, k.frequency
      ORDER BY k.frequency DESC
    `;
    
    const result = await this.pg.query(query, [normalizedKeywords]);
    return result.rows;
  }
}

// Example usage and testing
async function testKeywordSearch() {
  const searchEngine = new KeywordSearchEngine();
  await searchEngine.connect();
  
  try {
    // Test different search strategies
    console.log('=== Testing Keyword Search ===\n');
    
    // Test 1: Any keyword match
    console.log('1. Searching for ANY of: negligence, liability, damages');
    const anyResults = await searchEngine.searchByKeywords(
      ['negligence', 'liability', 'damages'],
      { matchStrategy: 'any', maxResults: 5 }
    );
    console.log(`Found ${anyResults.length} cases`);
    anyResults.forEach((result, i) => {
      console.log(`  ${i+1}. ${result.case_name} (relevance: ${result.relevance_score.toFixed(3)})`);
      console.log(`     Keywords: ${result.match_summary.keywords.join(', ')}`);
    });
    
    console.log('\n2. Searching for ALL of: contract, breach');
    const allResults = await searchEngine.searchByKeywords(
      ['contract', 'breach'],
      { matchStrategy: 'all', maxResults: 5 }
    );
    console.log(`Found ${allResults.length} cases`);
    
    // Test 3: Get related keywords
    console.log('\n3. Related keywords for "negligence":');
    const related = await searchEngine.getRelatedKeywords(['negligence'], 5);
    related.forEach(kw => {
      console.log(`  ${kw.keyword_text} (co-occurrence: ${kw.co_occurrence_count}, relevance: ${parseFloat(kw.avg_relevance).toFixed(3)})`);
    });
    
    // Test 4: Keyword statistics
    console.log('\n4. Keyword statistics:');
    const stats = await searchEngine.getKeywordStats(['negligence', 'contract', 'liability']);
    stats.forEach(stat => {
      console.log(`  ${stat.keyword_text}: ${stat.opinion_count} opinions, avg relevance: ${parseFloat(stat.avg_relevance || 0).toFixed(3)}`);
    });
    
  } finally {
    await searchEngine.disconnect();
  }
}

// Run test if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  testKeywordSearch().catch(console.error);
}

export { KeywordSearchEngine };