import { Client } from 'pg';

/**
 * Keyword Search Service for NY Court of Appeals Database
 * Provides search functionality that can be integrated with the multi-source search system
 */
export class KeywordSearchService {
  constructor(connectionString = 'postgresql://localhost/ny_court_of_appeals') {
    this.connectionString = connectionString;
  }

  async connect() {
    this.pg = new Client({ connectionString: this.connectionString });
    await this.pg.connect();
  }

  async disconnect() {
    if (this.pg) {
      await this.pg.end();
    }
  }

  /**
   * Search opinions by keywords with various strategies
   * @param {string[]} keywords - Array of keywords to search for
   * @param {string} strategy - 'any', 'all', or 'phrase'
   * @param {Object} options - Search options
   * @returns {Promise<Array>} Search results
   */
  async searchByKeywords(keywords, strategy = 'any', options = {}) {
    const {
      limit = 50,
      minRelevance = 0.5,
      sortBy = 'relevance', // 'relevance' or 'date'
      categories = null // Filter by keyword categories
    } = options;

    if (!this.pg) {
      await this.connect();
    }

    try {
      let query;
      let params;

      if (strategy === 'phrase') {
        // For phrase search, look for exact phrase in opinion text
        query = this.buildPhraseSearchQuery(keywords.join(' '), limit, minRelevance, sortBy);
        params = [keywords.join(' '), minRelevance, limit];
      } else {
        // Use the database function for 'any' and 'all' strategies
        query = `
          SELECT 
            opinion_id,
            case_id,
            case_name,
            total_relevance,
            matching_keywords,
            keyword_count,
            binding_type,
            date_filed
          FROM search_opinions_by_keywords($1, $2, $3, $4)
          LEFT JOIN opinions o ON o.id = opinion_id
          LEFT JOIN cases c ON c.id = case_id
          ORDER BY ${sortBy === 'date' ? 'c.date_filed DESC NULLS LAST,' : ''} total_relevance DESC
        `;
        params = [keywords, strategy, minRelevance, limit];
      }

      const result = await this.pg.query(query, params);
      
      // Enhance results with additional metadata
      const enhancedResults = await this.enhanceResults(result.rows);
      
      return {
        results: enhancedResults,
        total_found: enhancedResults.length,
        search_metadata: {
          keywords,
          strategy,
          options,
          execution_time: Date.now()
        }
      };

    } catch (error) {
      console.error('Keyword search error:', error);
      throw error;
    }
  }

  buildPhraseSearchQuery(phrase, limit, minRelevance, sortBy) {
    return `
      WITH phrase_matches AS (
        SELECT DISTINCT
          o.id as opinion_id,
          o.case_id,
          c.case_name,
          o.binding_type,
          c.date_filed,
          -- Calculate relevance based on phrase frequency in opinion text
          (
            SELECT COUNT(*)::float * 0.8 -- Base relevance for phrase matches
            FROM opinion_paragraphs op 
            WHERE op.opinion_id = o.id 
              AND op.raw_text ILIKE '%' || $1 || '%'
          ) as total_relevance,
          ARRAY[$1] as matching_keywords,
          1 as keyword_count
        FROM opinions o
        INNER JOIN cases c ON o.case_id = c.id
        INNER JOIN opinion_paragraphs op ON op.opinion_id = o.id
        WHERE op.raw_text ILIKE '%' || $1 || '%'
          AND o.binding_type IN ('015unanimous', '010combined', '020lead')
      )
      SELECT *
      FROM phrase_matches
      WHERE total_relevance >= $2
      ORDER BY ${sortBy === 'date' ? 'date_filed DESC NULLS LAST,' : ''} total_relevance DESC
      LIMIT $3
    `;
  }

  async enhanceResults(results) {
    if (results.length === 0) return results;

    const opinionIds = results.map(r => r.opinion_id);
    
    // Get additional metadata for each opinion
    const metadataQuery = `
      SELECT 
        o.id as opinion_id,
        o.url,
        o.substantial,
        COUNT(DISTINCT ok.keyword_id) as total_keywords,
        ARRAY_AGG(DISTINCT fc.category) FILTER (WHERE fc.category IS NOT NULL) as sentence_categories,
        -- Get top 5 keywords by relevance
        ARRAY_AGG(
          DISTINCT jsonb_build_object(
            'keyword', k.keyword_text,
            'relevance', ok.relevance_score,
            'category', ok.category
          ) ORDER BY ok.relevance_score DESC
        ) FILTER (WHERE k.keyword_text IS NOT NULL) as top_keywords
      FROM opinions o
      LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
      LEFT JOIN keywords k ON ok.keyword_id = k.id
      LEFT JOIN opinion_sentences os ON o.id = os.opinion_id
      LEFT JOIN firac_classifications fc ON os.classification_id = fc.id
      WHERE o.id = ANY($1)
      GROUP BY o.id, o.url, o.substantial
    `;

    const metadata = await this.pg.query(metadataQuery, [opinionIds]);
    const metadataMap = new Map(metadata.rows.map(row => [row.opinion_id, row]));

    // Enhance results with metadata
    return results.map(result => {
      const meta = metadataMap.get(result.opinion_id) || {};
      return {
        ...result,
        url: meta.url,
        substantial: meta.substantial,
        total_keywords: parseInt(meta.total_keywords) || 0,
        sentence_categories: meta.sentence_categories || [],
        top_keywords: (meta.top_keywords || []).slice(0, 5), // Limit to top 5
        // Format for multi-source search compatibility
        source: 'ny_appeals',
        source_name: 'NY Court of Appeals',
        relevance_score: result.total_relevance,
        match_type: 'keyword'
      };
    });
  }

  /**
   * Get keyword suggestions based on partial input
   * @param {string} partial - Partial keyword input
   * @param {number} limit - Maximum suggestions to return
   * @returns {Promise<Array>} Keyword suggestions
   */
  async getKeywordSuggestions(partial, limit = 10) {
    if (!this.pg) {
      await this.connect();
    }

    const query = `
      SELECT 
        k.keyword_text,
        k.frequency,
        COUNT(DISTINCT ok.opinion_id) as opinion_count,
        ARRAY_AGG(DISTINCT ok.category) FILTER (WHERE ok.category IS NOT NULL) as categories
      FROM keywords k
      LEFT JOIN opinion_keywords ok ON k.id = ok.keyword_id
      WHERE k.keyword_text ILIKE $1 || '%'
      GROUP BY k.id, k.keyword_text, k.frequency
      ORDER BY k.frequency DESC, opinion_count DESC
      LIMIT $2
    `;

    const result = await this.pg.query(query, [partial.toLowerCase(), limit]);
    return result.rows;
  }

  /**
   * Get popular keywords by category
   * @param {string} category - Keyword category
   * @param {number} limit - Maximum keywords to return
   * @returns {Promise<Array>} Popular keywords
   */
  async getPopularKeywords(category = null, limit = 20) {
    if (!this.pg) {
      await this.connect();
    }

    let query = `
      SELECT 
        k.keyword_text,
        k.frequency,
        COUNT(DISTINCT ok.opinion_id) as opinion_count,
        AVG(ok.relevance_score) as avg_relevance,
        ok.category
      FROM keywords k
      INNER JOIN opinion_keywords ok ON k.id = ok.keyword_id
    `;

    const params = [limit];
    
    if (category) {
      query += ` WHERE ok.category = $2`;
      params.unshift(category);
    }

    query += `
      GROUP BY k.id, k.keyword_text, k.frequency, ok.category
      ORDER BY opinion_count DESC, k.frequency DESC
      LIMIT $${params.length}
    `;

    const result = await this.pg.query(query, params);
    return result.rows;
  }

  /**
   * Get search statistics
   * @returns {Promise<Object>} Search system statistics
   */
  async getSearchStats() {
    if (!this.pg) {
      await this.connect();
    }

    const statsQuery = `
      SELECT 
        COUNT(DISTINCT o.id) as total_opinions,
        COUNT(DISTINCT ok.opinion_id) as opinions_with_keywords,
        COUNT(DISTINCT k.id) as unique_keywords,
        COUNT(*) as total_keyword_assignments,
        AVG(ok.relevance_score) as avg_relevance_score,
        (
          SELECT jsonb_object_agg(category, count)
          FROM (
            SELECT 
              COALESCE(ok.category, 'uncategorized') as category,
              COUNT(*) as count
            FROM opinion_keywords ok
            GROUP BY ok.category
          ) cat_counts
        ) as keywords_by_category
      FROM opinions o
      LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
      LEFT JOIN keywords k ON ok.keyword_id = k.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
    `;

    const result = await this.pg.query(statsQuery);
    return result.rows[0];
  }
}

// Example usage and testing
async function testKeywordSearch() {
  const service = new KeywordSearchService();
  
  try {
    await service.connect();
    
    console.log('🔍 Testing Keyword Search Service\n');
    
    // Test keyword search
    const searchResults = await service.searchByKeywords(
      ['contract', 'breach'], 
      'any', 
      { limit: 5, sortBy: 'relevance' }
    );
    
    console.log(`Found ${searchResults.total_found} results for "contract" OR "breach"`);
    console.log('Sample results:', searchResults.results.slice(0, 2));
    
    // Test keyword suggestions
    const suggestions = await service.getKeywordSuggestions('neg', 5);
    console.log('\nKeyword suggestions for "neg":', suggestions);
    
    // Test popular keywords
    const popular = await service.getPopularKeywords('legal_doctrines', 5);
    console.log('\nPopular legal doctrine keywords:', popular);
    
    // Test stats
    const stats = await service.getSearchStats();
    console.log('\nSearch system statistics:', stats);
    
  } catch (error) {
    console.error('Test failed:', error);
  } finally {
    await service.disconnect();
  }
}

// Export for use in other modules
export default KeywordSearchService;

// Run test if this file is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  testKeywordSearch().catch(console.error);
}
