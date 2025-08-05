-- Enhanced hybrid search function that combines keyword matching with text-based paragraph search
-- This provides comprehensive coverage by finding cases through both extracted keywords and direct text matches

-- Drop existing function to replace it
DROP FUNCTION IF EXISTS search_opinions_by_keywords(TEXT[], TEXT, FLOAT, INTEGER);

-- Create enhanced hybrid search function
CREATE OR REPLACE FUNCTION search_opinions_by_keywords_hybrid(
    keyword_terms TEXT[],
    match_strategy TEXT DEFAULT 'any', -- 'any', 'all', 'phrase'
    min_relevance FLOAT DEFAULT 0.5,
    result_limit INTEGER DEFAULT 50,
    include_text_search BOOLEAN DEFAULT true -- Enable/disable text-based search
)
RETURNS TABLE (
    opinion_id INTEGER,
    case_id INTEGER,
    case_name TEXT,
    total_relevance FLOAT,
    matching_keywords TEXT[],
    keyword_count INTEGER,
    text_matches INTEGER,
    search_source TEXT -- 'keyword', 'text', or 'both'
) AS $$
BEGIN
    -- Strategy 1: ANY keyword match (OR logic)
    IF match_strategy = 'any' THEN
        RETURN QUERY
        WITH keyword_matches AS (
            -- Get results from keyword extraction
            SELECT 
                ok.opinion_id,
                o.case_id,
                c.case_name,
                SUM(ok.relevance_score) as total_relevance,
                ARRAY_AGG(DISTINCT k.keyword_text) as matching_keywords,
                COUNT(DISTINCT k.id)::INTEGER as keyword_count,
                0::INTEGER as text_matches,
                'keyword'::TEXT as search_source
            FROM opinion_keywords ok
            JOIN keywords k ON ok.keyword_id = k.id
            JOIN opinions o ON ok.opinion_id = o.id
            JOIN cases c ON o.case_id = c.id
            WHERE k.keyword_text = ANY(keyword_terms)
              AND ok.relevance_score >= min_relevance
            GROUP BY ok.opinion_id, o.case_id, c.case_name
        ),
        text_matches AS (
            -- Get results from text-based paragraph search (if enabled)
            SELECT 
                op.opinion_id,
                o.case_id,
                c.case_name,
                -- Calculate relevance based on text match frequency and position
                (COUNT(DISTINCT op.id)::FLOAT / 10.0 + 0.5) as total_relevance,
                keyword_terms as matching_keywords, -- Return searched terms
                array_length(keyword_terms, 1)::INTEGER as keyword_count,
                COUNT(DISTINCT op.id)::INTEGER as text_matches,
                'text'::TEXT as search_source
            FROM opinion_paragraphs op
            JOIN opinions o ON op.opinion_id = o.id
            JOIN cases c ON o.case_id = c.id
            WHERE include_text_search = true
              AND EXISTS (
                  SELECT 1 FROM unnest(keyword_terms) as term
                  WHERE op.raw_text ILIKE '%' || term || '%'
              )
            GROUP BY op.opinion_id, o.case_id, c.case_name
            HAVING COUNT(DISTINCT op.id) > 0
        ),
        combined_results AS (
            -- Combine and deduplicate results
            SELECT 
                COALESCE(km.opinion_id, tm.opinion_id) as opinion_id,
                COALESCE(km.case_id, tm.case_id) as case_id,
                COALESCE(km.case_name, tm.case_name) as case_name,
                COALESCE(km.total_relevance, 0) + COALESCE(tm.total_relevance, 0) as total_relevance,
                COALESCE(km.matching_keywords, tm.matching_keywords) as matching_keywords,
                COALESCE(km.keyword_count, tm.keyword_count) as keyword_count,
                COALESCE(tm.text_matches, 0) as text_matches,
                CASE 
                    WHEN km.opinion_id IS NOT NULL AND tm.opinion_id IS NOT NULL THEN 'both'
                    WHEN km.opinion_id IS NOT NULL THEN 'keyword'
                    ELSE 'text'
                END as search_source
            FROM keyword_matches km
            FULL OUTER JOIN text_matches tm ON km.opinion_id = tm.opinion_id
        )
        SELECT cr.opinion_id, cr.case_id, cr.case_name, cr.total_relevance, 
               cr.matching_keywords, cr.keyword_count, cr.text_matches, cr.search_source
        FROM combined_results cr
        ORDER BY cr.total_relevance DESC, cr.text_matches DESC
        LIMIT result_limit;
        
    -- Strategy 2: ALL keywords match (AND logic)
    ELSIF match_strategy = 'all' THEN
        RETURN QUERY
        WITH keyword_matches AS (
            SELECT 
                ok.opinion_id,
                o.case_id,
                c.case_name,
                SUM(ok.relevance_score) as total_relevance,
                ARRAY_AGG(DISTINCT k.keyword_text) as matching_keywords,
                COUNT(DISTINCT k.id)::INTEGER as keyword_count,
                0::INTEGER as text_matches,
                'keyword'::TEXT as search_source
            FROM opinion_keywords ok
            JOIN keywords k ON ok.keyword_id = k.id
            JOIN opinions o ON ok.opinion_id = o.id
            JOIN cases c ON o.case_id = c.id
            WHERE k.keyword_text = ANY(keyword_terms)
              AND ok.relevance_score >= min_relevance
            GROUP BY ok.opinion_id, o.case_id, c.case_name
            HAVING COUNT(DISTINCT k.keyword_text) = array_length(keyword_terms, 1)
        ),
        text_matches AS (
            SELECT 
                op.opinion_id,
                o.case_id,
                c.case_name,
                (COUNT(DISTINCT op.id)::FLOAT / 10.0 + 0.5) as total_relevance,
                keyword_terms as matching_keywords,
                array_length(keyword_terms, 1)::INTEGER as keyword_count,
                COUNT(DISTINCT op.id)::INTEGER as text_matches,
                'text'::TEXT as search_source
            FROM opinion_paragraphs op
            JOIN opinions o ON op.opinion_id = o.id
            JOIN cases c ON o.case_id = c.id
            WHERE include_text_search = true
              AND (
                  SELECT COUNT(*)
                  FROM unnest(keyword_terms) as term
                  WHERE op.raw_text ILIKE '%' || term || '%'
              ) = array_length(keyword_terms, 1)
            GROUP BY op.opinion_id, o.case_id, c.case_name
        ),
        combined_results AS (
            SELECT 
                COALESCE(km.opinion_id, tm.opinion_id) as opinion_id,
                COALESCE(km.case_id, tm.case_id) as case_id,
                COALESCE(km.case_name, tm.case_name) as case_name,
                COALESCE(km.total_relevance, 0) + COALESCE(tm.total_relevance, 0) as total_relevance,
                COALESCE(km.matching_keywords, tm.matching_keywords) as matching_keywords,
                COALESCE(km.keyword_count, tm.keyword_count) as keyword_count,
                COALESCE(tm.text_matches, 0) as text_matches,
                CASE 
                    WHEN km.opinion_id IS NOT NULL AND tm.opinion_id IS NOT NULL THEN 'both'
                    WHEN km.opinion_id IS NOT NULL THEN 'keyword'
                    ELSE 'text'
                END as search_source
            FROM keyword_matches km
            FULL OUTER JOIN text_matches tm ON km.opinion_id = tm.opinion_id
        )
        SELECT cr.opinion_id, cr.case_id, cr.case_name, cr.total_relevance, 
               cr.matching_keywords, cr.keyword_count, cr.text_matches, cr.search_source
        FROM combined_results cr
        ORDER BY cr.total_relevance DESC, cr.text_matches DESC
        LIMIT result_limit;
        
    -- Strategy 3: PHRASE search (exact phrase matching)
    ELSIF match_strategy = 'phrase' THEN
        DECLARE
            search_phrase TEXT := array_to_string(keyword_terms, ' ');
        BEGIN
            RETURN QUERY
            WITH text_matches AS (
                SELECT 
                    op.opinion_id,
                    o.case_id,
                    c.case_name,
                    -- Higher relevance for phrase matches
                    (COUNT(DISTINCT op.id)::FLOAT / 5.0 + 0.7) as total_relevance,
                    ARRAY[search_phrase] as matching_keywords,
                    1::INTEGER as keyword_count,
                    COUNT(DISTINCT op.id)::INTEGER as text_matches,
                    'text'::TEXT as search_source
                FROM opinion_paragraphs op
                JOIN opinions o ON op.opinion_id = o.id
                JOIN cases c ON o.case_id = c.id
                WHERE op.raw_text ILIKE '%' || search_phrase || '%'
                GROUP BY op.opinion_id, o.case_id, c.case_name
            )
            SELECT tm.opinion_id, tm.case_id, tm.case_name, tm.total_relevance, 
                   tm.matching_keywords, tm.keyword_count, tm.text_matches, tm.search_source
            FROM text_matches tm
            ORDER BY tm.total_relevance DESC, tm.text_matches DESC
            LIMIT result_limit;
        END;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create backward compatibility function (calls hybrid with text search enabled)
CREATE OR REPLACE FUNCTION search_opinions_by_keywords(
    keyword_terms TEXT[],
    match_strategy TEXT DEFAULT 'any',
    min_relevance FLOAT DEFAULT 0.5,
    result_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    opinion_id INTEGER,
    case_id INTEGER,
    case_name TEXT,
    total_relevance FLOAT,
    matching_keywords TEXT[],
    keyword_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.opinion_id,
        h.case_id,
        h.case_name,
        h.total_relevance,
        h.matching_keywords,
        h.keyword_count
    FROM search_opinions_by_keywords_hybrid(
        keyword_terms, 
        match_strategy, 
        min_relevance, 
        result_limit, 
        true -- Enable text search
    ) h;
END;
$$ LANGUAGE plpgsql;

-- Create index on opinion_paragraphs.raw_text for better text search performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_opinion_paragraphs_text_gin 
ON opinion_paragraphs USING gin(to_tsvector('english', raw_text));

-- Create additional index for ILIKE searches
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_opinion_paragraphs_text_lower
ON opinion_paragraphs (lower(raw_text) text_pattern_ops);

-- Create view for easy access to hybrid search results with metadata
CREATE OR REPLACE VIEW hybrid_search_results AS
SELECT 
    h.*,
    o.binding_type,
    o.date_decided,
    c.citation,
    CASE 
        WHEN h.search_source = 'both' THEN 'Keyword + Text Match'
        WHEN h.search_source = 'keyword' THEN 'Keyword Match Only'
        WHEN h.search_source = 'text' THEN 'Text Match Only'
    END as match_description
FROM search_opinions_by_keywords_hybrid(ARRAY['contract'], 'any', 0.5, 50, true) h
JOIN opinions o ON h.opinion_id = o.id
JOIN cases c ON h.case_id = c.id;

COMMENT ON FUNCTION search_opinions_by_keywords_hybrid IS 
'Enhanced search function that combines keyword extraction matches with text-based paragraph searches for comprehensive case discovery';

COMMENT ON FUNCTION search_opinions_by_keywords IS 
'Backward compatible search function that uses hybrid search with text search enabled by default';

COMMENT ON VIEW hybrid_search_results IS 
'View providing easy access to hybrid search results with additional case metadata and match type descriptions';
