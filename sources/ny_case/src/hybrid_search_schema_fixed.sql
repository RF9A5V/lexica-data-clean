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
                LEAST((COUNT(DISTINCT op.id)::FLOAT / 10.0 + 0.5), 1.0) as total_relevance,
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
                LEAST((COUNT(DISTINCT op.id)::FLOAT / 10.0 + 0.5), 1.0) as total_relevance,
                keyword_terms as matching_keywords,
                array_length(keyword_terms, 1)::INTEGER as keyword_count,
                COUNT(DISTINCT op.id)::INTEGER as text_matches,
                'text'::TEXT as search_source
            FROM opinion_paragraphs op
            JOIN opinions o ON op.opinion_id = o.id
            JOIN cases c ON o.case_id = c.id
            WHERE include_text_search = true
            GROUP BY op.opinion_id, o.case_id, c.case_name
            HAVING (
                SELECT COUNT(DISTINCT term.term)
                FROM unnest(keyword_terms) as term(term)
                WHERE EXISTS (
                    SELECT 1 FROM opinion_paragraphs op2 
                    WHERE op2.opinion_id = op.opinion_id 
                    AND op2.raw_text ILIKE '%' || term.term || '%'
                )
            ) = array_length(keyword_terms, 1)
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
                    LEAST((COUNT(DISTINCT op.id)::FLOAT / 5.0 + 0.7), 1.0) as total_relevance,
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

-- Create a simpler text-based index for ILIKE searches
-- We'll create this in smaller chunks to avoid the size limit
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_opinion_paragraphs_text_hash
ON opinion_paragraphs USING hash(md5(lower(left(raw_text, 1000))));

COMMENT ON FUNCTION search_opinions_by_keywords_hybrid IS 
'Enhanced search function that combines keyword extraction matches with text-based paragraph searches for comprehensive case discovery';

COMMENT ON FUNCTION search_opinions_by_keywords IS 
'Backward compatible search function that uses hybrid search with text search enabled by default';
