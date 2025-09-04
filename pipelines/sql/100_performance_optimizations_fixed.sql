-- Performance Optimization Migration (Fixed for Actual Schema)
-- Generated: 2025-01-21
-- Purpose: Add indexes and materialized views for co-collection performance

-- ============================================================================
-- COMPOSITE INDEXES FOR FREQUENT JOIN PATTERNS
-- ============================================================================

-- Composite index for opinion_keywords lookups (most frequent join)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_opinion_keywords_composite 
ON opinion_keywords(opinion_id, keyword_id);

-- Index for case_citations lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_case_citations_case_id 
ON case_citations(case_id);

-- Composite index for cases date filtering with court
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cases_decision_date 
ON cases(decision_date DESC NULLS LAST);

-- Index for keyword text lookups with tier filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_keywords_text_tier 
ON keywords(keyword_text, tier);

-- Index for citations by case_id
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_citations_case_id 
ON citations(case_id);

-- ============================================================================
-- TEXT SEARCH OPTIMIZATION
-- ============================================================================

-- Add GIN indexes for text search on opinions
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_opinions_text_gin 
ON opinions USING gin(to_tsvector('english', text));

-- Index for keyword text pattern matching
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_keywords_text_prefix 
ON keywords(keyword_text text_pattern_ops);

-- ============================================================================
-- MATERIALIZED VIEWS FOR EXPENSIVE AGGREGATIONS
-- ============================================================================

-- Drop existing materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS keyword_cooccurrence_cache CASCADE;
DROP MATERIALIZED VIEW IF EXISTS case_citation_counts CASCADE;

-- Materialized view for keyword co-occurrences
CREATE MATERIALIZED VIEW keyword_cooccurrence_cache AS
WITH keyword_pairs AS (
    SELECT 
        ok1.keyword_id AS keyword1_id,
        ok2.keyword_id AS keyword2_id,
        COUNT(DISTINCT o.case_id) AS cooccurrence_count,
        ARRAY_AGG(DISTINCT o.case_id ORDER BY o.case_id) AS case_ids
    FROM opinions o
    JOIN opinion_keywords ok1 ON o.id = ok1.opinion_id
    JOIN opinion_keywords ok2 ON o.id = ok2.opinion_id
    WHERE ok1.keyword_id < ok2.keyword_id  -- Avoid duplicates and self-pairs
    GROUP BY ok1.keyword_id, ok2.keyword_id
    HAVING COUNT(DISTINCT o.case_id) > 1  -- Only pairs that appear together
)
SELECT 
    kp.keyword1_id,
    k1.keyword_text AS keyword1_text,
    k1.tier AS keyword1_tier,
    kp.keyword2_id,
    k2.keyword_text AS keyword2_text,
    k2.tier AS keyword2_tier,
    kp.cooccurrence_count,
    kp.case_ids,
    NOW() AS last_refreshed
FROM keyword_pairs kp
JOIN keywords k1 ON kp.keyword1_id = k1.id
JOIN keywords k2 ON kp.keyword2_id = k2.id
WHERE k1.tier != 'procedural_posture' 
  AND k2.tier != 'procedural_posture';

-- Create indexes on materialized view
CREATE INDEX idx_keyword_cooccurrence_keyword1 ON keyword_cooccurrence_cache(keyword1_id);
CREATE INDEX idx_keyword_cooccurrence_keyword2 ON keyword_cooccurrence_cache(keyword2_id);
CREATE INDEX idx_keyword_cooccurrence_both ON keyword_cooccurrence_cache(keyword1_id, keyword2_id);

-- Materialized view for case citation counts
CREATE MATERIALIZED VIEW case_citation_counts AS
SELECT 
    c.id AS case_id,
    COUNT(DISTINCT cc.id) AS citation_count,
    NOW() AS last_refreshed
FROM cases c
LEFT JOIN case_citations cc ON c.id = cc.case_id
GROUP BY c.id;

-- Create index on materialized view
CREATE UNIQUE INDEX idx_case_citation_counts_case_id ON case_citation_counts(case_id);

-- ============================================================================
-- PERFORMANCE MONITORING TABLE
-- ============================================================================

-- Create table for query performance logging if it doesn't exist
CREATE TABLE IF NOT EXISTS query_performance_log (
    id SERIAL PRIMARY KEY,
    query_hash TEXT NOT NULL,
    query_text TEXT,
    source_id TEXT,
    endpoint TEXT,
    execution_time_ms INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance log
CREATE INDEX IF NOT EXISTS idx_query_performance_created_at 
ON query_performance_log(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_query_performance_endpoint 
ON query_performance_log(endpoint, execution_time_ms);

-- ============================================================================
-- STATISTICS UPDATE
-- ============================================================================

-- Update table statistics for query planner
ANALYZE cases;
ANALYZE opinions;
ANALYZE keywords;
ANALYZE opinion_keywords;
ANALYZE citations;
ANALYZE case_citations;

-- ============================================================================
-- OPTIONAL: Add citation_count column to cases table for even faster access
-- ============================================================================

-- Check if column exists before adding
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'cases' 
        AND column_name = 'citation_count'
    ) THEN
        ALTER TABLE cases ADD COLUMN citation_count INTEGER DEFAULT 0;
        
        -- Populate the column from case_citations
        UPDATE cases c
        SET citation_count = (
            SELECT COUNT(*)
            FROM case_citations cc
            WHERE cc.case_id = c.id
        );
        
        -- Create index on the new column
        CREATE INDEX idx_cases_citation_count ON cases(citation_count DESC NULLS LAST);
    END IF;
END $$;

-- ============================================================================
-- REFRESH MATERIALIZED VIEWS
-- ============================================================================

-- Initial refresh of materialized views
REFRESH MATERIALIZED VIEW keyword_cooccurrence_cache;
REFRESH MATERIALIZED VIEW case_citation_counts;
