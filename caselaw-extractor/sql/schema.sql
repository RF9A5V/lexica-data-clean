-- Case.law Database Schema
-- Separate tables for case details and opinions

-- Main cases table
CREATE TABLE IF NOT EXISTS cases (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    name_abbreviation TEXT,
    decision_date TEXT,
    docket_number TEXT,
    first_page TEXT,
    last_page TEXT,
    file_name TEXT,
    
    -- Court information
    court_name TEXT,
    court_name_abbreviation TEXT,
    court_id INTEGER,
    
    -- Jurisdiction information
    jurisdiction_name TEXT,
    jurisdiction_abbreviation TEXT,
    jurisdiction_id INTEGER,
    
    -- Original case ID from source data (for cross-database citation mapping)
    original_id BIGINT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Citations table (for case citations like "1 N.Y. 17")
CREATE TABLE IF NOT EXISTS citations (
    id SERIAL PRIMARY KEY,
    case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    citation_type TEXT, -- 'official', 'parallel', etc.
    cite TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Case citations table (for cites_to relationships)
CREATE TABLE IF NOT EXISTS case_citations (
    id SERIAL PRIMARY KEY,
    case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    cited_case TEXT NOT NULL, -- The citation string (e.g., "6 John 408")
    category TEXT, -- 'reporters:state', etc.
    reporter TEXT, -- 'Johns.', 'Wend.', etc.
    opinion_index INTEGER,
    pin_cites JSONB, -- Array of specific page references
    cited_case_ids BIGINT[], -- Array of original case IDs that this citation refers to
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Opinions table (separate from cases for normalization)
CREATE TABLE IF NOT EXISTS opinions (
    id SERIAL PRIMARY KEY,
    case_id BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
    opinion_type TEXT, -- 'majority', 'dissenting', 'concurring', etc.
    author TEXT, -- Judge name
    text TEXT, -- Full opinion text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ensure uniqueness for metadata entries
CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata_type_value_unique ON metadata(type, value);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_cases_decision_date ON cases(decision_date);
CREATE INDEX IF NOT EXISTS idx_cases_court_id ON cases(court_id);
CREATE INDEX IF NOT EXISTS idx_cases_jurisdiction_id ON cases(jurisdiction_id);
CREATE INDEX IF NOT EXISTS idx_cases_name ON cases USING gin(to_tsvector('english', name));

CREATE INDEX IF NOT EXISTS idx_citations_case_id ON citations(case_id);
CREATE INDEX IF NOT EXISTS idx_citations_cite ON citations(cite);

CREATE INDEX IF NOT EXISTS idx_case_citations_case_id ON case_citations(case_id);
CREATE INDEX IF NOT EXISTS idx_case_citations_cited_case ON case_citations(cited_case);
CREATE INDEX IF NOT EXISTS idx_case_citations_cited_case_ids ON case_citations USING gin(cited_case_ids);

-- Index for original_id lookups (critical for cross-database citation resolution)
CREATE INDEX IF NOT EXISTS idx_cases_original_id ON cases(original_id);

CREATE INDEX IF NOT EXISTS idx_opinions_case_id ON opinions(case_id);
CREATE INDEX IF NOT EXISTS idx_opinions_author ON opinions(author);
CREATE INDEX IF NOT EXISTS idx_opinions_text ON opinions USING gin(to_tsvector('english', text));

-- Full-text search indexes
CREATE INDEX IF NOT EXISTS idx_cases_fulltext ON cases USING gin(
    to_tsvector('english', coalesce(name, '') || ' ' || coalesce(name_abbreviation, ''))
);

-- Views for common queries
CREATE OR REPLACE VIEW case_summary AS
SELECT 
    c.id,
    c.name,
    c.name_abbreviation,
    c.decision_date,
    c.court_name,
    c.jurisdiction_name,
    array_agg(DISTINCT ct.cite) as citations,
    count(DISTINCT o.id) as opinion_count,
    count(DISTINCT cc.id) as cited_cases_count
FROM cases c
LEFT JOIN citations ct ON c.id = ct.case_id
LEFT JOIN opinions o ON c.id = o.case_id
LEFT JOIN case_citations cc ON c.id = cc.case_id
GROUP BY c.id, c.name, c.name_abbreviation, c.decision_date, c.court_name, c.jurisdiction_name;

-- Sample queries for reference:

-- Find cases by court
-- SELECT * FROM cases WHERE court_name ILIKE '%court of appeals%';

-- Find cases citing a specific case
-- SELECT c.* FROM cases c 
-- JOIN case_citations cc ON c.id = cc.case_id 
-- WHERE cc.cited_case ILIKE '%wend%';

-- Full-text search in case names
-- SELECT * FROM cases WHERE to_tsvector('english', name) @@ plainto_tsquery('english', 'contract');

-- Get case with all related data
-- SELECT c.*, 
--        array_agg(DISTINCT ct.cite) as citations,
--        array_agg(DISTINCT o.author) as opinion_authors
-- FROM cases c
-- LEFT JOIN citations ct ON c.id = ct.case_id
-- LEFT JOIN opinions o ON c.id = o.case_id
-- WHERE c.id = 2004070
-- GROUP BY c.id;
