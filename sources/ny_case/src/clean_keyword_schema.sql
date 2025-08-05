-- Clean keyword schema for NY Court of Appeals database
-- Drops existing keywords table and creates new structure for opinion-level keyword extraction

-- 1. Drop existing keyword tables
DROP TABLE IF EXISTS paragraph_keywords CASCADE;
DROP TABLE IF EXISTS keywords CASCADE;

-- 2. Create new keywords table (normalized keyword storage)
CREATE TABLE keywords (
    id SERIAL PRIMARY KEY,
    keyword_text TEXT NOT NULL UNIQUE,
    frequency INTEGER DEFAULT 1, -- How often this keyword appears across corpus
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Create opinion-level keywords table (for broader case matching)
CREATE TABLE opinion_keywords (
    id SERIAL PRIMARY KEY,
    opinion_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    relevance_score FLOAT NOT NULL, -- LLM-assigned relevance (0.0-1.0)
    extraction_method VARCHAR(50) DEFAULT 'llm_generated', -- 'llm_generated', 'tf_idf', 'manual'
    category VARCHAR(50), -- 'legal_doctrines', 'causes_of_action', etc.
    context TEXT, -- Brief context from LLM extraction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (opinion_id) REFERENCES opinions(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
    
    -- Prevent duplicate keyword assignments per opinion
    UNIQUE (opinion_id, keyword_id)
);

-- 4. Create sentence-level keywords table (for precise matching)
CREATE TABLE sentence_keywords (
    id SERIAL PRIMARY KEY,
    sentence_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    relevance_score FLOAT NOT NULL,
    extraction_method VARCHAR(50) DEFAULT 'llm_generated',
    category VARCHAR(50),
    context TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (sentence_id) REFERENCES opinion_sentences(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
    
    -- Prevent duplicate keyword assignments per sentence
    UNIQUE (sentence_id, keyword_id)
);

-- 5. Create indexes for efficient keyword searching
CREATE INDEX idx_keywords_text ON keywords(keyword_text);
CREATE INDEX idx_keywords_frequency ON keywords(frequency DESC);

CREATE INDEX idx_opinion_keywords_opinion_id ON opinion_keywords(opinion_id);
CREATE INDEX idx_opinion_keywords_keyword_id ON opinion_keywords(keyword_id);
CREATE INDEX idx_opinion_keywords_relevance ON opinion_keywords(relevance_score DESC);
CREATE INDEX idx_opinion_keywords_category ON opinion_keywords(category);

CREATE INDEX idx_sentence_keywords_sentence_id ON sentence_keywords(sentence_id);
CREATE INDEX idx_sentence_keywords_keyword_id ON sentence_keywords(keyword_id);
CREATE INDEX idx_sentence_keywords_relevance ON sentence_keywords(relevance_score DESC);
CREATE INDEX idx_sentence_keywords_category ON sentence_keywords(category);

-- 6. Create views for easy querying
CREATE VIEW opinion_keywords_with_text AS
SELECT 
    ok.opinion_id,
    k.keyword_text,
    ok.relevance_score,
    ok.extraction_method,
    ok.category,
    ok.context,
    ok.created_at
FROM opinion_keywords ok
JOIN keywords k ON ok.keyword_id = k.id
ORDER BY ok.opinion_id, ok.relevance_score DESC;

CREATE VIEW sentence_keywords_with_text AS
SELECT 
    sk.sentence_id,
    os.opinion_id,
    k.keyword_text,
    sk.relevance_score,
    sk.extraction_method,
    sk.category,
    sk.context,
    fc.category as sentence_category,
    fc.subcategory as sentence_subcategory
FROM sentence_keywords sk
JOIN keywords k ON sk.keyword_id = k.id
JOIN opinion_sentences os ON sk.sentence_id = os.id
JOIN firac_classifications fc ON os.classification_id = fc.id
ORDER BY sk.sentence_id, sk.relevance_score DESC;

-- 7. Function to get or create keyword
CREATE OR REPLACE FUNCTION get_or_create_keyword(keyword_text TEXT)
RETURNS INTEGER AS $$
DECLARE
    keyword_id INTEGER;
BEGIN
    -- Try to get existing keyword
    SELECT id INTO keyword_id FROM keywords WHERE keywords.keyword_text = get_or_create_keyword.keyword_text;
    
    -- If not found, create it
    IF keyword_id IS NULL THEN
        INSERT INTO keywords (keyword_text) VALUES (keyword_text) RETURNING id INTO keyword_id;
    ELSE
        -- Increment frequency
        UPDATE keywords SET frequency = frequency + 1 WHERE id = keyword_id;
    END IF;
    
    RETURN keyword_id;
END;
$$ LANGUAGE plpgsql;

-- 8. Function to search opinions by keywords
CREATE OR REPLACE FUNCTION search_opinions_by_keywords(
    keyword_terms TEXT[],
    match_strategy TEXT DEFAULT 'any', -- 'any', 'all', 'phrase'
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
    IF match_strategy = 'any' THEN
        RETURN QUERY
        SELECT 
            ok.opinion_id,
            o.case_id,
            c.case_name,
            SUM(ok.relevance_score) as total_relevance,
            ARRAY_AGG(DISTINCT k.keyword_text) as matching_keywords,
            COUNT(DISTINCT k.id)::INTEGER as keyword_count
        FROM opinion_keywords ok
        JOIN keywords k ON ok.keyword_id = k.id
        JOIN opinions o ON ok.opinion_id = o.id
        JOIN cases c ON o.case_id = c.id
        WHERE k.keyword_text = ANY(keyword_terms)
          AND ok.relevance_score >= min_relevance
        GROUP BY ok.opinion_id, o.case_id, c.case_name
        ORDER BY total_relevance DESC, keyword_count DESC
        LIMIT result_limit;
        
    ELSIF match_strategy = 'all' THEN
        RETURN QUERY
        SELECT 
            ok.opinion_id,
            o.case_id,
            c.case_name,
            SUM(ok.relevance_score) as total_relevance,
            ARRAY_AGG(DISTINCT k.keyword_text) as matching_keywords,
            COUNT(DISTINCT k.id)::INTEGER as keyword_count
        FROM opinion_keywords ok
        JOIN keywords k ON ok.keyword_id = k.id
        JOIN opinions o ON ok.opinion_id = o.id
        JOIN cases c ON o.case_id = c.id
        WHERE k.keyword_text = ANY(keyword_terms)
          AND ok.relevance_score >= min_relevance
        GROUP BY ok.opinion_id, o.case_id, c.case_name
        HAVING COUNT(DISTINCT k.keyword_text) = array_length(keyword_terms, 1)
        ORDER BY total_relevance DESC, keyword_count DESC
        LIMIT result_limit;
    END IF;
END;
$$ LANGUAGE plpgsql;
