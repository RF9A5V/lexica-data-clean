-- Additional schema for keyword-based search
-- Add this to your existing case_schema.sql

-- 1. Keywords table (normalized keyword storage)
CREATE TABLE keywords (
    id SERIAL PRIMARY KEY,
    keyword_text TEXT NOT NULL UNIQUE,
    frequency INTEGER DEFAULT 1, -- How often this keyword appears across corpus
    tier TEXT, -- Tier of the keyword
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Opinion-level keywords (for broader case matching)
CREATE TABLE opinion_keywords (
    id SERIAL PRIMARY KEY,
    opinion_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    relevance_score FLOAT NOT NULL, -- LLM-assigned relevance (0.0-1.0)
    extraction_method VARCHAR(50) DEFAULT 'llm_generated', -- 'llm_generated', 'tf_idf', 'manual'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (opinion_id) REFERENCES opinions(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
    
    -- Prevent duplicate keyword assignments per opinion
    UNIQUE (opinion_id, keyword_id)
);

-- 3. Sentence-level keywords (for precise matching)
CREATE TABLE sentence_keywords (
    id SERIAL PRIMARY KEY,
    sentence_id INTEGER NOT NULL,
    keyword_id INTEGER NOT NULL,
    relevance_score FLOAT NOT NULL,
    extraction_method VARCHAR(50) DEFAULT 'llm_generated',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (sentence_id) REFERENCES opinion_sentences(id) ON DELETE CASCADE,
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
    
    -- Prevent duplicate keyword assignments per sentence
    UNIQUE (sentence_id, keyword_id)
);

-- 4. Indexes for efficient keyword searching
CREATE INDEX idx_keywords_text ON keywords(keyword_text);
CREATE INDEX idx_keywords_frequency ON keywords(frequency DESC);

CREATE INDEX idx_opinion_keywords_opinion_id ON opinion_keywords(opinion_id);
CREATE INDEX idx_opinion_keywords_keyword_id ON opinion_keywords(keyword_id);
CREATE INDEX idx_opinion_keywords_relevance ON opinion_keywords(relevance_score DESC);

CREATE INDEX idx_sentence_keywords_sentence_id ON sentence_keywords(sentence_id);
CREATE INDEX idx_sentence_keywords_keyword_id ON sentence_keywords(keyword_id);
CREATE INDEX idx_sentence_keywords_relevance ON sentence_keywords(relevance_score DESC);

-- 5. Views for easy querying
CREATE VIEW opinion_keywords_with_text AS
SELECT 
    ok.opinion_id,
    k.keyword_text,
    ok.relevance_score,
    ok.extraction_method,
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
    fc.category,
    fc.subcategory
FROM sentence_keywords sk
JOIN keywords k ON sk.keyword_id = k.id
JOIN opinion_sentences os ON sk.sentence_id = os.id
JOIN firac_classifications fc ON os.classification_id = fc.id
ORDER BY sk.sentence_id, sk.relevance_score DESC;

-- 6. Function to get or create keyword with tier support
CREATE OR REPLACE FUNCTION get_or_create_keyword(keyword_text TEXT, keyword_tier TEXT DEFAULT NULL)
RETURNS INTEGER AS $$
DECLARE
    keyword_id INTEGER;
BEGIN
    -- Try to get existing keyword
    SELECT id INTO keyword_id FROM keywords WHERE keywords.keyword_text = get_or_create_keyword.keyword_text;
    
    -- If not found, create it with tier
    IF keyword_id IS NULL THEN
        INSERT INTO keywords (keyword_text, tier) VALUES (keyword_text, keyword_tier) RETURNING id INTO keyword_id;
    ELSE
        -- Increment frequency and update tier if provided
        UPDATE keywords SET 
            frequency = frequency + 1,
            tier = COALESCE(keyword_tier, tier)
        WHERE id = keyword_id;
    END IF;
    
    RETURN keyword_id;
END;
$$ LANGUAGE plpgsql;