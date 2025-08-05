-- Schema for keyword descriptors
-- Add this to your existing keyword schema

-- 1. Descriptors table for plain language descriptions
CREATE TABLE keyword_descriptors (
    id SERIAL PRIMARY KEY,
    keyword_id INTEGER NOT NULL,
    descriptor_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraint
    FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
    
    -- Prevent duplicate descriptors per keyword
    UNIQUE (keyword_id, descriptor_text)
);

-- 2. Index for efficient descriptor lookup
CREATE INDEX idx_keyword_descriptors_keyword_id ON keyword_descriptors(keyword_id);
CREATE INDEX idx_keyword_descriptors_text ON keyword_descriptors USING gin(to_tsvector('english', descriptor_text));

-- 3. View for keywords with their descriptors
CREATE VIEW keywords_with_descriptors AS
SELECT 
    k.id as keyword_id,
    k.keyword_text,
    k.tier,
    ARRAY_AGG(kd.descriptor_text) as descriptors
FROM keywords k
LEFT JOIN keyword_descriptors kd ON k.id = kd.keyword_id
WHERE k.tier IN ('major_doctrine', 'legal_concept')
GROUP BY k.id, k.keyword_text, k.tier;