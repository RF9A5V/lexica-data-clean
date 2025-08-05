-- Rebuild the IVFFlat index on keyword embeddings for optimal performance
-- This should be run after populating embeddings to optimize the index for actual data

-- Drop the existing index (created with lists=100, suboptimal for 95k rows)
DROP INDEX IF EXISTS idx_keywords_embedding;

-- Recreate the IVFFlat index with optimal parameters
-- Using lists = sqrt(row_count) as recommended for IVFFlat
-- With ~95k keywords, sqrt(95000) ≈ 308, so we'll use lists = 300
CREATE INDEX idx_keywords_embedding 
ON keywords 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 300);

-- Add a comment explaining the index
COMMENT ON INDEX idx_keywords_embedding IS 
'IVFFlat index for keyword embedding similarity search, optimized for ~95k keywords with lists=300';

-- Analyze the table to update statistics
ANALYZE keywords;
