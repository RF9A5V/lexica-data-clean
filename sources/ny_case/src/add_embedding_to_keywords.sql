-- Add embedding field to keywords table for semantic search
-- Run this against the ny_court_of_appeals database

\c ny_court_of_appeals

-- Add embedding column to keywords table
-- Using vector(768) to match the LegalBERT embedding dimension
ALTER TABLE keywords ADD COLUMN embedding vector(768);

-- Create index on embedding column for efficient similarity searches
CREATE INDEX idx_keywords_embedding ON keywords USING ivfflat (embedding vector_cosine_ops)
WITH (lists = 100);

-- Add a comment to document the embedding field
COMMENT ON COLUMN keywords.embedding IS 'LegalBERT 768-dimensional embedding vector for semantic similarity search';

-- Verify the change
\d keywords;

-- Show current keyword count for reference
SELECT COUNT(*) as total_keywords FROM keywords;
SELECT COUNT(*) as keywords_with_embeddings FROM keywords WHERE embedding IS NOT NULL;
