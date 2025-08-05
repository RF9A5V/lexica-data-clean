-- Add descriptor embedding column to keywords table
-- This enables semantic search across keyword descriptors

-- Check if column exists before adding
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'keywords' 
        AND column_name = 'descriptor_embedding'
    ) THEN
        ALTER TABLE keywords 
        ADD COLUMN descriptor_embedding vector(768);
        
        RAISE NOTICE '✅ Added descriptor_embedding column to keywords table';
    ELSE
        RAISE NOTICE 'ℹ️  descriptor_embedding column already exists';
    END IF;
END $$;

-- Create index for efficient similarity search
CREATE INDEX IF NOT EXISTS idx_keywords_descriptor_embedding 
ON keywords USING ivfflat (descriptor_embedding vector_cosine_ops);

-- Create index for filtering by tier and embedding
CREATE INDEX IF NOT EXISTS idx_keywords_tier_embedding 
ON keywords (tier) WHERE descriptor_embedding IS NOT NULL;

-- Create view for keywords with embeddings and descriptors
CREATE OR REPLACE VIEW keywords_with_embeddings AS
SELECT 
    k.id,
    k.keyword_text,
    k.tier,
    k.descriptor_embedding,
    ARRAY_AGG(kd.descriptor_text) as descriptors,
    COUNT(kd.id) as descriptor_count
FROM keywords k
LEFT JOIN keyword_descriptors kd ON k.id = kd.keyword_id
WHERE k.descriptor_embedding IS NOT NULL
GROUP BY k.id, k.keyword_text, k.tier, k.descriptor_embedding
ORDER BY k.id;

-- Grant permissions (adjust as needed for your setup)
GRANT SELECT ON keywords_with_embeddings TO ny_state_user;

-- Show current status
SELECT 
    COUNT(*) as total_keywords,
    COUNT(descriptor_embedding) as embedded_keywords,
    ROUND(COUNT(descriptor_embedding) * 100.0 / COUNT(*), 2) as embedding_percentage
FROM keywords k
WHERE EXISTS (
    SELECT 1 FROM keyword_descriptors kd 
    WHERE kd.keyword_id = k.id
);
