-- Keyword Descriptors Normalization Script
-- This script normalizes the keyword_descriptors table by creating a separate descriptors table
-- and updating keyword_descriptors to use foreign key references instead of storing duplicate text

-- Step 1: Create the descriptors table
CREATE TABLE IF NOT EXISTS descriptors (
    id SERIAL PRIMARY KEY,
    descriptor_text TEXT NOT NULL UNIQUE,
    embedding vector(768), -- LegalBERT embedding dimension (can be populated later)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for the descriptors table
CREATE INDEX IF NOT EXISTS idx_descriptors_text 
ON descriptors USING gin(to_tsvector('english', descriptor_text));

CREATE INDEX IF NOT EXISTS idx_descriptors_embedding 
ON descriptors USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);

-- Step 2: Create backup of original table
CREATE TABLE IF NOT EXISTS keyword_descriptors_backup AS 
SELECT * FROM keyword_descriptors;

-- Step 3: Populate descriptors table with unique descriptor texts
INSERT INTO descriptors (descriptor_text)
SELECT DISTINCT descriptor_text 
FROM keyword_descriptors
ON CONFLICT (descriptor_text) DO NOTHING;

-- Step 4: Add descriptor_id column to keyword_descriptors
ALTER TABLE keyword_descriptors 
ADD COLUMN IF NOT EXISTS descriptor_id INTEGER;

-- Step 5: Populate descriptor_id by joining with descriptors table
UPDATE keyword_descriptors kd
SET descriptor_id = d.id
FROM descriptors d
WHERE kd.descriptor_text = d.descriptor_text;

-- Step 6: Add foreign key constraint
ALTER TABLE keyword_descriptors 
ADD CONSTRAINT fk_keyword_descriptors_descriptor_id 
FOREIGN KEY (descriptor_id) REFERENCES descriptors(id) ON DELETE CASCADE;

-- Step 7: Create index on descriptor_id
CREATE INDEX IF NOT EXISTS idx_keyword_descriptors_descriptor_id 
ON keyword_descriptors(descriptor_id);

-- Step 8: Verify migration (run these queries to check)
-- SELECT COUNT(*) as total_descriptors FROM descriptors;
-- SELECT COUNT(*) as total_associations FROM keyword_descriptors;
-- SELECT COUNT(*) as valid_associations FROM keyword_descriptors WHERE descriptor_id IS NOT NULL;

-- Step 9: Sample query to verify the join works
-- SELECT k.keyword_text, d.descriptor_text 
-- FROM keyword_descriptors kd
-- JOIN keywords k ON kd.keyword_id = k.id
-- JOIN descriptors d ON kd.descriptor_id = d.id
-- LIMIT 10;

-- Step 10: MANUAL CLEANUP (only run after verifying everything works)
-- -- Drop the old descriptor_text column
-- ALTER TABLE keyword_descriptors DROP COLUMN descriptor_text;
-- 
-- -- Drop the old unique constraint
-- ALTER TABLE keyword_descriptors 
-- DROP CONSTRAINT IF EXISTS keyword_descriptors_keyword_id_descriptor_text_key;
-- 
-- -- Add new unique constraint on keyword_id, descriptor_id
-- ALTER TABLE keyword_descriptors 
-- ADD CONSTRAINT keyword_descriptors_keyword_id_descriptor_id_key 
-- UNIQUE (keyword_id, descriptor_id);
-- 
-- -- Drop backup table (only after everything is verified)
-- DROP TABLE keyword_descriptors_backup;

-- Migration Statistics Query
SELECT 
    'Original keyword_descriptors' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT descriptor_text) as unique_values
FROM keyword_descriptors_backup
UNION ALL
SELECT 
    'New descriptors' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT descriptor_text) as unique_values
FROM descriptors
UNION ALL
SELECT 
    'Updated keyword_descriptors' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT descriptor_id) as unique_values
FROM keyword_descriptors
WHERE descriptor_id IS NOT NULL;
