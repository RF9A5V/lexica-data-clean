-- Migration: Add tier column to keywords table
-- Run this first to add the tier classification column

ALTER TABLE keywords 
ADD COLUMN tier VARCHAR(50) CHECK (tier IN (
    'field_of_law', 
    'major_doctrine', 
    'legal_concept', 
    'distinguishing_factor', 
    'procedural_posture', 
    'case_outcome'
));

-- Add index for tier-based queries
CREATE INDEX idx_keywords_tier ON keywords(tier);

-- Add partial index for unclassified keywords (optimization)
CREATE INDEX idx_keywords_unclassified ON keywords(id) WHERE tier IS NULL;
