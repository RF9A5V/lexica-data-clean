-- Create keyword validation table
CREATE TABLE keyword_validation (
  id SERIAL PRIMARY KEY,
  field_of_law_keyword_id INTEGER REFERENCES keywords(id) ON DELETE CASCADE,
  doctrine_or_concept_keyword_id INTEGER REFERENCES keywords(id) ON DELETE CASCADE,
  UNIQUE(field_of_law_keyword_id, doctrine_or_concept_keyword_id)
);

-- Create indexes for efficient querying
CREATE INDEX idx_keyword_validation_field ON keyword_validation(field_of_law_keyword_id);
CREATE INDEX idx_keyword_validation_doctrine ON keyword_validation(doctrine_or_concept_keyword_id);
CREATE INDEX idx_keyword_validation_both ON keyword_validation(field_of_law_keyword_id, doctrine_or_concept_keyword_id);

-- Create view for easy validation queries
CREATE VIEW validated_keyword_relationships AS
SELECT 
  k1.keyword AS field_of_law,
  k2.keyword AS doctrine_or_concept,
  k1.id AS field_id,
  k2.id AS doctrine_id
FROM keyword_validation kv
JOIN keywords k1 ON kv.field_of_law_keyword_id = k1.id
JOIN keywords k2 ON kv.doctrine_or_concept_keyword_id = k2.id
WHERE k1.tier = 'field_of_law' 
  AND k2.tier IN ('major_doctrine', 'legal_concept');
