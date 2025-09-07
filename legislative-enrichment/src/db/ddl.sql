-- Canonical keywords list
CREATE TABLE IF NOT EXISTS keywords (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL UNIQUE,
  keywords_set_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add tier classification for keywords (if not already added by other migrations)
ALTER TABLE keywords 
  ADD COLUMN IF NOT EXISTS tier VARCHAR(50) CHECK (tier IN (
    'field_of_law',
    'doctrine',
    'distinguishing_factor',
    'procedural_posture',
    'case_outcome',
    'major_doctrine',
    'legal_concept'
  ));

-- Indexes for tier-based queries
CREATE INDEX IF NOT EXISTS idx_keywords_tier ON keywords(tier);
CREATE INDEX IF NOT EXISTS idx_keywords_unclassified ON keywords(id) WHERE tier IS NULL;

-- Enrichment results per unit+text version+prompt setup
CREATE TABLE IF NOT EXISTS unit_enrichments (
  id BIGSERIAL PRIMARY KEY,
  unit_id TEXT NOT NULL,
  text_version_id BIGINT,
  law_id TEXT NOT NULL,
  label TEXT,
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  keywords_set_version TEXT NOT NULL,
  digest TEXT NOT NULL,
  json_raw JSONB,
  status TEXT NOT NULL DEFAULT 'succeeded',
  error_message TEXT,
  prompt_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_enrich_unit FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE,
  CONSTRAINT fk_enrich_textver FOREIGN KEY (text_version_id) REFERENCES unit_text_versions(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_unit_enrich
ON unit_enrichments(unit_id, text_version_id, prompt_version, model, keywords_set_version);

CREATE INDEX IF NOT EXISTS idx_unit_enrich_law ON unit_enrichments(law_id);
CREATE INDEX IF NOT EXISTS idx_unit_enrich_status ON unit_enrichments(status);

-- Token usage & cost records
CREATE TABLE IF NOT EXISTS enrichment_usage (
  id BIGSERIAL PRIMARY KEY,
  enrichment_id BIGINT NOT NULL,
  provider TEXT NOT NULL DEFAULT 'openai',
  model TEXT NOT NULL,
  prompt_tokens INT,
  completion_tokens INT,
  total_tokens INT,
  cost_usd NUMERIC(12,6),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_usage_enrichment FOREIGN KEY (enrichment_id)
    REFERENCES unit_enrichments(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_usage_enrichment ON enrichment_usage(enrichment_id);

-- M:N association: unit↔keyword with provenance
CREATE TABLE IF NOT EXISTS unit_keywords (
  unit_id TEXT NOT NULL,
  keyword_id BIGINT NOT NULL,
  enrichment_id BIGINT,
  keywords_set_version TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  model TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (unit_id, keyword_id, prompt_version, keywords_set_version, model),
  CONSTRAINT fk_unit_keywords_unit FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE,
  CONSTRAINT fk_unit_keywords_keyword FOREIGN KEY (keyword_id) REFERENCES keywords(id) ON DELETE CASCADE,
  CONSTRAINT fk_unit_keywords_enrichment FOREIGN KEY (enrichment_id) REFERENCES unit_enrichments(id) ON DELETE SET NULL
);

-- Store full structured taxonomy JSON per enrichment
CREATE TABLE IF NOT EXISTS unit_taxonomy (
  id BIGSERIAL PRIMARY KEY,
  unit_id TEXT NOT NULL,
  enrichment_id BIGINT NOT NULL,
  taxonomy JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_taxonomy_unit FOREIGN KEY (unit_id) REFERENCES units(id) ON DELETE CASCADE,
  CONSTRAINT fk_taxonomy_enrichment FOREIGN KEY (enrichment_id) REFERENCES unit_enrichments(id) ON DELETE CASCADE,
  CONSTRAINT uniq_unit_taxonomy UNIQUE (unit_id, enrichment_id)
);

CREATE INDEX IF NOT EXISTS idx_unit_taxonomy_unit ON unit_taxonomy(unit_id);

-- Helper view to select current section text
CREATE OR REPLACE VIEW current_section_text AS
SELECT
  u.id AS unit_id,
  u.law_id,
  u.label,
  t.id AS text_version_id,
  t.text_plain
FROM units u
JOIN LATERAL (
  SELECT t.*
  FROM unit_text_versions t
  WHERE t.unit_id = u.id
    AND t.effective_start <= CURRENT_DATE
    AND (t.effective_end IS NULL OR t.effective_end > CURRENT_DATE)
  ORDER BY t.effective_start DESC NULLS LAST
  LIMIT 1
) t ON TRUE
WHERE u.unit_type = 'section' AND t.text_plain IS NOT NULL;
