-- NYSenate API Extractor Database Schema
-- Adapted from legislative-extractor schema for hierarchical legislative content

-- Enum types
DO $$ BEGIN
  CREATE TYPE unit_type AS ENUM (
    'title', 'subtitle', 'part', 'subpart', 'chapter', 'subchapter',
    'article', 'subarticle', 'section', 'subsection', 'paragraph',
    'subparagraph', 'clause', 'subclause', 'item', 'subitem',
    'subdivision', 'normal_level', 'heading_level', 'other'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE citation_target_kind AS ENUM (
    'statute_code', 'regulatory_code', 'charter', 'case', 'external'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- Code metadata table
CREATE TABLE IF NOT EXISTS code_meta (
  id SMALLINT PRIMARY KEY DEFAULT 1,
  code_key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  publisher TEXT,
  source_uri TEXT,
  current_edition_date DATE,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Main units table (hierarchical tree structure)
CREATE TABLE IF NOT EXISTS units (
  id TEXT PRIMARY KEY,
  unit_type unit_type NOT NULL,
  number TEXT,
  label TEXT,
  parent_id TEXT REFERENCES units(id) ON DELETE CASCADE,
  sort_key TEXT,
  citation TEXT,
  canonical_id TEXT,
  source_id TEXT NOT NULL DEFAULT 'nysenate',
  law_id TEXT,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Versioned text content with effective dates
CREATE TABLE IF NOT EXISTS unit_text_versions (
  id BIGSERIAL PRIMARY KEY,
  unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
  effective_start DATE NOT NULL,
  effective_end DATE,
  source_doc_date DATE,
  published_at TIMESTAMPTZ DEFAULT NOW(),
  text_html TEXT,
  text_plain TEXT,
  checksum TEXT,
  version_note TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  CONSTRAINT chk_effective_range CHECK (effective_end IS NULL OR effective_end > effective_start),
  UNIQUE(unit_id, effective_start)
);

-- Citations and cross-references
CREATE TABLE IF NOT EXISTS citations (
  id BIGSERIAL PRIMARY KEY,
  source_unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
  source_text_version_id BIGINT REFERENCES unit_text_versions(id) ON DELETE SET NULL,
  raw_citation TEXT NOT NULL,
  target_kind citation_target_kind NOT NULL,
  target_unit_id TEXT REFERENCES units(id) ON DELETE SET NULL,
  external_curie TEXT,
  context TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Search optimization table
CREATE TABLE IF NOT EXISTS unit_search (
  unit_id TEXT PRIMARY KEY REFERENCES units(id) ON DELETE CASCADE,
  heading TEXT,
  text_plain TEXT,
  tsv tsvector,
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_units_parent_id ON units(parent_id);
CREATE INDEX IF NOT EXISTS idx_units_type ON units(unit_type);
CREATE INDEX IF NOT EXISTS idx_units_canonical_id ON units(canonical_id);
CREATE INDEX IF NOT EXISTS idx_units_sort_key ON units(sort_key);
CREATE INDEX IF NOT EXISTS idx_units_law_id ON units(law_id);
CREATE INDEX IF NOT EXISTS idx_units_source_id ON units(source_id);

CREATE INDEX IF NOT EXISTS idx_unit_text_versions_unit_id ON unit_text_versions(unit_id);
CREATE INDEX IF NOT EXISTS idx_unit_text_versions_effective ON unit_text_versions(effective_start, effective_end);

CREATE INDEX IF NOT EXISTS idx_citations_source_unit_id ON citations(source_unit_id);
CREATE INDEX IF NOT EXISTS idx_citations_target_unit_id ON citations(target_unit_id);
CREATE INDEX IF NOT EXISTS idx_citations_external_curie ON citations(external_curie);
CREATE INDEX IF NOT EXISTS idx_citations_target_kind ON citations(target_kind);

CREATE INDEX IF NOT EXISTS idx_unit_search_tsv ON unit_search USING gin(tsv);

-- Triggers for automatic timestamp updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_units_updated_at') THEN
    CREATE TRIGGER update_units_updated_at BEFORE UPDATE ON units
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_unit_search_updated_at') THEN
    CREATE TRIGGER update_unit_search_updated_at BEFORE UPDATE ON unit_search
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
  END IF;
END $$;

-- Function to update search vectors
CREATE OR REPLACE FUNCTION update_unit_search_tsv()
RETURNS TRIGGER AS $$
BEGIN
    NEW.tsv = to_tsvector('english', 
        COALESCE(NEW.heading, '') || ' ' || 
        COALESCE(NEW.text_plain, '')
    );
    RETURN NEW;
END;
$$ language 'plpgsql';

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'update_unit_search_tsv_trigger') THEN
    CREATE TRIGGER update_unit_search_tsv_trigger BEFORE INSERT OR UPDATE ON unit_search
        FOR EACH ROW EXECUTE FUNCTION update_unit_search_tsv();
  END IF;
END $$;

-- Compatibility views for co-collection integration
CREATE OR REPLACE VIEW compat_units AS
SELECT 
    id,
    unit_type,
    number,
    label,
    citation,
    sort_key,
    is_active,
    canonical_id,
    law_id
FROM units
WHERE is_active = TRUE;

CREATE OR REPLACE VIEW compat_current_text AS
SELECT u.id AS unit_id, v.text_plain, v.text_html
FROM units u
JOIN LATERAL (
  SELECT *
  FROM unit_text_versions t
  WHERE t.unit_id = u.id
    AND t.effective_start <= CURRENT_DATE
    AND (t.effective_end IS NULL OR t.effective_end > CURRENT_DATE)
  ORDER BY t.effective_start DESC
  LIMIT 1
) v ON true;
