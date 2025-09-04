-- Legislative Database Schema
-- Per-code database schema for hierarchical legislative content

-- Enum types
CREATE TYPE unit_type AS ENUM (
  'title', 'subtitle', 'part', 'subpart', 'chapter', 'subchapter',
  'article', 'subarticle', 'section', 'subsection', 'paragraph',
  'subparagraph', 'clause', 'subclause', 'item', 'subitem',
  'normal_level', 'heading_level'
);

CREATE TYPE citation_target_kind AS ENUM (
  'statute_code', 'regulatory_code', 'charter', 'case', 'external'
);

-- Main units table (hierarchical tree structure)
CREATE TABLE units (
  id BIGSERIAL PRIMARY KEY,
  unit_type unit_type NOT NULL,
  number TEXT,
  label TEXT,
  parent_id BIGINT REFERENCES units(id),
  sort_key TEXT,
  citation TEXT,
  canonical_id TEXT,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Versioned text content with effective dates
CREATE TABLE unit_text_versions (
  id BIGSERIAL PRIMARY KEY,
  unit_id BIGINT NOT NULL REFERENCES units(id),
  effective_start DATE NOT NULL,
  effective_end DATE,
  text_html TEXT,
  text_plain TEXT,
  checksum TEXT,
  version_note TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Citations and cross-references
CREATE TABLE citations (
  id BIGSERIAL PRIMARY KEY,
  source_unit_id BIGINT NOT NULL REFERENCES units(id),
  raw_citation TEXT NOT NULL,
  target_kind citation_target_kind NOT NULL,
  target_unit_id BIGINT REFERENCES units(id),
  external_curie TEXT,
  context TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Search optimization table
CREATE TABLE unit_search (
  unit_id BIGINT PRIMARY KEY REFERENCES units(id),
  heading TEXT,
  text_plain TEXT,
  tsv tsvector,
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_units_parent_id ON units(parent_id);
CREATE INDEX idx_units_type ON units(unit_type);
CREATE INDEX idx_units_canonical_id ON units(canonical_id);
CREATE INDEX idx_units_sort_key ON units(sort_key);

CREATE INDEX idx_unit_text_versions_unit_id ON unit_text_versions(unit_id);
CREATE INDEX idx_unit_text_versions_effective ON unit_text_versions(effective_start, effective_end);

CREATE INDEX idx_citations_source_unit_id ON citations(source_unit_id);
CREATE INDEX idx_citations_target_unit_id ON citations(target_unit_id);
CREATE INDEX idx_citations_external_curie ON citations(external_curie);
CREATE INDEX idx_citations_target_kind ON citations(target_kind);

CREATE INDEX idx_unit_search_tsv ON unit_search USING gin(tsv);

-- Triggers for automatic timestamp updates
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_units_updated_at BEFORE UPDATE ON units
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_unit_search_updated_at BEFORE UPDATE ON unit_search
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

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

CREATE TRIGGER update_unit_search_tsv_trigger BEFORE INSERT OR UPDATE ON unit_search
    FOR EACH ROW EXECUTE FUNCTION update_unit_search_tsv();

-- View for compatibility with global norms system
CREATE VIEW compat_units AS
SELECT 
    id as unit_pk,
    canonical_id,
    citation as display_citation,
    unit_type,
    label,
    number
FROM units
WHERE is_active = TRUE AND canonical_id IS NOT NULL;
