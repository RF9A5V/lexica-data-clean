-- nys_legislative schema. Parallel to rcny_legislative (units / unit_text_versions /
-- citations / code_meta / unit_search) plus NYS-specific bits:
--   - units.repealed_date / law_id / law_type / published_dates
--   - unit_aliases (one row per kebab CURIE form that matches the case-side regex)
-- Idempotent — safe to re-run.

DO $$ BEGIN
  CREATE TYPE unit_type AS ENUM (
    'title','subtitle','chapter','subchapter','article','subarticle',
    'part','subpart','section','subsection','paragraph','subparagraph',
    'clause','subclause','item','subitem','appendix','normal_level',
    'heading_level','rule','other'
  );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
  CREATE TYPE citation_target_kind AS ENUM ('statute_section','reg_section','case','unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS code_meta (
  id SMALLINT PRIMARY KEY DEFAULT 1,
  code_key TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  publisher TEXT,
  source_uri TEXT,
  current_edition_date DATE
);

CREATE TABLE IF NOT EXISTS units (
  id TEXT PRIMARY KEY,
  unit_type unit_type NOT NULL,
  number TEXT,
  label TEXT,
  parent_id TEXT REFERENCES units(id) ON DELETE CASCADE,
  sort_key TEXT,
  citation TEXT,
  canonical_id TEXT,
  source_id TEXT NOT NULL DEFAULT 'nys',
  law_id TEXT,
  law_type TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  repealed_date DATE,
  published_dates TEXT[],
  active_date DATE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

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
  CONSTRAINT chk_effective_range CHECK (effective_end IS NULL OR effective_end > effective_start)
);

CREATE TABLE IF NOT EXISTS citations (
  id BIGSERIAL PRIMARY KEY,
  source_unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
  source_text_version_id BIGINT REFERENCES unit_text_versions(id) ON DELETE SET NULL,
  raw_citation TEXT NOT NULL,
  target_kind citation_target_kind NOT NULL,
  target_unit_id TEXT REFERENCES units(id) ON DELETE SET NULL,
  external_curie TEXT,
  context_snippet TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- One row per (alias_curie, unit) — extra forms beyond units.canonical_id.
-- Resolver and seeder UNION across canonical_id and alias to match
-- everything the case-side regex emits.
CREATE TABLE IF NOT EXISTS unit_aliases (
  alias TEXT NOT NULL,
  unit_id TEXT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
  PRIMARY KEY (alias, unit_id)
);

CREATE TABLE IF NOT EXISTS unit_search (
  unit_id TEXT PRIMARY KEY REFERENCES units(id) ON DELETE CASCADE,
  heading TEXT,
  text_plain TEXT,
  tsv tsvector,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS units_parent_idx ON units(parent_id);
CREATE INDEX IF NOT EXISTS units_source_idx ON units(source_id);
CREATE INDEX IF NOT EXISTS units_canonical_id_idx ON units(canonical_id) WHERE canonical_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS units_law_id_idx ON units(law_id);
CREATE INDEX IF NOT EXISTS units_repealed_idx ON units(repealed_date) WHERE repealed_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS unit_aliases_unit_idx ON unit_aliases(unit_id);
CREATE INDEX IF NOT EXISTS utv_unit_idx ON unit_text_versions(unit_id, effective_start, effective_end);
CREATE INDEX IF NOT EXISTS citations_src_idx ON citations(source_unit_id);
CREATE INDEX IF NOT EXISTS citations_external_curie_idx ON citations(external_curie) WHERE external_curie IS NOT NULL;
CREATE INDEX IF NOT EXISTS unit_search_tsv_idx ON unit_search USING GIN(tsv);
