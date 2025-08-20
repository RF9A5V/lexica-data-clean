-- Opinion Overruled Cases Table
-- Stores cases that are explicitly overruled by court opinions
-- Part of unified extraction pipeline

CREATE TABLE IF NOT EXISTS opinion_overruled_cases (
  id SERIAL PRIMARY KEY,
  opinion_id INTEGER NOT NULL REFERENCES opinions(id) ON DELETE CASCADE,
  case_name TEXT NOT NULL,
  citation TEXT,
  scope TEXT NOT NULL CHECK (scope IN ('complete','partial')),
  overruling_language TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Basic lookup/indexes
CREATE INDEX IF NOT EXISTS idx_opinion_overruled_cases_opinion_id ON opinion_overruled_cases (opinion_id);

-- Unique constraint to prevent duplicate overruled cases per opinion
-- Allow same case to be overruled by different opinions, but not duplicated within same opinion
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uniq_opinion_overruled_cases_key'
    ) THEN
        EXECUTE 'ALTER TABLE opinion_overruled_cases ADD CONSTRAINT uniq_opinion_overruled_cases_key UNIQUE (opinion_id, case_name, scope, overruling_language)';
    END IF;
END
$$;

-- Trigram indexes for fuzzy text search on case names and overruling language
-- These require pg_trgm extension (should already be enabled from other migrations)

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_opinion_overruled_cases_case_name_trgm' AND n.nspname = 'public'
    ) THEN
        EXECUTE 'CREATE INDEX idx_opinion_overruled_cases_case_name_trgm ON opinion_overruled_cases USING gin (case_name gin_trgm_ops)';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_opinion_overruled_cases_overruling_language_trgm' AND n.nspname = 'public'
    ) THEN
        EXECUTE 'CREATE INDEX idx_opinion_overruled_cases_overruling_language_trgm ON opinion_overruled_cases USING gin (overruling_language gin_trgm_ops)';
    END IF;
END
$$;

-- Optional: Citation trigram index (if citation is provided)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_opinion_overruled_cases_citation_trgm' AND n.nspname = 'public'
    ) THEN
        EXECUTE 'CREATE INDEX idx_opinion_overruled_cases_citation_trgm ON opinion_overruled_cases USING gin (citation gin_trgm_ops)';
    END IF;
END
$$;
