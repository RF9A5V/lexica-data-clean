-- Phase 1: GIN trigram indexes for fast fuzzy matches (no vectors)

-- Only index doctrine/doctrinal_test keyword texts (partial index)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_keywords_trgm_doctrines' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_keywords_trgm_doctrines ON keywords USING gin (keyword_text gin_trgm_ops) WHERE tier IN (''doctrine'',''doctrinal_test'')';
  END IF;
END $$;
