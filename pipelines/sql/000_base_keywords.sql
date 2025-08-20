-- Phase 1 bootstrap for ny_reporter: create minimal keyword tables if absent

-- keywords: globally unique keyword_text; tier added now, constraint added later in 003
CREATE TABLE IF NOT EXISTS keywords (
  id SERIAL PRIMARY KEY,
  keyword_text TEXT NOT NULL UNIQUE,
  tier TEXT NOT NULL,
  frequency INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- opinion_keywords: opinion-level associations with evidence JSON
-- Avoid FK to opinions for portability; FK to keywords is enforced.
CREATE TABLE IF NOT EXISTS opinion_keywords (
  id SERIAL PRIMARY KEY,
  opinion_id INTEGER NOT NULL,
  keyword_id INTEGER NOT NULL REFERENCES keywords(id) ON DELETE CASCADE,
  relevance_score NUMERIC NULL,
  extraction_method TEXT NULL,
  category TEXT NULL,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE (opinion_id, keyword_id)
);

-- helpful indexes
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_keywords_keyword' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_keywords_keyword ON opinion_keywords (keyword_id)';
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_keywords_opinion' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_keywords_opinion ON opinion_keywords (opinion_id)';
  END IF;
END $$;
