-- Opinion Citations Table
-- Stores citations extracted from opinion text via unified prompt (ci)
-- Minimal fields mapped to verbose columns in this table

CREATE TABLE IF NOT EXISTS opinion_citations (
  id SERIAL PRIMARY KEY,
  opinion_id INTEGER NOT NULL REFERENCES opinions(id) ON DELETE CASCADE,
  -- Raw citation string as it appears in text
  cite_text TEXT,
  -- Case or authority name, when available
  case_name TEXT,
  -- Normalized citation (e.g., formatted reporter cite or canonical form)
  normalized_citation TEXT,
  -- Authority type: case | statute | regulation | constitutional | secondary
  authority_type TEXT CHECK (authority_type IN ('case','statute','regulation','constitutional','secondary')),
  -- Jurisdiction and court level
  jurisdiction TEXT,
  court_level TEXT CHECK (court_level IN ('supreme','appellate','trial','federal_appellate','federal_district')),
  -- Year (if present)
  year INTEGER,
  -- Pincite and textual context/signal
  pincite TEXT,
  citation_context TEXT,
  citation_signal TEXT,
  -- Weight and discussion level per prompt guidance
  precedential_weight TEXT CHECK (precedential_weight IN ('binding','highly_persuasive','persuasive','non_binding')),
  discussion_level TEXT,
  -- If model inferred the legal proposition linked to this citation
  legal_proposition TEXT,
  -- Confidence (0.5 - 1.0) if provided by model
  confidence NUMERIC CHECK (confidence >= 0.5 AND confidence <= 1),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Lookups
CREATE INDEX IF NOT EXISTS idx_opinion_citations_opinion_id ON opinion_citations (opinion_id);

-- Fuzzy search helpers (requires pg_trgm; enabled earlier)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_citations_normalized_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_citations_normalized_trgm ON opinion_citations USING gin (normalized_citation gin_trgm_ops)';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_citations_cite_text_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_citations_cite_text_trgm ON opinion_citations USING gin (cite_text gin_trgm_ops)';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_citations_case_name_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_citations_case_name_trgm ON opinion_citations USING gin (case_name gin_trgm_ops)';
  END IF;
END $$;

-- De-duplication: unique per opinion on (normalized_citation, cite_text, pincite)
-- Use COALESCE to treat NULLs as empty for uniqueness
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'uniq_opinion_citations_key' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE UNIQUE INDEX uniq_opinion_citations_key ON opinion_citations (
      opinion_id,
      COALESCE(normalized_citation, ''''),
      COALESCE(cite_text, ''''),
      COALESCE(pincite, '''')
    )';
  END IF;
END $$;
