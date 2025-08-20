-- Phase: Opinion Holdings storage

-- Stores extracted legal holdings per opinion based on unified prompt minimal schema (h)
-- Minimal fields: is (issue), ho (holding), ru (rule), re (reasoning), pv (precedential_value), cf (confidence)

CREATE TABLE IF NOT EXISTS opinion_holdings (
  id SERIAL PRIMARY KEY,
  opinion_id INTEGER NOT NULL REFERENCES opinions(id) ON DELETE CASCADE,
  issue TEXT NOT NULL,
  holding TEXT NOT NULL,
  rule TEXT NOT NULL,
  reasoning TEXT NOT NULL,
  precedential_value TEXT NOT NULL CHECK (precedential_value IN ('high','medium','low')),
  confidence NUMERIC NOT NULL CHECK (confidence >= 0.5 AND confidence <= 1),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Basic lookup/indexes
CREATE INDEX IF NOT EXISTS idx_opinion_holdings_opinion_id ON opinion_holdings (opinion_id);

-- Ensure uniqueness per opinion on (issue, holding, rule)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'uniq_opinion_holdings_key'
  ) THEN
    EXECUTE 'ALTER TABLE opinion_holdings ADD CONSTRAINT uniq_opinion_holdings_key UNIQUE (opinion_id, issue, holding, rule)';
  END IF;
END $$;

-- Trigram indexes for fast fuzzy search (requires pg_trgm; enabled by 001_enable_pgtrgm.sql)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_holdings_issue_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_holdings_issue_trgm ON opinion_holdings USING gin (issue gin_trgm_ops)';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_holdings_holding_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_holdings_holding_trgm ON opinion_holdings USING gin (holding gin_trgm_ops)';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_holdings_rule_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_holdings_rule_trgm ON opinion_holdings USING gin (rule gin_trgm_ops)';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_opinion_holdings_reasoning_trgm' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_opinion_holdings_reasoning_trgm ON opinion_holdings USING gin (reasoning gin_trgm_ops)';
  END IF;
END $$;
