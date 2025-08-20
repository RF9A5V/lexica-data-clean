-- Phase 1: Consolidate major_doctrine + legal_concept into doctrine
-- 1) Ensure new allowed tiers include doctrine & doctrinal_test while tolerating legacy labels
--    We cannot reliably drop an unknown check constraint by name; add a new one that is superset-safe.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'keywords_tier_allowed_superset'
  ) THEN
    EXECUTE 'ALTER TABLE keywords
      ADD CONSTRAINT keywords_tier_allowed_superset
      CHECK (tier IN (
        ''field_of_law'',
        ''major_doctrine'',        -- legacy
        ''legal_concept'',         -- legacy
        ''doctrine'',
        ''doctrinal_test'',
        ''distinguishing_factor'',
        ''procedural_posture'',
        ''case_outcome''
      )) NOT VALID';
  END IF;
END $$;

-- 2) Migrate legacy rows to 'doctrine'
UPDATE keywords SET tier = 'doctrine' WHERE tier IN ('major_doctrine','legal_concept');

-- 3) Validate the superset constraint now that data conforms
ALTER TABLE keywords VALIDATE CONSTRAINT keywords_tier_allowed_superset;

-- 4) Helpful indexes (idempotent guards)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_keywords_tier' AND n.nspname = 'public'
  ) THEN
    EXECUTE 'CREATE INDEX idx_keywords_tier ON keywords (tier)';
  END IF;
END $$;
