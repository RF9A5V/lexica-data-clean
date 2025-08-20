-- Migration: Enhance opinion_overruled_cases with overruling metadata
-- Adds overruling_type (enum), overruling_court, overruling_case
-- Updates uniqueness to include overruling_type

-- 1) Add new columns if they do not already exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name='opinion_overruled_cases' AND column_name='overruling_type'
  ) THEN
    ALTER TABLE opinion_overruled_cases ADD COLUMN overruling_type TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name='opinion_overruled_cases' AND column_name='overruling_court'
  ) THEN
    ALTER TABLE opinion_overruled_cases ADD COLUMN overruling_court TEXT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name='opinion_overruled_cases' AND column_name='overruling_case'
  ) THEN
    ALTER TABLE opinion_overruled_cases ADD COLUMN overruling_case TEXT;
  END IF;
END
$$;

-- 2) Add/ensure check constraint on overruling_type values
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint 
    WHERE conname = 'chk_ooc_overruling_type'
  ) THEN
    ALTER TABLE opinion_overruled_cases
      ADD CONSTRAINT chk_ooc_overruling_type
      CHECK (overruling_type IN ('direct', 'reported'));
  END IF;
END
$$;

-- 3) Replace the unique constraint to include overruling_type in the key
--    Old: UNIQUE (opinion_id, case_name, scope, overruling_language)
--    New: UNIQUE (opinion_id, case_name, scope, overruling_language, overruling_type)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'uniq_opinion_overruled_cases_key'
  ) THEN
    ALTER TABLE opinion_overruled_cases DROP CONSTRAINT uniq_opinion_overruled_cases_key;
  END IF;

  ALTER TABLE opinion_overruled_cases
    ADD CONSTRAINT uniq_opinion_overruled_cases_key
    UNIQUE (opinion_id, case_name, scope, overruling_language, overruling_type);
END
$$;

-- 4) Helpful indexes for new columns
--    Btree on overruling_type for filtering
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_ooc_overruling_type' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_ooc_overruling_type ON opinion_overruled_cases (overruling_type);
  END IF;
END
$$;

--    Trigram on overruling_court
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_ooc_overruling_court_trgm' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_ooc_overruling_court_trgm ON opinion_overruled_cases USING gin (overruling_court gin_trgm_ops);
  END IF;
END
$$;

--    Trigram on overruling_case
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'idx_ooc_overruling_case_trgm' AND n.nspname = 'public'
  ) THEN
    CREATE INDEX idx_ooc_overruling_case_trgm ON opinion_overruled_cases USING gin (overruling_case gin_trgm_ops);
  END IF;
END
$$;
