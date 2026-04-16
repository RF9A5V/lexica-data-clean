-- Migration: Add opinion_curie to analysis tables for reimport-safe decoupling.
--
-- Adds a TEXT column alongside the existing opinion_id INTEGER FK. The
-- dual-key phase lets both paths coexist: old code writes opinion_id,
-- new code writes both. A later migration (095) will enforce NOT NULL
-- on opinion_curie and drop opinion_id once all analysis rows are
-- backfilled and the importer is emitting curies.
--
-- Requires migration 093 (cases.curie + opinions.curie) to have been applied
-- and CURIEs backfilled on the opinions table before this backfill runs.

-- Helper: add column + index + backfill for one table (idempotent).
-- Each DO block checks column existence first to survive re-runs.

-- ==================== opinion_holdings ====================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_holdings') THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_holdings' AND column_name='opinion_curie'
    ) THEN
      ALTER TABLE opinion_holdings ADD COLUMN opinion_curie TEXT;
    END IF;
  END IF;
END $$;

UPDATE opinion_holdings h
   SET opinion_curie = o.curie
  FROM opinions o
 WHERE o.id = h.opinion_id
   AND h.opinion_curie IS NULL
   AND o.curie IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_opinion_holdings_curie
  ON opinion_holdings (opinion_curie)
  WHERE opinion_curie IS NOT NULL;

-- ==================== opinion_keywords ====================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_keywords') THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_keywords' AND column_name='opinion_curie'
    ) THEN
      ALTER TABLE opinion_keywords ADD COLUMN opinion_curie TEXT;
    END IF;
  END IF;
END $$;

UPDATE opinion_keywords ok
   SET opinion_curie = o.curie
  FROM opinions o
 WHERE o.id = ok.opinion_id
   AND ok.opinion_curie IS NULL
   AND o.curie IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_opinion_keywords_curie
  ON opinion_keywords (opinion_curie)
  WHERE opinion_curie IS NOT NULL;

-- ==================== opinion_citations ====================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_citations') THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_citations' AND column_name='opinion_curie'
    ) THEN
      ALTER TABLE opinion_citations ADD COLUMN opinion_curie TEXT;
    END IF;
  END IF;
END $$;

UPDATE opinion_citations oc
   SET opinion_curie = o.curie
  FROM opinions o
 WHERE o.id = oc.opinion_id
   AND oc.opinion_curie IS NULL
   AND o.curie IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_opinion_citations_curie
  ON opinion_citations (opinion_curie)
  WHERE opinion_curie IS NOT NULL;

-- ==================== opinion_overruled_cases (legacy, pre-092) ====================
-- This table is superseded by opinion_negative_treatments (092) but may
-- still exist on source DBs where 092 hasn't been applied yet.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_overruled_cases') THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_overruled_cases' AND column_name='opinion_curie'
    ) THEN
      ALTER TABLE opinion_overruled_cases ADD COLUMN opinion_curie TEXT;
    END IF;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_overruled_cases') THEN
    EXECUTE '
      UPDATE opinion_overruled_cases ooc
         SET opinion_curie = o.curie
        FROM opinions o
       WHERE o.id = ooc.opinion_id
         AND ooc.opinion_curie IS NULL
         AND o.curie IS NOT NULL';

    EXECUTE '
      CREATE INDEX IF NOT EXISTS idx_opinion_overruled_cases_curie
        ON opinion_overruled_cases (opinion_curie)
        WHERE opinion_curie IS NOT NULL';
  END IF;
END $$;

-- ==================== opinion_negative_treatments (post-092) ====================
-- May not exist yet on source DBs where 092 hasn't been applied.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_negative_treatments') THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_negative_treatments' AND column_name='opinion_curie'
    ) THEN
      ALTER TABLE opinion_negative_treatments ADD COLUMN opinion_curie TEXT;
    END IF;
  END IF;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_negative_treatments') THEN
    EXECUTE '
      UPDATE opinion_negative_treatments ont
         SET opinion_curie = o.curie
        FROM opinions o
       WHERE o.id = ont.opinion_id
         AND ont.opinion_curie IS NULL
         AND o.curie IS NOT NULL';

    EXECUTE '
      CREATE INDEX IF NOT EXISTS idx_ont_curie
        ON opinion_negative_treatments (opinion_curie)
        WHERE opinion_curie IS NOT NULL';
  END IF;
END $$;
