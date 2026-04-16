-- Migration: Final cut — make opinion_curie the sole analysis key.
--
-- Prerequisites (verify before running):
--   1. Migration 093 applied and CURIEs backfilled on opinions table
--   2. Migration 094 applied and opinion_curie backfilled on all analysis tables
--   3. Zero rows with opinion_curie IS NULL in any analysis table:
--
--      SELECT 'opinion_holdings' AS t, COUNT(*) FROM opinion_holdings WHERE opinion_curie IS NULL
--      UNION ALL SELECT 'opinion_keywords', COUNT(*) FROM opinion_keywords WHERE opinion_curie IS NULL
--      UNION ALL SELECT 'opinion_citations', COUNT(*) FROM opinion_citations WHERE opinion_curie IS NULL;
--
--   4. All code paths write opinion_curie (dual-write confirmed in
--      analysisQueueV2.js and co-data/pipelines/src/upsert.js)
--
-- This migration is DESTRUCTIVE and NOT easily reversible. The opinion_id
-- column and its FK constraints are dropped. Rolling back requires
-- re-adding the column and re-deriving opinion_id from opinions.curie.
--
-- No FK is added from analysis tables → opinions(curie). This is intentional:
-- the whole point of decoupling is that opinions can be rebuilt (via CAP
-- reimport) without cascading into analysis. Referential integrity is
-- enforced in application code and periodic data checks, not DB constraints.

-- ==================== opinion_holdings ====================
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name='opinion_holdings' AND column_name='opinion_curie'
  ) THEN
    -- Enforce NOT NULL
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_holdings' AND column_name='opinion_curie' AND is_nullable='YES'
    ) THEN
      -- Clear any orphaned rows that lack a curie (shouldn't exist post-094 backfill)
      DELETE FROM opinion_holdings WHERE opinion_curie IS NULL;
      ALTER TABLE opinion_holdings ALTER COLUMN opinion_curie SET NOT NULL;
    END IF;

    -- Drop opinion_id FK constraint if present
    IF EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conrelid = 'opinion_holdings'::regclass AND conname LIKE '%opinion_id%fkey%'
    ) THEN
      EXECUTE format('ALTER TABLE opinion_holdings DROP CONSTRAINT %I',
        (SELECT conname FROM pg_constraint
          WHERE conrelid = 'opinion_holdings'::regclass AND conname LIKE '%opinion_id%fkey%'
          LIMIT 1));
    END IF;

    -- Drop opinion_id column
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_holdings' AND column_name='opinion_id'
    ) THEN
      ALTER TABLE opinion_holdings DROP COLUMN opinion_id;
    END IF;
  END IF;
END $$;

-- ==================== opinion_keywords ====================

-- Drop materialized views that depend on opinion_keywords.opinion_id.
-- They will be recreated below using opinion_curie joins.
DROP MATERIALIZED VIEW IF EXISTS keyword_cooccurrence_cache CASCADE;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name='opinion_keywords' AND column_name='opinion_curie'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_keywords' AND column_name='opinion_curie' AND is_nullable='YES'
    ) THEN
      DELETE FROM opinion_keywords WHERE opinion_curie IS NULL;
      ALTER TABLE opinion_keywords ALTER COLUMN opinion_curie SET NOT NULL;
    END IF;

    -- Drop the unique constraint that includes opinion_id before dropping the column
    IF EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conrelid = 'opinion_keywords'::regclass AND contype = 'u'
         AND array_to_string(conkey, ',') LIKE '%'
         || (SELECT attnum::text FROM pg_attribute WHERE attrelid = 'opinion_keywords'::regclass AND attname = 'opinion_id')
         || '%'
    ) THEN
      EXECUTE format('ALTER TABLE opinion_keywords DROP CONSTRAINT %I',
        (SELECT conname FROM pg_constraint
          WHERE conrelid = 'opinion_keywords'::regclass AND contype = 'u'
            AND array_to_string(conkey, ',') LIKE '%'
            || (SELECT attnum::text FROM pg_attribute WHERE attrelid = 'opinion_keywords'::regclass AND attname = 'opinion_id')
            || '%'
          LIMIT 1));
    END IF;

    IF EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conrelid = 'opinion_keywords'::regclass AND conname LIKE '%opinion_id%fkey%'
    ) THEN
      EXECUTE format('ALTER TABLE opinion_keywords DROP CONSTRAINT %I',
        (SELECT conname FROM pg_constraint
          WHERE conrelid = 'opinion_keywords'::regclass AND conname LIKE '%opinion_id%fkey%'
          LIMIT 1));
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_keywords' AND column_name='opinion_id'
    ) THEN
      ALTER TABLE opinion_keywords DROP COLUMN opinion_id;
    END IF;

    -- Re-create uniqueness on (opinion_curie, keyword_id)
    CREATE UNIQUE INDEX IF NOT EXISTS uniq_opinion_keywords_curie_kw
      ON opinion_keywords (opinion_curie, keyword_id);
  END IF;
END $$;

-- Recreate keyword_cooccurrence_cache using opinion_curie joins.
CREATE MATERIALIZED VIEW keyword_cooccurrence_cache AS
WITH keyword_pairs AS (
    SELECT
        ok1.keyword_id AS keyword1_id,
        ok2.keyword_id AS keyword2_id,
        COUNT(DISTINCT o.case_id) AS cooccurrence_count,
        ARRAY_AGG(DISTINCT o.case_id ORDER BY o.case_id) AS case_ids
    FROM opinions o
    JOIN opinion_keywords ok1 ON o.curie = ok1.opinion_curie
    JOIN opinion_keywords ok2 ON o.curie = ok2.opinion_curie
    WHERE ok1.keyword_id < ok2.keyword_id
    GROUP BY ok1.keyword_id, ok2.keyword_id
    HAVING COUNT(DISTINCT o.case_id) > 1
)
SELECT
    kp.keyword1_id,
    k1.keyword_text AS keyword1_text,
    k1.tier AS keyword1_tier,
    kp.keyword2_id,
    k2.keyword_text AS keyword2_text,
    k2.tier AS keyword2_tier,
    kp.cooccurrence_count,
    kp.case_ids,
    NOW() AS last_refreshed
FROM keyword_pairs kp
JOIN keywords k1 ON kp.keyword1_id = k1.id
JOIN keywords k2 ON kp.keyword2_id = k2.id
WHERE k1.tier != 'procedural_posture'
  AND k2.tier != 'procedural_posture';

CREATE INDEX IF NOT EXISTS idx_keyword_cooccurrence_keyword1 ON keyword_cooccurrence_cache(keyword1_id);
CREATE INDEX IF NOT EXISTS idx_keyword_cooccurrence_keyword2 ON keyword_cooccurrence_cache(keyword2_id);
CREATE INDEX IF NOT EXISTS idx_keyword_cooccurrence_both ON keyword_cooccurrence_cache(keyword1_id, keyword2_id);

-- ==================== opinion_citations ====================
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_name='opinion_citations' AND column_name='opinion_curie'
  ) THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_citations' AND column_name='opinion_curie' AND is_nullable='YES'
    ) THEN
      DELETE FROM opinion_citations WHERE opinion_curie IS NULL;
      ALTER TABLE opinion_citations ALTER COLUMN opinion_curie SET NOT NULL;
    END IF;

    IF EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conrelid = 'opinion_citations'::regclass AND conname LIKE '%opinion_id%fkey%'
    ) THEN
      EXECUTE format('ALTER TABLE opinion_citations DROP CONSTRAINT %I',
        (SELECT conname FROM pg_constraint
          WHERE conrelid = 'opinion_citations'::regclass AND conname LIKE '%opinion_id%fkey%'
          LIMIT 1));
    END IF;

    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_citations' AND column_name='opinion_id'
    ) THEN
      ALTER TABLE opinion_citations DROP COLUMN opinion_id;
    END IF;
  END IF;
END $$;

-- ==================== opinion_overruled_cases (legacy, pre-092) ====================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_overruled_cases') THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_overruled_cases' AND column_name='opinion_curie'
    ) THEN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name='opinion_overruled_cases' AND column_name='opinion_curie' AND is_nullable='YES'
      ) THEN
        DELETE FROM opinion_overruled_cases WHERE opinion_curie IS NULL;
        ALTER TABLE opinion_overruled_cases ALTER COLUMN opinion_curie SET NOT NULL;
      END IF;

      IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'opinion_overruled_cases'::regclass AND conname LIKE '%opinion_id%fkey%'
      ) THEN
        EXECUTE format('ALTER TABLE opinion_overruled_cases DROP CONSTRAINT %I',
          (SELECT conname FROM pg_constraint
            WHERE conrelid = 'opinion_overruled_cases'::regclass AND conname LIKE '%opinion_id%fkey%'
            LIMIT 1));
      END IF;

      -- Drop uniqueness constraints that reference opinion_id
      IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conrelid = 'opinion_overruled_cases'::regclass
          AND conname = 'uniq_opinion_overruled_cases_key'
      ) THEN
        ALTER TABLE opinion_overruled_cases DROP CONSTRAINT uniq_opinion_overruled_cases_key;
      END IF;

      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name='opinion_overruled_cases' AND column_name='opinion_id'
      ) THEN
        ALTER TABLE opinion_overruled_cases DROP COLUMN opinion_id;
      END IF;
    END IF;
  END IF;
END $$;

-- ==================== opinion_negative_treatments (post-092) ====================
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname='opinion_negative_treatments') THEN
    IF EXISTS (
      SELECT 1 FROM information_schema.columns
       WHERE table_name='opinion_negative_treatments' AND column_name='opinion_curie'
    ) THEN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name='opinion_negative_treatments' AND column_name='opinion_curie' AND is_nullable='YES'
      ) THEN
        DELETE FROM opinion_negative_treatments WHERE opinion_curie IS NULL;
        ALTER TABLE opinion_negative_treatments ALTER COLUMN opinion_curie SET NOT NULL;
      END IF;

      IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'opinion_negative_treatments'::regclass AND conname LIKE '%opinion_id%fkey%'
      ) THEN
        EXECUTE format('ALTER TABLE opinion_negative_treatments DROP CONSTRAINT %I',
          (SELECT conname FROM pg_constraint
            WHERE conrelid = 'opinion_negative_treatments'::regclass AND conname LIKE '%opinion_id%fkey%'
            LIMIT 1));
      END IF;

      -- Drop the dedup unique index that references opinion_id
      DROP INDEX IF EXISTS uniq_ont_dedup;

      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_name='opinion_negative_treatments' AND column_name='opinion_id'
      ) THEN
        ALTER TABLE opinion_negative_treatments DROP COLUMN opinion_id;
      END IF;

      -- Recreate dedup index keyed on opinion_curie
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_ont_dedup
        ON opinion_negative_treatments (
          opinion_curie, tier, type,
          COALESCE(case_name, ''),
          COALESCE(citation, ''),
          basis
        );
    END IF;
  END IF;
END $$;
