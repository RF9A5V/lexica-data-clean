-- Phase 1: Rename keyword_validation to doctrines_by_field if present
DO $$
BEGIN
  IF to_regclass('public.keyword_validation') IS NOT NULL AND to_regclass('public.doctrines_by_field') IS NULL
  THEN
    EXECUTE 'ALTER TABLE keyword_validation RENAME TO doctrines_by_field';
  ELSIF to_regclass('public.keyword_validation') IS NOT NULL AND to_regclass('public.doctrines_by_field') IS NOT NULL THEN
    RAISE NOTICE 'Skipping rename: doctrines_by_field already exists; leaving keyword_validation as-is';
  END IF;
END $$;

-- Back-compat view (optional): retain old name as a view if table was renamed
DO $$
BEGIN
  IF to_regclass('public.doctrines_by_field') IS NOT NULL AND to_regclass('public.keyword_validation') IS NULL
  THEN
    EXECUTE 'CREATE OR REPLACE VIEW keyword_validation AS SELECT * FROM doctrines_by_field';
  END IF;
END $$;
