-- 060_opinion_value_flag.sql
-- Adds an idempotent flag to mark opinions with no substantive value so they can be skipped in Pass 2.

-- Add is_valueless boolean column if missing
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'opinions' AND column_name = 'is_valueless'
  ) THEN
    ALTER TABLE opinions ADD COLUMN is_valueless boolean NOT NULL DEFAULT false;
    COMMENT ON COLUMN opinions.is_valueless IS 'True when the opinion contains no substantive value for keywording/extraction; skip from Pass 2.';
  END IF;
END $$;

-- Add optional valueless_reason text column if missing
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'opinions' AND column_name = 'valueless_reason'
  ) THEN
    ALTER TABLE opinions ADD COLUMN valueless_reason text;
    COMMENT ON COLUMN opinions.valueless_reason IS 'Optional reason why the opinion was marked valueless (e.g., no opinion; memorandum; summary order).';
  END IF;
END $$;

-- Create an index on is_valueless to speed filtering
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
      AND c.relname = 'opinions_is_valueless_idx'
      AND n.nspname = 'public'
  ) THEN
    CREATE INDEX opinions_is_valueless_idx ON opinions (is_valueless);
  END IF;
END $$;
