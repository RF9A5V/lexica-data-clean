-- Phase 1 / schema bootstrap — extensions + types.
-- All definitions ported verbatim from ny_reporter pg_dump (2026-05-25).

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

-- citation_type_enum — referenced by citations.citation_type + the
-- citation_type_priority() function backing v_case_preferred_citation.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'citation_type_enum') THEN
    CREATE TYPE public.citation_type_enum AS ENUM (
      'official',
      'parallel',
      'regional',
      'public_domain',
      'slip_op',
      'wl',
      'lexis'
    );
  END IF;
END
$$;
