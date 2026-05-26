-- Phase 1 / schema bootstrap — functions.
-- All ported verbatim from ny_reporter pg_dump.
-- Spec D-7 calls out cases_name_search_trigger + cases_bump_updated_at;
-- D-8 calls out citation_type_priority. The remaining trigger functions
-- (doctrine_anchors_*, doctrine_case_classifications_*, keyword_relations_*,
-- update_opinion_text_search_vector) are needed by triggers we're carrying
-- over verbatim from the source DBs and so come along too.

-- ---------------------------------------------------------------------------
-- cases triggers (D-7)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.cases_bump_updated_at() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION public.cases_name_search_trigger() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.name_search_vector := to_tsvector(
    'english',
    coalesce(NEW.name, '') || ' ' || coalesce(NEW.name_abbreviation, '')
  );
  RETURN NEW;
END
$$;

-- ---------------------------------------------------------------------------
-- citation_type_priority — backs v_case_preferred_citation ORDER BY (D-8).
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.citation_type_priority(t public.citation_type_enum)
  RETURNS integer LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE t
    WHEN 'official'      THEN 1
    WHEN 'parallel'      THEN 2
    WHEN 'public_domain' THEN 3
    WHEN 'regional'      THEN 4
    WHEN 'slip_op'       THEN 5
    WHEN 'wl'            THEN 6
    WHEN 'lexis'         THEN 7
  END
$$;

-- ---------------------------------------------------------------------------
-- doctrine_anchors triggers
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.doctrine_anchors_bump_updated_at() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION public.doctrine_anchors_validate_keyword_tiers() RETURNS trigger
  LANGUAGE plpgsql AS $$
DECLARE
  v_doc_tier TEXT;
  v_fol_tier TEXT;
BEGIN
  SELECT tier INTO v_doc_tier FROM keywords WHERE id = NEW.doctrine_keyword_id;
  IF v_doc_tier IS NULL OR v_doc_tier <> 'doctrine' THEN
    RAISE EXCEPTION
      'doctrine_anchors.doctrine_keyword_id (%) must reference keywords.tier=doctrine, got %',
      NEW.doctrine_keyword_id, COALESCE(v_doc_tier, 'NULL');
  END IF;

  IF NEW.field_of_law_keyword_id IS NOT NULL THEN
    SELECT tier INTO v_fol_tier FROM keywords WHERE id = NEW.field_of_law_keyword_id;
    IF v_fol_tier IS NULL OR v_fol_tier <> 'field_of_law' THEN
      RAISE EXCEPTION
        'doctrine_anchors.field_of_law_keyword_id (%) must reference keywords.tier=field_of_law, got %',
        NEW.field_of_law_keyword_id, COALESCE(v_fol_tier, 'NULL');
    END IF;
  END IF;

  RETURN NEW;
END
$$;

-- ---------------------------------------------------------------------------
-- doctrine_case_classifications + keyword_relations updated_at triggers
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.doctrine_case_classifications_bump_updated_at() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION public.keyword_relations_bump_updated_at() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END
$$;

-- ---------------------------------------------------------------------------
-- opinions.text_search_vector trigger
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.update_opinion_text_search_vector() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  NEW.text_search_vector := to_tsvector('english', COALESCE(NEW.text, ''));
  RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- batch_jobs helpers — used by admin tooling, kept for parity. The
-- materialized view refresh functions (refresh_case_citation_counts,
-- refresh_keyword_cooccurrence_cache) are deliberately omitted because
-- their referenced mat-views aren't created here; reintroduce them with
-- the mat-views if a follow-up needs them.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION public.get_batch_stats(p_batch_id character varying)
  RETURNS TABLE(
    total_opinions integer,
    pending_opinions integer,
    completed_opinions integer,
    failed_opinions integer,
    successful_extractions integer,
    validation_errors integer,
    upsert_errors integer
  )
  LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT
    COUNT(*)::INTEGER AS total_opinions,
    COUNT(CASE WHEN bor.status = 'pending'   THEN 1 END)::INTEGER AS pending_opinions,
    COUNT(CASE WHEN bor.status = 'completed' THEN 1 END)::INTEGER AS completed_opinions,
    COUNT(CASE WHEN bor.status = 'failed'    THEN 1 END)::INTEGER AS failed_opinions,
    COUNT(CASE WHEN bor.extraction_successful = true   THEN 1 END)::INTEGER AS successful_extractions,
    COUNT(CASE WHEN bor.validation_successful = false  THEN 1 END)::INTEGER AS validation_errors,
    COUNT(CASE WHEN bor.upsert_successful = false      THEN 1 END)::INTEGER AS upsert_errors
  FROM batch_jobs bj
  JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
  WHERE bj.batch_id = p_batch_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.update_batch_job_status(
  p_batch_id character varying,
  p_status character varying,
  p_completed_requests integer DEFAULT NULL,
  p_failed_requests integer DEFAULT NULL,
  p_output_file_id character varying DEFAULT NULL,
  p_error_file_id character varying DEFAULT NULL,
  p_error_message text DEFAULT NULL
) RETURNS void
  LANGUAGE plpgsql AS $$
BEGIN
  UPDATE batch_jobs
  SET
    status              = p_status,
    completed_requests  = COALESCE(p_completed_requests, completed_requests),
    failed_requests     = COALESCE(p_failed_requests,    failed_requests),
    output_file_id      = COALESCE(p_output_file_id,     output_file_id),
    error_file_id       = COALESCE(p_error_file_id,      error_file_id),
    error_message       = COALESCE(p_error_message,      error_message),
    completed_at        = CASE
      WHEN p_status IN ('completed','failed','expired','cancelled') THEN NOW()
      ELSE completed_at
    END
  WHERE batch_id = p_batch_id;
END;
$$;
