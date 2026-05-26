-- Phase 1 / schema bootstrap — views.
-- Ported verbatim from ny_reporter. Picks the highest-priority parallel
-- citation per case for display, using citation_type_priority() (D-8).

CREATE VIEW public.v_case_preferred_citation AS
  SELECT DISTINCT ON (case_id)
    case_id,
    id AS citation_id,
    cite,
    curie,
    citation_type,
    normalized_form
  FROM public.citations
  ORDER BY case_id, (public.citation_type_priority(citation_type)), id;
