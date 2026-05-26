-- Phase 1 / schema bootstrap — triggers.
-- All ported verbatim from ny_reporter.

-- cases (D-7)
CREATE TRIGGER cases_name_search_update
  BEFORE INSERT OR UPDATE OF name, name_abbreviation ON public.cases
  FOR EACH ROW EXECUTE FUNCTION public.cases_name_search_trigger();

CREATE TRIGGER cases_updated_at_trigger
  BEFORE UPDATE ON public.cases
  FOR EACH ROW EXECUTE FUNCTION public.cases_bump_updated_at();

-- doctrine_anchors
CREATE TRIGGER doctrine_anchors_updated_at_trigger
  BEFORE UPDATE ON public.doctrine_anchors
  FOR EACH ROW EXECUTE FUNCTION public.doctrine_anchors_bump_updated_at();

CREATE TRIGGER doctrine_anchors_validate_tiers_trigger
  BEFORE INSERT OR UPDATE OF doctrine_keyword_id, field_of_law_keyword_id
  ON public.doctrine_anchors
  FOR EACH ROW EXECUTE FUNCTION public.doctrine_anchors_validate_keyword_tiers();

-- doctrine_case_classifications
CREATE TRIGGER doctrine_case_classifications_updated_at_trigger
  BEFORE UPDATE ON public.doctrine_case_classifications
  FOR EACH ROW EXECUTE FUNCTION public.doctrine_case_classifications_bump_updated_at();

-- keyword_relations
CREATE TRIGGER keyword_relations_updated_at_trigger
  BEFORE UPDATE ON public.keyword_relations
  FOR EACH ROW EXECUTE FUNCTION public.keyword_relations_bump_updated_at();

-- opinions
CREATE TRIGGER trigger_update_opinion_text_search_vector
  BEFORE INSERT OR UPDATE OF text ON public.opinions
  FOR EACH ROW EXECUTE FUNCTION public.update_opinion_text_search_vector();
