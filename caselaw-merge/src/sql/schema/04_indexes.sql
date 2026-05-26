-- Phase 1 / schema bootstrap — indexes.
-- All ported from ny_reporter pg_dump, minus the two D-0b / D-0c deviations:
--   * idx_citations_case_type   — dropped (D-0b)
--   * idx_keywords_text_tier    — partial form preserved (D-0c)
-- Plus the new source_ref indexes per spec §C.1 / C.2.

-- ---------------------------------------------------------------------------
-- appellate_history_*
-- ---------------------------------------------------------------------------
CREATE INDEX idx_ahc_citing_case        ON public.appellate_history_connections (citing_case_id);
CREATE INDEX idx_ahc_citing_opinion     ON public.appellate_history_connections (citing_opinion_id);
CREATE INDEX idx_ahc_extraction_method  ON public.appellate_history_connections (extraction_method);
CREATE INDEX idx_ahc_relation_kind      ON public.appellate_history_connections (relation_kind);
CREATE INDEX idx_ahc_source_case        ON public.appellate_history_connections (source_case_source_ref, source_case_id, relation);
CREATE INDEX idx_ahc_target_case        ON public.appellate_history_connections (target_case_source_ref, target_case_id, relation);

CREATE INDEX idx_ahcs_completeness      ON public.appellate_history_case_status (completeness);

CREATE INDEX idx_ahrq_citing_case       ON public.appellate_history_resolution_queue (citing_case_id);
CREATE INDEX idx_ahrq_citing_opinion    ON public.appellate_history_resolution_queue (citing_opinion_id);
CREATE INDEX idx_ahrq_status            ON public.appellate_history_resolution_queue (status, enqueued_at);

CREATE UNIQUE INDEX uniq_ahc_idempotency
  ON public.appellate_history_connections
  (citing_opinion_id, raw_citation_string, parser_version, extraction_method, COALESCE(llm_extraction_version, 0))
  WHERE citing_opinion_id IS NOT NULL;

CREATE UNIQUE INDEX uniq_ahc_idempotency_legacy
  ON public.appellate_history_connections
  (citing_case_id, raw_citation_string, parser_version, extraction_method, COALESCE(llm_extraction_version, 0))
  WHERE citing_opinion_id IS NULL;

CREATE UNIQUE INDEX uniq_ahrq_idempotency
  ON public.appellate_history_resolution_queue
  (citing_opinion_id, raw_window, parser_version)
  WHERE citing_opinion_id IS NOT NULL;

CREATE UNIQUE INDEX uniq_ahrq_idempotency_legacy
  ON public.appellate_history_resolution_queue
  (citing_case_id, raw_window, parser_version)
  WHERE citing_opinion_id IS NULL;

-- ---------------------------------------------------------------------------
-- batch_jobs + batch_opinion_requests
-- ---------------------------------------------------------------------------
CREATE INDEX idx_batch_jobs_batch_id              ON public.batch_jobs (batch_id);
CREATE INDEX idx_batch_jobs_created_at            ON public.batch_jobs (created_at);
CREATE INDEX idx_batch_jobs_status                ON public.batch_jobs (status);
CREATE INDEX idx_batch_opinion_requests_batch_job_id ON public.batch_opinion_requests (batch_job_id);
CREATE INDEX idx_batch_opinion_requests_opinion_id   ON public.batch_opinion_requests (opinion_id);
CREATE INDEX idx_batch_opinion_requests_status       ON public.batch_opinion_requests (status);

-- ---------------------------------------------------------------------------
-- case_captions
-- ---------------------------------------------------------------------------
CREATE INDEX        idx_case_captions_case_id     ON public.case_captions (case_id);
CREATE INDEX        idx_case_captions_name_search ON public.case_captions
  USING gin (to_tsvector('english'::regconfig, COALESCE(name_abbreviation, '') || ' ' || COALESCE(name, '')));
CREATE UNIQUE INDEX uniq_case_captions_idx        ON public.case_captions (case_id, caption_index);

-- ---------------------------------------------------------------------------
-- cases
-- ---------------------------------------------------------------------------
CREATE INDEX idx_cases_citation_count             ON public.cases (citation_count DESC);
CREATE INDEX idx_cases_court_department           ON public.cases (court_department) WHERE court_department IS NOT NULL;
CREATE INDEX idx_cases_court_id                   ON public.cases (court_id);
CREATE INDEX idx_cases_decision_date              ON public.cases (decision_date);
CREATE INDEX idx_cases_display_slug               ON public.cases (display_slug) WHERE display_slug IS NOT NULL;
CREATE INDEX idx_cases_fulltext                   ON public.cases USING gin (to_tsvector('english'::regconfig, COALESCE(name, '') || ' ' || COALESCE(name_abbreviation, '')));
CREATE INDEX idx_cases_jurisdiction_id            ON public.cases (jurisdiction_id);
CREATE INDEX idx_cases_name                       ON public.cases USING gin (to_tsvector('english'::regconfig, name));
CREATE INDEX idx_cases_name_abbreviation_lower    ON public.cases (lower(name_abbreviation), decision_date, court_id) WHERE name_abbreviation IS NOT NULL;
CREATE INDEX idx_cases_name_abbreviation_trgm     ON public.cases USING gin (name_abbreviation public.gin_trgm_ops) WHERE name_abbreviation IS NOT NULL;
CREATE INDEX idx_cases_name_search_vector         ON public.cases USING gin (name_search_vector);
CREATE INDEX idx_cases_original_id                ON public.cases (original_id);

-- New per spec §C.1: source-filtered library queries + source-date pagination.
CREATE INDEX idx_cases_source_ref                 ON public.cases (source_ref);
CREATE INDEX idx_cases_source_decision_date       ON public.cases (source_ref, decision_date);

CREATE UNIQUE INDEX uniq_cases_curie              ON public.cases (curie) WHERE curie IS NOT NULL;

-- ---------------------------------------------------------------------------
-- citations + citation_corrections
-- (idx_citations_case_type dropped per D-0b)
-- ---------------------------------------------------------------------------
CREATE INDEX idx_citations_case_id                ON public.citations (case_id);
CREATE INDEX idx_citations_cite                   ON public.citations (cite);
CREATE INDEX idx_citations_normalized_form        ON public.citations (normalized_form) WHERE normalized_form IS NOT NULL;
CREATE UNIQUE INDEX uniq_citations_curie          ON public.citations (curie) WHERE curie IS NOT NULL;

CREATE UNIQUE INDEX uq_citation_corrections_applied
  ON public.citation_corrections (citation_id) WHERE applied = true;

-- ---------------------------------------------------------------------------
-- doctrine_anchors + doctrine_case_classifications
-- ---------------------------------------------------------------------------
CREATE INDEX idx_doctrine_anchors_co_occurrence_count   ON public.doctrine_anchors (co_occurrence_count DESC NULLS LAST);
CREATE INDEX idx_doctrine_anchors_doctrine_keyword_id   ON public.doctrine_anchors (doctrine_keyword_id);
CREATE INDEX idx_doctrine_anchors_field_of_law_keyword_id
  ON public.doctrine_anchors (field_of_law_keyword_id) WHERE field_of_law_keyword_id IS NOT NULL;
CREATE INDEX idx_doctrine_anchors_status                ON public.doctrine_anchors (status);
CREATE UNIQUE INDEX uniq_doctrine_anchors_doc_field
  ON public.doctrine_anchors (doctrine_keyword_id, field_of_law_keyword_id) WHERE field_of_law_keyword_id IS NOT NULL;
CREATE UNIQUE INDEX uniq_doctrine_anchors_doc_null_field
  ON public.doctrine_anchors (doctrine_keyword_id) WHERE field_of_law_keyword_id IS NULL;

CREATE INDEX idx_doctrine_case_classifications_anchor_classification
  ON public.doctrine_case_classifications (anchor_id, classification);
CREATE INDEX idx_doctrine_case_classifications_case_id  ON public.doctrine_case_classifications (case_id);
CREATE INDEX idx_doctrine_case_classifications_run      ON public.doctrine_case_classifications (analysis_run_id);
CREATE INDEX idx_doctrine_case_classifications_status   ON public.doctrine_case_classifications (status) WHERE status <> 'active';
CREATE UNIQUE INDEX uniq_doctrine_case_classifications_anchor_case
  ON public.doctrine_case_classifications (anchor_id, case_id);

-- ---------------------------------------------------------------------------
-- keyword_relations + keywords
-- ---------------------------------------------------------------------------
CREATE INDEX idx_keyword_relations_anchor_id            ON public.keyword_relations (anchor_id);
CREATE INDEX idx_keyword_relations_anchor_relation_type ON public.keyword_relations (anchor_id, relation_type);
CREATE INDEX idx_keyword_relations_related_keyword_id   ON public.keyword_relations (related_keyword_id);
CREATE UNIQUE INDEX uniq_keyword_relations_anchor_keyword_type
  ON public.keyword_relations (anchor_id, related_keyword_id, relation_type);

CREATE INDEX idx_keywords_text_prefix                   ON public.keywords (keyword_text text_pattern_ops);
-- D-0c: partial form preserved (smaller; covers curator-tool access pattern).
CREATE INDEX idx_keywords_text_tier
  ON public.keywords (keyword_text, tier)
  WHERE tier IN ('doctrine','doctrinal_test','claim','defense');
CREATE INDEX idx_keywords_tier                          ON public.keywords (tier);
CREATE INDEX idx_keywords_trgm_doctrines
  ON public.keywords USING gin (keyword_text public.gin_trgm_ops)
  WHERE tier IN ('doctrine','doctrinal_test');

-- ---------------------------------------------------------------------------
-- opinion_negative_treatments
-- ---------------------------------------------------------------------------
CREATE INDEX idx_ont_basis_trgm        ON public.opinion_negative_treatments USING gin (basis        public.gin_trgm_ops);
CREATE INDEX idx_ont_case_name_trgm    ON public.opinion_negative_treatments USING gin (case_name    public.gin_trgm_ops);
CREATE INDEX idx_ont_citation_trgm     ON public.opinion_negative_treatments USING gin (citation     public.gin_trgm_ops);
CREATE INDEX idx_ont_opinion_curie     ON public.opinion_negative_treatments (opinion_curie);
CREATE INDEX idx_ont_tier              ON public.opinion_negative_treatments (tier);
CREATE INDEX idx_ont_type              ON public.opinion_negative_treatments (type);
CREATE UNIQUE INDEX uniq_ont_dedup_active
  ON public.opinion_negative_treatments
  (opinion_curie, tier, type, COALESCE(case_name, ''), COALESCE(citation, ''), basis)
  WHERE status = 'active';

-- ---------------------------------------------------------------------------
-- opinion_citations
-- ---------------------------------------------------------------------------
CREATE INDEX idx_opinion_citations_case_name_trgm   ON public.opinion_citations USING gin (case_name            public.gin_trgm_ops);
CREATE INDEX idx_opinion_citations_cite_text_trgm   ON public.opinion_citations USING gin (cite_text            public.gin_trgm_ops);
CREATE INDEX idx_opinion_citations_curie            ON public.opinion_citations (opinion_curie) WHERE opinion_curie IS NOT NULL;
CREATE INDEX idx_opinion_citations_normalized_trgm  ON public.opinion_citations USING gin (normalized_citation  public.gin_trgm_ops);
CREATE INDEX idx_opinion_citations_run              ON public.opinion_citations (analysis_run_id);
CREATE INDEX idx_opinion_citations_status           ON public.opinion_citations (status) WHERE status <> 'active';

-- ---------------------------------------------------------------------------
-- opinion_footnotes (D-6: opinion_id dropped; unique key is now curie+index)
-- ---------------------------------------------------------------------------
CREATE INDEX        idx_opinion_footnotes_curie   ON public.opinion_footnotes (opinion_curie);
CREATE UNIQUE INDEX uniq_opinion_footnotes_idx    ON public.opinion_footnotes (opinion_curie, footnote_index);

-- ---------------------------------------------------------------------------
-- opinion_holdings + opinion_keywords
-- ---------------------------------------------------------------------------
CREATE INDEX idx_opinion_holdings_curie         ON public.opinion_holdings (opinion_curie) WHERE opinion_curie IS NOT NULL;
CREATE INDEX idx_opinion_holdings_holding_trgm  ON public.opinion_holdings USING gin (holding   public.gin_trgm_ops);
CREATE INDEX idx_opinion_holdings_issue_trgm    ON public.opinion_holdings USING gin (issue     public.gin_trgm_ops);
CREATE INDEX idx_opinion_holdings_reasoning_trgm ON public.opinion_holdings USING gin (reasoning public.gin_trgm_ops);
CREATE INDEX idx_opinion_holdings_rule_trgm     ON public.opinion_holdings USING gin (rule      public.gin_trgm_ops);
CREATE INDEX idx_opinion_holdings_run           ON public.opinion_holdings (analysis_run_id);
CREATE INDEX idx_opinion_holdings_status        ON public.opinion_holdings (status) WHERE status <> 'active';

CREATE INDEX idx_opinion_keywords_curie         ON public.opinion_keywords (opinion_curie) WHERE opinion_curie IS NOT NULL;
CREATE INDEX idx_opinion_keywords_keyword       ON public.opinion_keywords (keyword_id);
CREATE INDEX idx_opinion_keywords_run           ON public.opinion_keywords (analysis_run_id);
CREATE INDEX idx_opinion_keywords_status        ON public.opinion_keywords (status) WHERE status <> 'active';
CREATE UNIQUE INDEX uniq_opinion_keywords_curie_kw_active
  ON public.opinion_keywords (opinion_curie, keyword_id) WHERE status = 'active';

CREATE INDEX idx_opinion_negative_treatments_run     ON public.opinion_negative_treatments (analysis_run_id);
CREATE INDEX idx_opinion_negative_treatments_status  ON public.opinion_negative_treatments (status) WHERE status <> 'active';

-- ---------------------------------------------------------------------------
-- opinions (curie has UNIQUE constraint inline in 03_tables.sql)
-- ---------------------------------------------------------------------------
CREATE INDEX idx_opinions_author              ON public.opinions (author);
CREATE INDEX idx_opinions_case_id             ON public.opinions (case_id);
CREATE INDEX idx_opinions_text                ON public.opinions USING gin (to_tsvector('english'::regconfig, text));
CREATE INDEX idx_opinions_text_search_vector  ON public.opinions USING gin (text_search_vector);
CREATE INDEX opinions_is_valueless_idx        ON public.opinions (is_valueless);
CREATE UNIQUE INDEX uniq_opinions_case_index  ON public.opinions (case_id, opinion_index) WHERE opinion_index IS NOT NULL;

-- New per spec §C.2.
CREATE INDEX idx_opinions_source_ref          ON public.opinions (source_ref);
