-- Phase 1 / schema bootstrap — tables.
-- Spec deviations from ny_reporter, applied below:
--   * `metadata` table dropped (D-0a) — not in ny_reporter to start, but
--     also explicitly excluded for the other two sources.
--   * `schema_migrations` dropped (§C.8) — new DB starts fresh; whatever
--     migration tool the post-merge admin path picks owns this.
--   * `opinion_footnotes`: `opinion_id` column dropped; `opinion_curie`
--     becomes the lone FK-bearing column (D-6). The pre-merge DDL prereq
--     `ALTER COLUMN opinion_curie SET NOT NULL` on source DBs makes this
--     safe to merge.
--   * `cases`, `opinions`, `analysis_runs`, `batch_jobs`: gain
--     `source_ref text NOT NULL CHECK (... IN three values)` per D-2.
--   * `opinions.curie` and `opinion_footnotes.opinion_curie` are NOT NULL
--     so a regular UNIQUE constraint on opinions.curie can back the
--     footnotes FK. cases.curie + citations.curie keep their existing
--     partial-unique-index nullable form (no FKs target them).
--   * New table `keyword_dedup_conflicts` (D-3) for curator review of the
--     ~83 cross-source tier classifications that disagree.

-- ---------------------------------------------------------------------------
-- Reference / source-stamping
-- ---------------------------------------------------------------------------

-- source_ref check constraint values match reference_source_ref_vs_db_name:
--   ny_supreme  = was ny_reporter            DB (NY Court of Appeals)
--   ny_appellate = was ny_appellate_division DB
--   ny_trial    = was ny_trial_courts        DB

-- ---------------------------------------------------------------------------
-- analysis_runs
-- ---------------------------------------------------------------------------

CREATE TABLE public.analysis_runs (
  id              bigserial PRIMARY KEY,
  env             text NOT NULL,
  pipeline        text NOT NULL,
  model           text NOT NULL,
  prompt_version  text NOT NULL,
  schema_version  text,
  git_sha         text,
  started_at      timestamp with time zone NOT NULL DEFAULT now(),
  finished_at     timestamp with time zone,
  input_tokens    bigint,
  output_tokens   bigint,
  cost_usd        numeric,
  notes           text,
  source_ref      text NOT NULL CHECK (source_ref IN ('ny_supreme','ny_appellate','ny_trial'))
);

-- ---------------------------------------------------------------------------
-- keywords + keyword_dedup_conflicts
-- ---------------------------------------------------------------------------

CREATE TABLE public.keywords (
  id            serial  PRIMARY KEY,
  keyword_text  text    NOT NULL UNIQUE,
  tier          text    NOT NULL,
  frequency     integer NOT NULL DEFAULT 0,
  created_at    timestamp with time zone DEFAULT now(),
  CONSTRAINT keywords_tier_allowed_superset CHECK (
    tier IN (
      'field_of_law','major_doctrine','legal_concept','doctrine',
      'doctrinal_test','distinguishing_factor','procedural_posture','case_outcome'
    )
  )
);

-- D-3: ~83 expected rows after phase 2. Curator workflow consumes via
-- /admin/keyword-conflicts (out of scope for B1).
CREATE TABLE public.keyword_dedup_conflicts (
  id                bigserial PRIMARY KEY,
  keyword_text      text      NOT NULL,
  winning_tier      text      NOT NULL,
  losing_tier       text      NOT NULL,
  winning_count     integer   NOT NULL,
  losing_count      integer   NOT NULL,
  sources_winning   text[]    NOT NULL,
  sources_losing    text[]    NOT NULL,
  curator_decision  text      CHECK (
    curator_decision IS NULL
    OR curator_decision IN ('accept_winner','use_losing','split_into_two','ignore')
  ),
  curator_notes     text,
  created_at        timestamp with time zone NOT NULL DEFAULT now(),
  reviewed_at       timestamp with time zone
);

-- ---------------------------------------------------------------------------
-- cases
-- ---------------------------------------------------------------------------

CREATE TABLE public.cases (
  id                         bigserial PRIMARY KEY,
  name                       text NOT NULL,
  name_abbreviation          text,
  decision_date              text,                       -- text per feedback_decision_date_text
  docket_number              text,
  first_page                 text,
  last_page                  text,
  file_name                  text,
  court_name                 text,
  court_name_abbreviation    text,
  court_id                   integer,
  jurisdiction_name          text,
  jurisdiction_abbreviation  text,
  jurisdiction_id            integer,
  created_at                 timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  updated_at                 timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  citation_count             integer DEFAULT 0,
  original_id                bigint,
  name_search_vector         tsvector,
  source_url                 text,
  court_department           smallint,
  curie                      text,
  curie_locked_at            timestamp with time zone,
  display_slug               text,
  source_ref                 text NOT NULL CHECK (source_ref IN ('ny_supreme','ny_appellate','ny_trial')),
  CONSTRAINT cases_court_department_chk
    CHECK (court_department IS NULL OR (court_department BETWEEN 1 AND 4))
);

COMMENT ON COLUMN public.cases.original_id IS
  'Original case ID from extracted JSON data for cross-database citation resolution';

-- ---------------------------------------------------------------------------
-- case_captions
-- ---------------------------------------------------------------------------

CREATE TABLE public.case_captions (
  id                 bigserial PRIMARY KEY,
  case_id            bigint NOT NULL REFERENCES public.cases(id) ON DELETE CASCADE,
  caption_index      integer NOT NULL,
  name               text NOT NULL,
  name_abbreviation  text,
  docket_number      text,
  created_at         timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- case_notes
-- ---------------------------------------------------------------------------

CREATE TABLE public.case_notes (
  id          bigserial PRIMARY KEY,
  case_id     bigint REFERENCES public.cases(id) ON DELETE CASCADE,
  note_type   text NOT NULL,
  note        text NOT NULL,
  metadata    jsonb,
  created_at  timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- citations + citation_corrections
-- ---------------------------------------------------------------------------

CREATE TABLE public.citations (
  id              serial PRIMARY KEY,
  case_id         bigint NOT NULL REFERENCES public.cases(id) ON DELETE CASCADE,
  citation_type   public.citation_type_enum,
  cite            text NOT NULL,
  created_at      timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  curie           text,
  normalized_form text
);

CREATE TABLE public.citation_corrections (
  id                  bigserial PRIMARY KEY,
  citation_id         integer REFERENCES public.citations(id) ON DELETE CASCADE,
  case_id             bigint  REFERENCES public.cases(id)     ON DELETE CASCADE,
  observed_cite       text NOT NULL,
  observed_reporter   text,
  corrected_cite      text NOT NULL,
  expected_reporter   text,
  source_id           text,
  file_volume         integer,
  reason              text,
  confidence          text,
  applied             boolean DEFAULT false,
  applied_at          timestamp with time zone,
  created_at          timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- opinions
-- ---------------------------------------------------------------------------
-- curie is forced NOT NULL UNIQUE (full constraint, not partial index) so
-- it can be the target of opinion_footnotes.opinion_curie FK. Audit E
-- confirmed 0 nulls in opinions.curie across all sources (968k rows).

CREATE TABLE public.opinions (
  id                  serial PRIMARY KEY,
  case_id             bigint NOT NULL REFERENCES public.cases(id) ON DELETE CASCADE,
  opinion_type        text,
  author              text,
  text                text,
  created_at          timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
  is_valueless        boolean NOT NULL DEFAULT false,
  valueless_reason    text,
  text_search_vector  tsvector,
  curie               text NOT NULL UNIQUE,
  opinion_index       integer,
  page_breaks         jsonb,
  source_ref          text NOT NULL CHECK (source_ref IN ('ny_supreme','ny_appellate','ny_trial'))
);

COMMENT ON COLUMN public.opinions.is_valueless IS
  'True when the opinion contains no substantive value for keywording/extraction; skip from Pass 2.';
COMMENT ON COLUMN public.opinions.valueless_reason IS
  'Optional reason why the opinion was marked valueless (e.g., no opinion; memorandum; summary order).';

-- ---------------------------------------------------------------------------
-- opinion_footnotes (D-6: opinion_id dropped; opinion_curie is the join key)
-- ---------------------------------------------------------------------------

CREATE TABLE public.opinion_footnotes (
  id              bigserial PRIMARY KEY,
  opinion_curie   text NOT NULL REFERENCES public.opinions(curie) ON DELETE CASCADE,
  footnote_index  integer NOT NULL,
  marker          text NOT NULL,
  text            text,
  body_offset     integer,
  page_index      integer,
  volume_page     integer,
  created_at      timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- opinion_holdings + opinion_keywords + opinion_negative_treatments
-- + opinion_citations — all key on opinion_curie (text), not opinion_id.
-- ---------------------------------------------------------------------------

CREATE TABLE public.opinion_holdings (
  id                  serial PRIMARY KEY,
  issue               text NOT NULL,
  holding             text NOT NULL,
  rule                text NOT NULL,
  reasoning           text NOT NULL,
  precedential_value  text NOT NULL,
  confidence          numeric NOT NULL,
  created_at          timestamp with time zone DEFAULT now(),
  opinion_curie       text NOT NULL,
  analysis_run_id     bigint REFERENCES public.analysis_runs(id),
  status              text NOT NULL DEFAULT 'active',
  CONSTRAINT opinion_holdings_confidence_check         CHECK (confidence >= 0.5 AND confidence <= 1),
  CONSTRAINT opinion_holdings_precedential_value_check CHECK (precedential_value IN ('high','medium','low')),
  CONSTRAINT opinion_holdings_status_check             CHECK (status IN ('active','superseded','suppressed'))
);

CREATE TABLE public.opinion_keywords (
  id                serial PRIMARY KEY,
  keyword_id        integer NOT NULL REFERENCES public.keywords(id) ON DELETE CASCADE,
  relevance_score   numeric,
  extraction_method text,
  category          text,
  context           jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at        timestamp with time zone DEFAULT now(),
  opinion_curie     text NOT NULL,
  analysis_run_id   bigint REFERENCES public.analysis_runs(id),
  status            text NOT NULL DEFAULT 'active',
  CONSTRAINT opinion_keywords_status_check CHECK (status IN ('active','superseded','suppressed'))
);

CREATE TABLE public.opinion_negative_treatments (
  id              serial PRIMARY KEY,
  opinion_curie   text NOT NULL,
  tier            text NOT NULL,
  type            text NOT NULL,
  case_name       text,
  citation        text,
  basis           text NOT NULL,
  created_at      timestamp with time zone DEFAULT now(),
  analysis_run_id bigint REFERENCES public.analysis_runs(id),
  status          text NOT NULL DEFAULT 'active',
  CONSTRAINT opinion_negative_treatments_tier_check
    CHECK (tier IN ('hard_negative','advisory')),
  CONSTRAINT opinion_negative_treatments_type_check
    CHECK (type IN ('overruled','reversed','declined_to_follow','distinguished','criticized','limited')),
  CONSTRAINT chk_ont_tier_type CHECK (
    (tier = 'hard_negative' AND type IN ('overruled','reversed'))
    OR
    (tier = 'advisory'      AND type IN ('declined_to_follow','distinguished','criticized','limited'))
  ),
  CONSTRAINT opinion_negative_treatments_status_check
    CHECK (status IN ('active','superseded','suppressed'))
);

CREATE TABLE public.opinion_citations (
  id                    serial PRIMARY KEY,
  cite_text             text,
  case_name             text,
  normalized_citation   text,
  authority_type        text,
  jurisdiction          text,
  court_level           text,
  year                  integer,
  pincite               text,
  citation_context      text,
  citation_signal       text,
  precedential_weight   text,
  discussion_level      text,
  legal_proposition     text,
  confidence            numeric,
  created_at            timestamp with time zone DEFAULT now(),
  opinion_curie         text NOT NULL,
  analysis_run_id       bigint REFERENCES public.analysis_runs(id),
  status                text NOT NULL DEFAULT 'active',
  CONSTRAINT opinion_citations_authority_type_check
    CHECK (authority_type IN ('case','statute','regulation','constitutional','secondary')),
  CONSTRAINT opinion_citations_court_level_check
    CHECK (court_level IN ('supreme','appellate','trial','federal_appellate','federal_district')),
  CONSTRAINT opinion_citations_precedential_weight_check
    CHECK (precedential_weight IN ('binding','highly_persuasive','persuasive','non_binding')),
  CONSTRAINT opinion_citations_confidence_check CHECK (confidence >= 0.5 AND confidence <= 1),
  CONSTRAINT opinion_citations_status_check     CHECK (status IN ('active','superseded','suppressed'))
);

-- ---------------------------------------------------------------------------
-- doctrine_anchors + doctrine_case_classifications + keyword_relations
-- ---------------------------------------------------------------------------

CREATE TABLE public.doctrine_anchors (
  id                       serial PRIMARY KEY,
  doctrine_keyword_id      integer NOT NULL REFERENCES public.keywords(id) ON DELETE RESTRICT,
  field_of_law_keyword_id  integer          REFERENCES public.keywords(id) ON DELETE RESTRICT,
  description              text,
  co_occurrence_count      integer NOT NULL DEFAULT 0,
  confidence_tier          text    NOT NULL DEFAULT 'uncurated',
  status                   text    NOT NULL DEFAULT 'pending',
  last_reviewed_at         timestamp with time zone,
  created_at               timestamp with time zone NOT NULL DEFAULT now(),
  updated_at               timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT doctrine_anchors_co_occurrence_count_chk
    CHECK (co_occurrence_count >= 0),
  CONSTRAINT doctrine_anchors_confidence_tier_chk
    CHECK (confidence_tier IN ('uncurated','llm_classified','expert_reviewed')),
  CONSTRAINT doctrine_anchors_status_chk
    CHECK (status IN ('pending','promoted','dismissed'))
);

CREATE TABLE public.doctrine_case_classifications (
  id                              serial PRIMARY KEY,
  anchor_id                       integer NOT NULL REFERENCES public.doctrine_anchors(id) ON DELETE CASCADE,
  case_id                         bigint  NOT NULL REFERENCES public.cases(id)            ON DELETE CASCADE,
  classification                  text    NOT NULL,
  classification_justification    text,
  citation_count_in_result_set    integer,
  citation_distribution_z_score   numeric,
  methodology_version             integer NOT NULL DEFAULT 1,
  confidence_tier                 text    NOT NULL DEFAULT 'llm_classified',
  reviewed_by_user_id             text,
  classified_at                   timestamp with time zone NOT NULL DEFAULT now(),
  created_at                      timestamp with time zone NOT NULL DEFAULT now(),
  updated_at                      timestamp with time zone NOT NULL DEFAULT now(),
  analysis_run_id                 bigint REFERENCES public.analysis_runs(id),
  status                          text    NOT NULL DEFAULT 'active',
  CONSTRAINT doctrine_case_classifications_citation_count_chk
    CHECK (citation_count_in_result_set IS NULL OR citation_count_in_result_set >= 0),
  CONSTRAINT doctrine_case_classifications_classification_chk
    CHECK (classification IN (
      'foundational','defining_interpretive','limiting_distinguishing',
      'codification_anchor','extending_applying','overruling_abrogating'
    )),
  CONSTRAINT doctrine_case_classifications_confidence_tier_chk
    CHECK (confidence_tier IN ('llm_classified','expert_reviewed')),
  CONSTRAINT doctrine_case_classifications_methodology_version_chk
    CHECK (methodology_version >= 1),
  CONSTRAINT doctrine_case_classifications_status_check
    CHECK (status IN ('active','superseded','suppressed'))
);

CREATE TABLE public.keyword_relations (
  id                  serial PRIMARY KEY,
  anchor_id           integer NOT NULL REFERENCES public.doctrine_anchors(id) ON DELETE CASCADE,
  related_keyword_id  integer NOT NULL REFERENCES public.keywords(id)         ON DELETE RESTRICT,
  relation_type       text    NOT NULL,
  provenance          jsonb   NOT NULL DEFAULT '{}'::jsonb,
  confidence_tier     text    NOT NULL DEFAULT 'uncurated',
  methodology_version integer NOT NULL DEFAULT 1,
  created_at          timestamp with time zone NOT NULL DEFAULT now(),
  updated_at          timestamp with time zone NOT NULL DEFAULT now(),
  CONSTRAINT keyword_relations_confidence_tier_chk
    CHECK (confidence_tier IN ('uncurated','llm_classified','expert_reviewed')),
  CONSTRAINT keyword_relations_methodology_version_chk
    CHECK (methodology_version >= 1),
  CONSTRAINT keyword_relations_relation_type_chk
    CHECK (relation_type IN (
      'predecessor_term','modern_label','codification_anchor',
      'doctrinal_test','related_concept','displaced_doctrine'
    ))
);

-- ---------------------------------------------------------------------------
-- appellate_history_*
-- ---------------------------------------------------------------------------
-- D-2 note: appellate_history_connections already carries
-- source_case_source_ref + target_case_source_ref so the table is
-- cross-source-aware as-is; only citing_case_id needs remap.

CREATE TABLE public.appellate_history_case_status (
  case_id              bigint PRIMARY KEY REFERENCES public.cases(id) ON DELETE CASCADE,
  completeness         text NOT NULL,
  pending_queue_count  integer NOT NULL DEFAULT 0,
  last_extracted_at    timestamp with time zone,
  parser_version       integer NOT NULL,
  last_opinion_id      bigint,
  CONSTRAINT appellate_history_case_status_completeness_check
    CHECK (completeness IN ('complete','incomplete_pending_llm','incomplete_external_source_missing'))
);

CREATE TABLE public.appellate_history_connections (
  id                       bigserial PRIMARY KEY,
  citing_case_id           bigint NOT NULL REFERENCES public.cases(id) ON DELETE CASCADE,
  source_case_source_ref   text NOT NULL,
  source_case_id           bigint NOT NULL,
  target_case_source_ref   text NOT NULL,
  target_case_id           bigint NOT NULL,
  relation                 text NOT NULL,
  relation_kind            text NOT NULL,
  raw_citation_string      text NOT NULL,
  source_parallels         jsonb,
  target_parallels         jsonb,
  extraction_method        text NOT NULL DEFAULT 'rule_based',
  parser_version           integer NOT NULL,
  llm_extraction_version   integer,
  extracted_at             timestamp with time zone NOT NULL DEFAULT now(),
  citing_opinion_id        bigint,
  CONSTRAINT appellate_history_connections_extraction_method_check
    CHECK (extraction_method IN ('rule_based','llm_extracted')),
  CONSTRAINT appellate_history_connections_relation_kind_check
    CHECK (relation_kind IN ('subsequent_history','treatment')),
  CONSTRAINT chk_ahc_llm_version CHECK (
    (extraction_method = 'rule_based'   AND llm_extraction_version IS NULL)
    OR
    (extraction_method = 'llm_extracted' AND llm_extraction_version IS NOT NULL)
  ),
  CONSTRAINT chk_ahc_relation CHECK (relation IN (
    'source_reversed_target','source_reversed_by_target',
    'source_affirmed_target','source_affirmed_by_target',
    'source_vacated_target','source_vacated_by_target',
    'source_modified_target','source_modified_by_target',
    'source_remanded_target','source_remanded_by_target',
    'source_cert_denied_by_target','source_cert_granted_by_target','source_cert_dismissed_by_target',
    'source_leave_denied_by_target','source_leave_granted_by_target','source_leave_dismissed_by_target',
    'source_reargument_denied_by_target','source_reargument_granted_by_target','source_reargument_dismissed_by_target',
    'source_rehearing_denied_by_target','source_rehearing_granted_by_target',
    'source_appeal_dismissed_by_target',
    'source_overruled_target','source_overruled_by_target',
    'source_abrogated_target','source_abrogated_by_target',
    'source_superseded_by_target'
  ))
);

CREATE TABLE public.appellate_history_resolution_queue (
  id                      bigserial PRIMARY KEY,
  citing_case_id          bigint NOT NULL REFERENCES public.cases(id) ON DELETE CASCADE,
  raw_window              text NOT NULL,
  trigger_text            text,
  reason                  text NOT NULL,
  parser_version          integer NOT NULL,
  status                  text NOT NULL DEFAULT 'pending',
  llm_extraction_version  integer,
  enqueued_at             timestamp with time zone NOT NULL DEFAULT now(),
  resolved_at             timestamp with time zone,
  notes                   text,
  citing_opinion_id       bigint,
  CONSTRAINT appellate_history_resolution_queue_reason_check CHECK (reason IN (
    'chain_orphan','pre_bluebook','no_clear_structure','unmapped_trigger',
    'external_source_missing','rule_based_low_confidence'
  )),
  CONSTRAINT appellate_history_resolution_queue_status_check CHECK (status IN (
    'pending','resolved_by_llm','resolved_by_external_import','unresolvable'
  ))
);

-- ---------------------------------------------------------------------------
-- batch_jobs + batch_opinion_requests
-- ---------------------------------------------------------------------------

CREATE TABLE public.batch_jobs (
  id                  serial PRIMARY KEY,
  batch_id            varchar(255) NOT NULL UNIQUE,
  status              varchar(50)  NOT NULL DEFAULT 'submitted',
  created_at          timestamp with time zone NOT NULL DEFAULT now(),
  submitted_at        timestamp with time zone,
  completed_at        timestamp with time zone,
  total_requests      integer NOT NULL,
  completed_requests  integer DEFAULT 0,
  failed_requests     integer DEFAULT 0,
  database_name       varchar(255) NOT NULL,
  limit_count         integer,
  resume_mode         boolean NOT NULL DEFAULT true,
  dry_run             boolean NOT NULL DEFAULT false,
  input_file_id       varchar(255),
  output_file_id      varchar(255),
  error_file_id       varchar(255),
  error_message       text,
  description         text,
  created_by          varchar(255) DEFAULT 'system',
  source_ref          text NOT NULL CHECK (source_ref IN ('ny_supreme','ny_appellate','ny_trial'))
);

CREATE TABLE public.batch_opinion_requests (
  id                              serial PRIMARY KEY,
  batch_job_id                    integer NOT NULL REFERENCES public.batch_jobs(id) ON DELETE CASCADE,
  opinion_id                      integer NOT NULL,
  custom_id                       varchar(255) NOT NULL,
  status                          varchar(50) NOT NULL DEFAULT 'pending',
  processed_at                    timestamp with time zone,
  error_code                      varchar(50),
  error_message                   text,
  extraction_successful           boolean,
  validation_successful           boolean,
  upsert_successful               boolean,
  keywords_extracted              integer DEFAULT 0,
  holdings_extracted              integer DEFAULT 0,
  negative_treatments_extracted   integer DEFAULT 0,
  UNIQUE (batch_job_id, opinion_id)
);
