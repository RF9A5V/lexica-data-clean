-- Create tables for citation corrections and case notes

BEGIN;

CREATE TABLE IF NOT EXISTS citation_corrections (
  id BIGSERIAL PRIMARY KEY,
  citation_id INTEGER REFERENCES citations(id) ON DELETE CASCADE,
  case_id BIGINT REFERENCES cases(id) ON DELETE CASCADE,
  observed_cite TEXT NOT NULL,
  observed_reporter TEXT,
  corrected_cite TEXT NOT NULL,
  expected_reporter TEXT,
  source_id TEXT,
  file_volume INTEGER,
  reason TEXT,
  confidence TEXT,
  applied BOOLEAN DEFAULT FALSE,
  applied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Ensure only one applied correction per citation
CREATE UNIQUE INDEX IF NOT EXISTS uq_citation_corrections_applied
  ON citation_corrections(citation_id)
  WHERE applied = TRUE;

CREATE TABLE IF NOT EXISTS case_notes (
  id BIGSERIAL PRIMARY KEY,
  case_id BIGINT REFERENCES cases(id) ON DELETE CASCADE,
  note_type TEXT NOT NULL,
  note TEXT NOT NULL,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
