-- Register the NYS legislative source in the co-collection app DB.
-- Run against the `collections` DB on port 5432.
--   PGPASSWORD=dev psql -h localhost -U dev -d collections -f sql/register_source.sql

INSERT INTO sources (
  name,
  reference,
  database_url,
  enabled,
  instrument_kind,
  code_key,
  jurisdiction_code,
  jurisdiction_scope
) VALUES (
  'New York State Consolidated and Unconsolidated Laws',
  'nys',
  'postgres://dev:dev@localhost:5432/nys_legislative',
  TRUE,
  'statute_code',
  'nys',
  'NY',
  'state'
)
ON CONFLICT (reference) DO UPDATE SET
  name = EXCLUDED.name,
  database_url = EXCLUDED.database_url,
  enabled = EXCLUDED.enabled,
  instrument_kind = EXCLUDED.instrument_kind,
  code_key = EXCLUDED.code_key,
  jurisdiction_code = EXCLUDED.jurisdiction_code,
  jurisdiction_scope = EXCLUDED.jurisdiction_scope,
  updated_at = NOW();

SELECT id, reference, instrument_kind, code_key, name, enabled, database_url
FROM sources
WHERE reference = 'nys';
