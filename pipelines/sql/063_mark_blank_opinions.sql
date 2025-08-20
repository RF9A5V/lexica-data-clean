-- Phase 1: Mark empty/blank opinion texts as valueless
-- Rationale: Entries with NULL or whitespace-only text are non-substantive.
-- Idempotent: only updates rows not already flagged as valueless.

UPDATE opinions
SET is_valueless = true,
    valueless_reason = COALESCE(valueless_reason, 'blank opinion text')
WHERE is_valueless IS DISTINCT FROM true
  AND (text IS NULL OR text ~ '^[[:space:]]*$');
