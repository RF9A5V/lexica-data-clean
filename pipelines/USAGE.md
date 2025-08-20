# NY Reporter Keywording Pipeline — Usage Guide

This document explains how to install, configure, and run the unified single-pass keywording pipeline for the NY Court of Appeals corpus, including concurrency, progress bars, and legacy tooling notes.

## Contents
- Quick start
- Installation
- Environment variables
- Database migrations
- Running the pipeline
- Flags reference
- Concurrency and progress bars
- Troubleshooting
- Performance tips

## Quick start
1) Install dependencies:
```
npm i
```
2) Set your environment:
```
export OPENAI_API_KEY=...        # Required for LLM mode
export DATABASE_URL=postgres://dev:dev@localhost:5432/ny_reporter
```
3) Run a small batch with LLM and progress bars:
```
node bin/run_unified.js --db $DATABASE_URL --limit 50 --concurrency 8 --debug
```
4) Doctrines/tests are included automatically in unified extraction; no separate triage step.

## Installation
- This pipeline is Node.js-based and lives in `pipelines/ny_reporter_keywording/`.
- Dependencies are declared in `package.json` (notably `pg`, `ajv`, `openai`, and `cli-progress`).
- Install with `npm i` from the pipeline directory.

## Environment variables
- OPENAI_API_KEY: required when using the LLM (omit or use `--no-llm` to bypass)
- OPENAI_MODEL: optional override of the model used by `src/llm.js`
- OPENAI_MAX_RETRIES: retry count for transient/429/5xx (default 4)
- OPENAI_RETRY_BASE_MS: base backoff in ms (default 500)
- DATABASE_URL: Postgres URL (can also be provided via `--db`)

## Database migrations
- Apply SQL in `sql/` with your preferred tool. Files are idempotent (`IF NOT EXISTS`, constraint guards).
- Notable steps:
  - 061_mark_valueless_opinions.sql — flags valueless opinions by conservative text patterns
  - 062_mark_short_dispo_orders.sql — flags very short order-only opinions
  - pg_trgm enablement and indexes for vectorless fuzzy matching
- The pipeline uses consolidated tiers in `keywords` (e.g., `field_of_law`, `doctrine`). Unified extraction writes to `opinion_keywords` with `category` values `field_of_law`, `procedural_posture`, `case_outcome`, `distinguishing_factor`, `doctrine`, and `doctrinal_test`. No separate doctrine-specific tables are required.

## Running the pipeline
Unified single-pass extraction via `bin/run_unified.js`:
- Extracts: `field_of_law`, `procedural_posture`, `case_outcome`, `distinguishing_factors`, `doctrines`, and `doctrinal_tests`.

Examples:
```
# Small LLM run with parallelism and debug (unified)
node bin/run_unified.js --db $DATABASE_URL --limit 100 --concurrency 8 --debug

# Dry-run (no writes), no LLM (uses samples)
node bin/run_unified.js --db $DATABASE_URL --limit 10 --dry-run --no-llm --concurrency 8

# Target specific opinions by ID
node bin/run_unified.js --db $DATABASE_URL --case-ids 123,456,789 --concurrency 6
```

## Flags reference
- --db <url>: Postgres connection string (or set DATABASE_URL)
- --limit <N>: maximum opinions to process (after prefiltering valueless)
- --case-ids <id1,id2,...>: process only specific opinions
- --no-llm: bypass LLM; use sample payloads from `samples/`
- --samples-dir <dir>: custom samples directory
- --dry-run: do not write DB changes (transactions are rolled back)
- --concurrency <N>: parallel LLM request fan-out (DB writes remain sequential)
- --debug: verbose per-opinion logging

## Concurrency and progress bars
- Concurrency applies to LLM calls only; DB writes use a separate, limited concurrency.
- Progress bars use `cli-progress`:
  - Extraction bar: total = number of selected opinions
  - DB writes bar: total = number of selected opinions
- In non-TTY environments, only final counts may be visible.

## Reviewer CLI (Legacy)
Legacy doctrine candidate triage tooling is retained for historical workflows. The unified runner extracts doctrines/tests directly as standard keywords; no separate triage is required.

List pending candidates:
```
node bin/review_candidates.js list --db $DATABASE_URL --status pending --limit 20 --debug
```
Accept or reject:
```
node bin/review_candidates.js accept --db $DATABASE_URL --id <candidate_id>
node bin/review_candidates.js reject --db $DATABASE_URL --id <candidate_id>
```
Alias or create:
```
node bin/review_candidates.js alias --db $DATABASE_URL --id <candidate_id> --to "Canonical Doctrine Name"
node bin/review_candidates.js create --db $DATABASE_URL --name "New Doctrine Name" --field "Field of Law"
```
Notes:
- The CLI leverages `src/llm.js` helpers and honors the same resolution rules used by the runner.
- Use `--dry-run` to preview actions without committing.

## Troubleshooting
- Rate limits / 429: lower `--concurrency`, increase `OPENAI_MAX_RETRIES`, or raise `OPENAI_RETRY_BASE_MS`.
- Model errors: set `OPENAI_MODEL` to a model available to your account.
- Punycode deprecation warning: benign Node.js warning; safe to ignore.
- No progress updates: ensure you’re in a TTY. In CI, rely on final summaries and logs.
- pg_trgm not found: ensure the extension is installed and enabled.
- Valueless opinions: these are pre-filtered; runner also flags blank text at runtime.

## Performance tips
- Start with `--concurrency 6-8`. Increase gradually while monitoring rate limits.
- Use `--limit` to iterate quickly when adjusting prompts or taxonomy.
- Keep `--debug` off for large runs to reduce log noise and maximize throughput.
- Good field coverage improves overall extraction quality for doctrines/tests as well.

---
For deeper context, see:
- `README.md` for phase overview
- `schemas/` and `prompts/` for output formats and LLM instructions
- `src/llm.js` for model selection and retry/backoff logic
- `src/upsert.js` for unified upsert details
