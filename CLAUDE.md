# co-data — Data Ingestion Pipelines

## What This Is

Offline CLI tools for loading and enriching legal data into the PostgreSQL databases that `co-collection` reads. **None of these are runtime services** — they run standalone to bootstrap or update the legal databases.

Changes here affect what `co-collection` can serve. Schema changes must be coordinated with `co-collection/src/`.

## Tools Overview

### `bound-volume-extractor/` and `slip-op-extractor/`

Both follow the same `in/` → `out/<stem>/cases.json` convention. Per-target
parsed payloads land in `out/<stem>/cases.json` (cases.json is the
per-target upload unit for bound-volume; for slip-op it's a single
case per file that gets merged downstream).

`slip-op-extractor` segments its `in/` by input type to keep the three
parsers safely separated:
- `in/index/<MM_YY_court>.html` — LRB monthly index pages, consumed by
  `scripts/resolve_index.js` (auto-attaches slip-op cites to existing cases)
- `in/html/<file>.html` — slip-op HTML pages, consumed by `main.js`
- `in/pdf/<file>.pdf` — slip-op PDFs, consumed by `main.js`

`bound-volume-extractor` keeps a flat `in/` (PDFs only).

**Slip-op-extractor only** has a `compile` step:

```bash
cd co-data/slip-op-extractor
node main.js parse-all                 # in/html/*.html + in/pdf/*.pdf → out/<stem>/cases.json
node main.js compile                   # merges per-stem outputs → compiled/<source>-<window>.json
node upload.js upload-all --target=staging --wait
node scripts/resolve_index.js          # in/index/*.html → out/index-resolution/resolved-<source>-<ts>.json
```

The slip-op compile step groups per-stem outputs by `target_source_db` and
emits one merged payload per source per window — one HTTP POST applies many
slip-ops at once. Bound-volume cases.json files are already the upload unit
(hundreds of cases per volume), so no compile step is needed there.

Both extractors emit schema_version 0.3 with rich `citations[]`
(`{cite, citation_type, curie?}` entries; `citation_type` is one of
`official | parallel | regional | public_domain | slip_op | wl | lexis`).
Bound-volume payloads carry the top-level `volume{}` block (always present
— cases come from a reporter); slip-op payloads omit it. The upload
runners (`upload.js` in each directory) accept
`--target=local|staging|prod` for routing.

Server-side dedup (Bucket A) handles the slip-op-then-bound-volume case
automatically — when a bound-volume parse hits a case that's already in the
DB as a slip-op, the new bound-volume citations attach to the existing
case_id rather than inserting a duplicate. Fuzzy matches that need human
review land in `case_match_candidates`; resolve them at
`/admin/case-matches`.

### `caselaw-extractor/`
Scrapes ZIP archives from static.case.law, extracts case JSON, loads opinions into PostgreSQL. Supports NY, CA, and federal jurisdictions.

**Target DB:** `postgres-collection` (port 5433, `ny_reporter_dev` in dev).

### `legislative-extractor/`
Downloads and parses legislative XML for NY laws and regulations (RCNY, NYCRR, NY Consolidated Laws). Loads hierarchical structure into PostgreSQL.

**Also registers sources in `co-collection`'s app DB** — running this tool affects what sources `co-collection` exposes at runtime.

**Target DB:** `postgres-collection` + `co-collection` app DB.

### `nys-extractor/`
End-to-end pipeline for NYS consolidated and unconsolidated laws via the NYSenate Open Legislation API.

**3 stages:** `fetch` (API → on-disk JSON cache, resumable) → `transform` (cache → NDJSON with `unit` + `alias` records) → `load` (NDJSON → `nys_legislative` Postgres).

CURIEs are aligned with the case-side extractor's output (`nys:<kebab-law-name>-<section>`). Each section emits a primary `units.canonical_id` plus extra forms in a separate `unit_aliases` table — both are UNIONed by `co-collection`'s legislative resolver/seeder.

**Target DB:** standalone `nys_legislative` on port 5432 (system Postgres).

### `legislative-enrichment/`
Enriches legislative sections with AI-generated taxonomy via OpenAI. Extracts: `field_of_law`, `doctrines`, `distinguishing_factors`, `digest`. Stores in `unit_enrichments`, `unit_keywords` tables.

**Requires OpenAI API key. Runs against live DB — use `--dry-run` or limit flags before bulk runs.**

### `sources/ny_case/`
Scripts for NY case data: keyword extraction, embedding generation, descriptor classification, keyword tier assignment.

### `sources/federal/`
Federal case embedding and diagnostic scripts.

### `sources/ny_reports/`
Scripts specific to NY Reports dataset.

### `xml-extractor/`
XML parsing utility. Has a Python `server.py` component alongside the Node.js tooling — both must be running for this tool to work.

### `pipelines/`
Earlier/alternative pipeline tooling. No README — inspect scripts before running.

### `utils/`
Shared utility modules used across the other tools.

### `scripts/`
Cost estimation (`estimateCost/`) and validation (`validate/`) utilities.

## General Notes

- All tools are Node.js CLI scripts unless noted otherwise.
- These tools write to the collection database. Always verify the target DB connection string before running.
- Large enrichment/embedding runs consume significant OpenAI API credits — estimate costs with `scripts/estimateCost/` before running.
- The `legislative-extractor` is the only tool that also modifies the `co-collection` app DB (source registration). Be aware of this side effect.
