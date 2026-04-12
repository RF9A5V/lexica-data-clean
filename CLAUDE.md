# co-data — Data Ingestion Pipelines

## What This Is

Offline CLI tools for loading and enriching legal data into the PostgreSQL databases that `co-collection` reads. **None of these are runtime services** — they run standalone to bootstrap or update the legal databases.

Changes here affect what `co-collection` can serve. Schema changes must be coordinated with `co-collection/src/`.

## Tools Overview

### `caselaw-extractor/`
Scrapes ZIP archives from static.case.law, extracts case JSON, loads opinions into PostgreSQL. Supports NY, CA, and federal jurisdictions.

**Target DB:** `postgres-collection` (port 5433, `ny_reporter_dev` in dev).

### `legislative-extractor/`
Downloads and parses legislative XML for NY laws and regulations (RCNY, NYCRR, NY Consolidated Laws). Loads hierarchical structure into PostgreSQL.

**Also registers sources in `co-collection`'s app DB** — running this tool affects what sources `co-collection` exposes at runtime.

**Target DB:** `postgres-collection` + `co-collection` app DB.

### `api-extractor/`
Fetches NY Senate Open Legislation API (`/laws` endpoint). Outputs NDJSON files consumed by `legislative-extractor`'s loader.

**Output:** NDJSON files → piped into `legislative-extractor`.

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
