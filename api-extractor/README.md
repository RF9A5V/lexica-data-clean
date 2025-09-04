NY Senate Laws API Extractor
================================

Purpose
 - Fetch all NY Senate Open Legislation “laws” and transform them into NDJSON compatible with the legislative-extractor loader.
 - Produce a resumable NDJSON export with retry and rate limiting. Database loading is optional and handled by the existing loader.

Quick Start
 - Create `.env` (see `.env.example`).
 - Fetch all laws (per-law files): `node src/index.js fetch --all-laws`.
 - Fetch specific law type: `node src/index.js fetch --law-type <TYPE>` (validated against the API).
 - Fetch specific law IDs: `node src/index.js fetch --law ABC --law DEF`.
 - To resume after an interruption, re-run the same command; checkpointing picks up where it left off.

Environment
 - `NYSENATE_API_KEY`: API key for Open Legislation API.
 - Optional tuning:
   - `NYSENATE_BASE_URL` (default: https://legislation.nysenate.gov/api/3)
   - `NYSENATE_RATE_RPS` (default: 3 requests/second)
   - `NYSENATE_RETRY_MAX` (default: 5)

Output
 - For each law, NDJSON at `data/nysenate/<lawid>/nysenate.<lawid>.ndjson` and checkpoint at `data/nysenate/<lawid>/nysenate.<lawid>.checkpoint.json`.

CLI
 - `node src/index.js fetch [--all-laws | --law-type TYPE | --law <LAWID> ...] [--out-dir DIR] [--ck-dir DIR] [--dry-run]`
 - `node src/index.js validate --file data/nysenate.ndjson`

Notes
 - Endpoint shapes are driven by `configs/nysenate.json` to keep API specifics configurable.
 - The NDJSON format aligns with the loader expectations: structural `unit` records followed by optional `unit_text_versions` and `citations` records.
