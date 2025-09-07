# Legislative Enrichment (Taxonomy-Based)

One-shot pipeline to enrich legislative sections (unit_type = `section`) with:
- Taxonomy extraction across three categories: `field_of_law`, `doctrines`, and `distinguishing_factors`
- A fifth-grade plain-language `digest`

Storage model:
- `unit_enrichments`: per-run output (raw JSON and digest)
- `unit_taxonomy`: full structured taxonomy JSON per unit+enrichment
- `enrichment_usage`: token usage and cost per run
- `keywords`: canonical keywords with `tier` classification (e.g., `field_of_law`, `doctrine`, `distinguishing_factor`)
- `unit_keywords`: M:N unit↔keyword with provenance

Notes:
- Fixed allowed-keyword lists are deprecated. The pipeline now upserts extracted terms dynamically into `keywords` with a `tier` value and links via `unit_keywords`.
- Existing `keywords.json` seeding remains available for compatibility but is optional.

The pipeline targets the legislative DB by default and is schema-aware:
- Uses `units.label`
- Pulls text from `unit_text_versions` via `current_section_text` view (`t.text_plain`)

## Setup

1. Copy `.env.example` to `.env` and set values
2. Install deps
```bash
cd co-data/legislative-enrichment
pnpm i || npm i || yarn
```
3. Initialize DB objects (tables + view)
```bash
npm run init-db
```
4. (Optional, deprecated) Seed keywords from a JSON file
```bash
# Provide your legacy set in keywords.json (optional)
npm run seed:keywords
```

## Run Enrichment (one-shot)
```bash
npm run enrich -- \
  --law PEN \
  --batch-size 50 \
  --concurrency 4 \
  --prompt-version statute_taxonomy_v1 \
  --keywords-set-version taxonomy_v1 \
  --model gpt-4o-mini \
  --replace
```
- `--replace` will reconcile `unit_keywords` for this run's version/model.
- Omit `--law` to process all laws.

## Export
Use `pg_dump` for SQL import, or run:
```bash
npm run export -- --format ndjson --law PEN > enrichments.ndjson
```
CSV export includes quick counts for taxonomy arrays.

## Notes
- Idempotency enforced by unique key on `(unit_id, text_version_id, prompt_version, model, keywords_set_version)`
- `unit_taxonomy` stores the structured taxonomy per enrichment
- `enrichment_usage` stores token usage and cost (by FK)
- `unit_keywords` stores the canonical unit↔keyword assignments with provenance

### Output Schema (LLM)
The model returns JSON matching:
```json
{
  "field_of_law": ["criminal law", "administrative law"],
  "doctrines": ["mens rea", "strict liability"],
  "distinguishing_factors": ["domestic violence context"],
  "digest": "Short plain-language summary (80–120 words)."
}
```
