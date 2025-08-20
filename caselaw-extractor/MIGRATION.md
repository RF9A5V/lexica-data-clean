# Original ID Migration Guide

This guide explains how to migrate existing databases to support dual-source citation mapping by adding `original_id` columns and backfilling data from extracted JSON files.

## Overview

The migration adds two new columns:
- `cases.original_id` - Stores the original case.law ID from extracted JSON data
- `case_citations.cited_case_ids` - Stores arrays of original case IDs referenced by citations

These columns enable precise cross-database citation resolution and accurate citation counts.

## Migration Steps

### 1. Schema Migration (Required)

Apply the schema changes to add the new columns and indexes:

```bash
# Apply schema migration only
node src/migrate.js
```

This is safe to run multiple times and will not affect existing data.

### 2. Data Backfill (Optional)

Backfill original IDs for existing cases by matching case names from extracted JSON data:

#### Config-Based Multi-Source Backfill (Recommended)
```bash
# Use config file to process multiple sources
node src/migrate.js --config configs/ny_appellate.json --backfill --verbose

# Dry run with config file
node src/migrate.js -c configs/ny_appellate.json -b -d -v
```

#### Legacy Single-Jurisdiction Backfill
```bash
# Backfill for a specific jurisdiction
node src/migrate.js --jurisdiction ny_appellate --backfill --verbose

# Dry run to see what would be backfilled
node src/migrate.js -j ny_appellate -b -d -v
```

## Command Line Options

- `-c, --config <file>` - Config file to use (supports multiple sources)
- `-j, --jurisdiction <name>` - Jurisdiction to migrate (legacy single-source mode)
- `-b, --backfill` - Backfill original IDs for existing cases
- `-d, --dry-run` - Show what would be done without making changes
- `-v, --verbose` - Show detailed output
- `-h, --help` - Show help message

## Config File Support

The migration script supports the same config file format used by the main data loading process. Config files enable processing multiple sources in a single migration run.

### Config File Structure
```json
{
  "database": { ... },
  "sources": [
    {
      "id": "ad",
      "label": "New York Appellate Division Reports, 1st Edition",
      "scrape_urls": ["https://static.case.law/ad/"]
    },
    {
      "id": "ad2d", 
      "label": "New York Appellate Division Reports, 2nd Edition",
      "scrape_urls": ["https://static.case.law/ad2d/"]
    }
  ]
}
```

### Available Config Files

- `configs/ny_appellate.json` - New York Appellate Division (ad, ad2d, ad3d)
- `configs/ny_coa.json` - New York Court of Appeals
- `configs/ny_trial.json` - New York Trial Courts

### Legacy Jurisdictions

- `ny_appellate` - New York Appellate Division
- `ny_coa` - New York Court of Appeals
- `ny_trial` - New York Trial Courts

## Migration Safety

The migration is designed to be safe:

1. **Idempotent**: Safe to run multiple times
2. **Non-destructive**: Only adds columns, never removes data
3. **Name-based matching**: Uses case names (not citations) to avoid misattribution
4. **Skip existing**: Won't overwrite existing original IDs
5. **Transactional**: All operations are wrapped in database transactions

## Data Flow

### Before Migration
```
cases: [id, name, court_name, ...]
case_citations: [case_id, cited_case, reporter, ...]
```

### After Migration
```
cases: [id, name, court_name, ..., original_id]
case_citations: [case_id, cited_case, reporter, ..., cited_case_ids]
```

### New Data Import
When importing new cases from JSON:
- `cases.original_id` = `caseData.id` (from JSON)
- `case_citations.cited_case_ids` = `citedCase.case_ids` (from JSON)

## Cross-Database Citation Resolution

With original IDs stored, the collection server can:

1. **Resolve citations accurately**: Match citations using original case IDs
2. **Handle cross-database references**: Link cases across different sources
3. **Provide accurate counts**: Eliminate citation count misattributions
4. **Enable precise queries**: Support dual-source citation API calls

## Troubleshooting

### No JSON files found
```
⚠️  No processed JSON files found for jurisdiction ny_appellate
```
Ensure the processed JSON files exist in `data/processed/` directory.

### Database connection issues
```
❌ Migration failed: connection refused
```
Check database configuration in `.env` file and ensure PostgreSQL is running.

### Permission issues
```
❌ Error: permission denied
```
Ensure the database user has CREATE and ALTER privileges.

## Verification

After migration, verify the changes:

```sql
-- Check new columns exist
\d cases
\d case_citations

-- Check indexes were created
\di idx_cases_original_id
\di idx_case_citations_cited_case_ids

-- Verify data was backfilled
SELECT COUNT(*) FROM cases WHERE original_id IS NOT NULL;
SELECT COUNT(*) FROM case_citations WHERE cited_case_ids IS NOT NULL;
```

## Rollback

If needed, the new columns can be removed:

```sql
-- Remove new columns (CAUTION: This will lose data)
ALTER TABLE cases DROP COLUMN IF EXISTS original_id;
ALTER TABLE case_citations DROP COLUMN IF EXISTS cited_case_ids;

-- Remove indexes
DROP INDEX IF EXISTS idx_cases_original_id;
DROP INDEX IF EXISTS idx_case_citations_cited_case_ids;
```

## Integration with Collection Server

The collection server's `citation_linkages` table will leverage these original IDs:

```sql
-- citation_linkages table structure
CREATE TABLE citation_linkages (
  citing_source_ref TEXT NOT NULL,
  citing_case_id BIGINT NOT NULL,  -- Maps to cases.original_id
  cited_source_ref TEXT,
  cited_case_id BIGINT,            -- Maps to cases.original_id
  ...
);
```

This enables the dual-source citation API to accurately resolve citations across databases.
