# Migration Guide: Adding Original ID Support

This guide covers migrating existing databases to support original case IDs and citation mappings for cross-database citation resolution.

## Overview

The migration adds two new columns:
- `original_id` to the `cases` table - stores the original case.law ID from source data
- `cited_case_ids` to the `case_citations` table - stores arrays of original case IDs for citation resolution

## For New Installations

**New installations automatically get the updated schema.** The `sql/schema.sql` file has been updated to include the new columns and indexes. Simply run:

```bash
# Create database with updated schema
psql -d your_database -f sql/schema.sql
```

## For Existing Databases

**Existing databases require a two-step migration process:**

### Step 1: Schema Migration

Add the new columns to your existing database:

```bash
# Test the schema migration (dry run)
node src/migrate.js --schema --config configs/your_config.json --dry-run

# Apply the schema migration
node src/migrate.js --schema --config configs/your_config.json --verbose
```

This will safely add:
- `original_id BIGINT` column to `cases` table
- `cited_case_ids BIGINT[]` column to `case_citations` table  
- Appropriate indexes for performance

### Step 2: Data Backfill

Populate the new columns with data from your extracted JSON files:

```bash
# Test the backfill (dry run)
node src/migrate.js --backfill --config configs/your_config.json --dry-run --verbose

# Run the actual backfill
node src/migrate.js --backfill --config configs/your_config.json --verbose
```

## Migration Script Options

### Required Options
- `--schema` - Run schema migration (add columns)
- `--backfill` - Populate original IDs from extracted data
- `--config FILE` - Config file with database connections and data paths

### Safety Options
- `--dry-run` - Show what would be done without making changes (**SAFE**)
- `--verbose` - Show detailed progress information
- `--sample` - Process only a small sample for testing

### Performance Options
- `--fast-commit` - Reduce commit/fsync overhead by setting `SET LOCAL synchronous_commit='off'` during batch transactions (ignored in `--dry-run`). Safe when runs are idempotent and re-runnable.

### Legacy Options
- `--jurisdiction NAME` - Single jurisdiction mode (legacy)

## Example Workflows

### Full Migration for New Config
```bash
# 1. Schema migration
node src/migrate.js --schema --config configs/ny_appellate.json

# 2. Data backfill  
node src/migrate.js --backfill --config configs/ny_appellate.json --verbose --fast-commit
```

### Safe Testing
```bash
# Test schema changes
node src/migrate.js --schema --dry-run --verbose

# Test backfill with sample data
node src/migrate.js --backfill --config configs/ny_appellate.json --sample --dry-run --verbose
```

### Legacy Single Jurisdiction
```bash
node src/migrate.js --jurisdiction ny_reporter --schema
node src/migrate.js --jurisdiction ny_reporter --backfill --verbose
```

## Data Processing

The migration script now processes extracted JSON files directly:
- **Efficient**: Processes `data/extracted/{source}/N.json` files directly
- **Memory optimized**: Uses in-memory batching instead of loading large consolidated files
- **Fast dry runs**: No database mutations during testing
- **Robust**: Individual file failures don't stop the entire process

## Safety Features

### Dry Run Mode
- **Completely safe** - always rolls back transactions
- **No database mutations** - only reads data to show what would change
- **Fast verification** - test migration logic without waiting for actual updates

### Idempotent Operations
- Safe to run multiple times
- Uses `IF NOT EXISTS` for schema changes
- Skips cases that already have `original_id` set
- Won't overwrite existing data

### Performance Tuning
- `--fast-commit` toggles `SET LOCAL synchronous_commit='off'` for each batch transaction to improve throughput for large backfills. 
- Effects: reduces fsyncs; in case of crash, only the last in-flight batch may be lost. Since the migration is re-runnable and idempotent, this is acceptable.
- Not applied when `--dry-run` is set.

### Error Handling
- Transactions ensure atomic operations
- Individual file processing errors don't stop migration
- Detailed logging for troubleshooting

## Verification

After migration, verify the results:

```sql
-- Check that original_id column was added
\d cases

-- Check that cited_case_ids column was added  
\d case_citations

-- Verify data was populated
SELECT COUNT(*) FROM cases WHERE original_id IS NOT NULL;
SELECT COUNT(*) FROM case_citations WHERE cited_case_ids IS NOT NULL;

-- Check indexes were created
\di idx_cases_original_id
\di idx_case_citations_cited_case_ids
```

## Rollback

If you need to rollback the schema changes:

```sql
-- Remove the new columns (THIS WILL DELETE DATA)
ALTER TABLE cases DROP COLUMN IF EXISTS original_id;
ALTER TABLE case_citations DROP COLUMN IF EXISTS cited_case_ids;

-- Remove the indexes
DROP INDEX IF EXISTS idx_cases_original_id;
DROP INDEX IF EXISTS idx_case_citations_cited_case_ids;
```

## Troubleshooting

### Common Issues

**"Column already exists" error**
- The schema migration is idempotent - this is expected if run multiple times
- Use `--dry-run` to verify what would be changed

**"No extracted files found"**
- Verify your config file has correct `dataPath` entries
- Check that `data/extracted/{source}/` directories exist
- Ensure JSON files are present in the extracted directories

**Memory issues with large datasets**
- The new migration processes files individually to avoid memory problems
- Use `--sample` flag for testing with large datasets

**Slow performance**
- Ensure database has adequate resources
- The migration uses batch processing (100 items per batch by default)
- Consider running during off-peak hours for production databases

### Getting Help

Run the migration script with `--help` for detailed usage information:

```bash
node src/migrate.js --help
```
