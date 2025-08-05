# Keyword Descriptor Migration

This directory contains scripts to normalize the `keyword_descriptors` table structure by moving duplicate descriptor text to a separate `descriptors` table and adding embeddings support.

## Problem

The current `keyword_descriptors` table stores descriptor text directly, leading to:
- **Data duplication**: 305,598 total rows but only 285,166 unique descriptors
- **Storage inefficiency**: Duplicate text stored multiple times
- **No embeddings**: Cannot perform semantic similarity searches on descriptors

## Solution

Create a normalized structure:
1. **`descriptors` table**: Stores unique descriptor text with embeddings
2. **`keyword_descriptors` table**: Links keywords to descriptors via foreign keys

## Migration Options

### Option 1: Full Migration with Embeddings (Recommended)

```bash
node normalizeKeywordDescriptors.js
```

**Features:**
- ✅ Creates normalized table structure
- ✅ Generates embeddings using local LegalBERT server for all descriptors
- ✅ Comprehensive progress tracking and error handling
- ✅ Creates backup tables for safety
- ✅ Validates migration results

**Requirements:**
- Local embedding server running at `http://izanagi:8000`
- No API costs (uses local server)

### Option 2: Schema-Only Migration (SQL)

```bash
psql postgresql://localhost/ny_court_of_appeals -f normalizeKeywordDescriptors.sql
```

**Features:**
- ✅ Creates normalized table structure
- ✅ Migrates data without embeddings
- ✅ Faster execution (no API calls)
- ⚠️ Manual cleanup steps required

## New Schema Structure

### Before Migration
```sql
keyword_descriptors (
    id SERIAL,
    keyword_id INTEGER,
    descriptor_text TEXT,  -- Duplicated data
    created_at TIMESTAMP
)
```

### After Migration
```sql
descriptors (
    id SERIAL PRIMARY KEY,
    descriptor_text TEXT UNIQUE,  -- Deduplicated
    embedding vector(768),        -- LegalBERT embeddings
    created_at TIMESTAMP
)

keyword_descriptors (
    id SERIAL,
    keyword_id INTEGER,
    descriptor_id INTEGER,        -- Foreign key reference
    created_at TIMESTAMP
)
```

## Usage Examples

### Query Keywords with Descriptors
```sql
SELECT k.keyword_text, d.descriptor_text
FROM keyword_descriptors kd
JOIN keywords k ON kd.keyword_id = k.id
JOIN descriptors d ON kd.descriptor_id = d.id
WHERE k.keyword_text = 'contract';
```

### Semantic Similarity Search
```sql
-- Find descriptors similar to a given text
SELECT descriptor_text, 
       1 - (embedding <=> $1) as similarity
FROM descriptors
WHERE embedding IS NOT NULL
ORDER BY embedding <=> $1
LIMIT 10;
```

### Get All Descriptors for a Keyword
```sql
SELECT ARRAY_AGG(d.descriptor_text) as descriptors
FROM keyword_descriptors kd
JOIN descriptors d ON kd.descriptor_id = d.id
WHERE kd.keyword_id = $1;
```

## Testing

Test the migration results:

```bash
node testKeywordDescriptorMigration.js
```

This will verify:
- ✅ Table structure is correct
- ✅ Data integrity is maintained
- ✅ Joins work properly
- ✅ No orphaned records exist
- ✅ Embeddings are generated (if applicable)
- ✅ Storage efficiency improvements

## Migration Steps (Full Process)

1. **Backup Current Data**
   ```sql
   CREATE TABLE keyword_descriptors_backup AS SELECT * FROM keyword_descriptors;
   ```

2. **Run Migration**
   ```bash
   node normalizeKeywordDescriptors.js
   ```

3. **Test Results**
   ```bash
   node testKeywordDescriptorMigration.js
   ```

4. **Verify Application Works**
   - Test your application with the new structure
   - Verify all queries work correctly

5. **Clean Up (Optional)**
   ```sql
   -- Only run after thorough testing
   ALTER TABLE keyword_descriptors DROP COLUMN descriptor_text;
   DROP TABLE keyword_descriptors_backup;
   ```

## Expected Results

- **Storage Reduction**: ~60-70% reduction in duplicate text storage
- **Unique Descriptors**: ~285,166 unique descriptors in `descriptors` table
- **Embeddings**: 768-dimensional LegalBERT embeddings for semantic similarity search
- **Performance**: Faster queries due to normalized structure
- **Flexibility**: Can add new descriptor features without affecting associations

## Rollback Instructions

If something goes wrong:

```sql
-- Drop new tables
DROP TABLE IF EXISTS descriptors CASCADE;

-- Restore original structure
ALTER TABLE keyword_descriptors DROP COLUMN IF EXISTS descriptor_id;

-- Restore from backup if needed
DROP TABLE keyword_descriptors;
ALTER TABLE keyword_descriptors_backup RENAME TO keyword_descriptors;
```

## Files

- `normalizeKeywordDescriptors.js` - Full migration with embeddings
- `normalizeKeywordDescriptors.sql` - Schema-only migration
- `testKeywordDescriptorMigration.js` - Test and validation script
- `KEYWORD_DESCRIPTOR_MIGRATION.md` - This documentation

## Support

If you encounter issues:
1. Check the backup tables exist before cleanup
2. Verify embedding server is running at `http://izanagi:8000`
3. Check server connectivity and response times
4. Test thoroughly before dropping old columns
