# Case.law Data Extraction Tool

A comprehensive tool to extract, process, and load case law data from static.case.law ZIP archives into PostgreSQL.

## Features

- **Web Scraping**: Automatically finds all ZIP file URLs from static.case.law pages
- **Bulk Download**: Downloads ZIP files with concurrency control and retry logic
- **Polite Downloading**: Configurable delay between starting downloads (`downloadDelayMs`) to avoid hammering servers
- **Data Extraction**: Extracts JSON files from ZIP archives and processes case data
- **Database Loading**: Creates PostgreSQL schema and loads structured data
- **Metadata Ingestion**: Inserts config-provided metadata into the DB with uniqueness on `(type, value)`
- **Multi-jurisdiction**: Configurable for different jurisdictions (NY, CA, US, etc.)
- **Config-driven (multi-source)**: Run multiple sources/URLs in one execution via a JSON config file
- **Robust Error Handling**: Comprehensive logging and error recovery
- **Modular Design**: Separate scripts for each step, can be run independently

## Quick Start

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Configure database** (create `.env` file):
   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=caselaw
   DB_USER=postgres
   DB_PASSWORD=your_password
   ```

3. **Run the complete pipeline**:
   ```bash
   node main.js --jurisdiction=ny --verbose
   ```

## Usage

### Command Line Options

```bash
node main.js [options]

Options:
  --jurisdiction=<code>    Jurisdiction to process [default: ny]
                          Available: ny, ca, us
  --step=<step>           Step to run [default: all]
                          Steps: scrape, download, extract, load, all
  --verbose, -v           Verbose output
  --dry-run              Show what would be done without executing
  --config=<path>        Use JSON config with multiple sources (overrides --jurisdiction)
  --help, -h             Show help message
```

### Examples

```bash
# Process New York cases (complete pipeline)
node main.js --jurisdiction=ny --verbose

# Just scrape ZIP URLs for California
node main.js --jurisdiction=ca --step=scrape

# Download files only (after scraping)
node main.js --step=download --verbose

# Extract and process JSON files
node main.js --step=extract

# Load data into database
node main.js --step=load

# Dry run to see what would happen
node main.js --dry-run --verbose

# Config-driven run (multi-source)
node main.js --config=./config/caselaw.sources.json --step=all --verbose
```

### Config-driven multi-source runs

Provide a JSON config file to process multiple sources in one run. Example:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "caselaw",
    "user": "postgres",
    "password": "postgres"
  },
  "options": {
    "maxConcurrency": 5,
    "batchSize": 100,
    "retryAttempts": 3,
    "downloadDelayMs": 1000
  },
  "metadata": {
    "jurisdiction": "New York Appellate",
    "source": "case.law",
    "data_owner": "CourtListener",
    "notes": "Bulk download on 2025-08-12"
  },
  "sources": [
    {
      "id": "ny",
      "label": "New York",
      "scrape_urls": ["https://static.case.law/ny/"],
      "urls_file": "./data/processed/ny-zip-urls.json"
    },
    {
      "id": "ny3d",
      "label": "New York 3d",
      "scrape_urls": ["https://static.case.law/ny3d/"]
    },
    {
      "id": "custom",
      "zip_urls": [
        "https://static.case.law/ny/1234.zip",
        "https://static.case.law/ny/5678.zip"
      ]
    }
  ]
}
```

Notes:

- In config mode, the tool iterates each `sources[]` entry and runs the selected step(s) per source ID.
- If `scrape_urls` are provided, URLs are scraped and saved to `urls_file` (or a default `./data/processed/<id>-zip-urls.json`).
- For downloads, the order of precedence is: explicit `zip_urls` → `urls_file` (if JSON with `urls`) → default saved file for `<id>`.
- Database connection can be overridden via the `database` section; otherwise `.env` values are used.
- Config `metadata` (object of key/value pairs) is written to the `metadata` table during the load step. Uniqueness is enforced on `(type, value)`.

### Individual Scripts

You can also run individual components:

```bash
# Scrape ZIP URLs
node src/scraper.js --jurisdiction=ny --verbose

# Download ZIP files
node src/downloader.js --jurisdiction=ny --verbose

# Extract JSON data
node src/extractor.js --jurisdiction=ny --verbose

# Load into database
node src/database.js --jurisdiction=ny --verbose
```

## Data Structure

### Extracted Fields

From each case JSON file, the tool extracts:

- **Basic Info**: `name`, `name_abbreviation`, `decision_date`
- **Citations**: Array of case citations (e.g., "1 N.Y. 17")
- **Court**: `court.name`, `court.name_abbreviation`, `court.id`
- **Jurisdiction**: `jurisdiction.name_long`, `jurisdiction.name`, `jurisdiction.id`
- **References**: `cites_to` array of cited cases
- **Opinions**: `casebody.opinions` array with full text and metadata

### Database Schema

The tool creates four main tables:

1. **`cases`** - Main case information
2. **`citations`** - Case citations (1 N.Y. 17, etc.)
3. **`case_citations`** - Cases cited by this case
4. **`opinions`** - Opinion text and metadata

See `sql/schema.sql` for the complete schema with indexes and views.

Deprecated/Removed:

- Opinion segmentation, statement extraction, doctrinal tests, keyword taxonomy population are not part of this extractor. Corresponding tables/indexes (e.g., `opinion_segments`, `opinion_statements`, `statement_concepts`, `opinion_outcomes`, `doctrinal_tests`) are pruned from the schema.

## Project Structure

```
caselaw-extractor/
├── src/
│   ├── config.js          # Configuration and jurisdiction settings
│   ├── scraper.js         # Web scraping for ZIP URLs
│   ├── downloader.js      # ZIP file downloading
│   ├── extractor.js       # ZIP extraction and JSON processing
│   └── database.js        # PostgreSQL operations
├── data/
│   ├── zips/              # Downloaded ZIP files (by jurisdiction)
│   ├── extracted/         # Extracted JSON files
│   └── processed/         # Combined JSON datasets
├── sql/
│   └── schema.sql         # Database schema
├── main.js                # Main orchestrator script
├── package.json           # Dependencies
└── README.md              # This file
```

## Configuration

### Adding New Jurisdictions

Edit `src/config.js` to add new jurisdictions:

```javascript
export const JURISDICTIONS = {
  ny: {
    name: 'New York',
    url: 'https://static.case.law/ny/',
    abbreviation: 'NY'
  },
  ca: {
    name: 'California',
    url: 'https://static.case.law/ca/',
    abbreviation: 'CA'
  },
  // Add new jurisdictions here
};
```

### Environment Variables

Create a `.env` file for database configuration:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=caselaw
DB_USER=postgres
DB_PASSWORD=your_password
NODE_ENV=development
```

## Performance Tuning

### Concurrency Settings

Adjust in `src/config.js`:

```javascript
export const EXTRACTION_CONFIG = {
  batchSize: 100,           // Database insert batch size
  maxConcurrency: 5,        // Max concurrent downloads/extractions
  retryAttempts: 3,         // Retry failed operations
  downloadDelayMs: 1000     // Delay between starting downloads (ms) for server politeness
};
```

### Database Optimization

- The schema includes indexes for common queries
- Use `EXPLAIN ANALYZE` to optimize your specific queries
- Consider partitioning large tables by date or jurisdiction

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase timeout in scraper config
2. **Memory Issues**: Reduce batch size and concurrency
3. **Database Errors**: Check connection settings and permissions
4. **ZIP Extraction Fails**: Verify ZIP file integrity

### Debugging

Run with verbose output and development mode:

```bash
NODE_ENV=development node main.js --verbose --jurisdiction=ny
```

### Logs and Monitoring

- All operations include progress tracking
- Errors are logged with context
- Use `--dry-run` to test without side effects

## Sample Queries

After loading data, try these PostgreSQL queries:

```sql
-- Count cases by court
SELECT court_name, COUNT(*) as case_count 
FROM cases 
WHERE court_name IS NOT NULL 
GROUP BY court_name 
ORDER BY case_count DESC;

-- Find cases citing a specific case
SELECT c.name, c.decision_date 
FROM cases c 
JOIN case_citations cc ON c.id = cc.case_id 
WHERE cc.cited_case ILIKE '%wend%';

-- Full-text search in case names
SELECT name, decision_date, court_name 
FROM cases 
WHERE to_tsvector('english', name) @@ plainto_tsquery('english', 'contract');

-- Get case with all opinions
SELECT c.name, o.author, o.opinion_type, length(o.text) as text_length
FROM cases c
JOIN opinions o ON c.id = o.case_id
WHERE c.id = 2004070;
```

## Contributing

1. Follow the existing code structure
2. Add error handling for new features
3. Update configuration for new jurisdictions
4. Test with `--dry-run` before running on large datasets

## License

MIT License - see LICENSE file for details.
