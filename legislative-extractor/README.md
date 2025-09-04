# Legislative Data Extractor

A comprehensive tool for extracting and processing legislative/statutory data from XML sources, designed to integrate seamlessly with the Curia Obscura legal data platform.

## Overview

This tool extracts legislative data from ZIP files containing XML documents, processes the hierarchical structure, and loads the data into PostgreSQL databases optimized for legal research. It supports multiple legislative sources (statutes, regulations, charters) and integrates with the existing co-collection system for unified search and citation resolution.

## Features

- **Multi-Source Support**: Extract from multiple legislative sources (RCNY, NYCRR, Consolidated Laws, etc.)
- **Hierarchical Processing**: Handle complex legislative hierarchies (Title → Chapter → Section → Subsection)
- **Citation Extraction**: Automatically extract and resolve cross-references between legislative documents
- **Database Integration**: Load processed data into per-source databases with optimized schema
- **co-collection Integration**: Register legislative sources for unified search and citation resolution
- **CURIE Support**: Generate Compact URIs for cross-database references

## Installation

```bash
cd co-data/legislative-extractor
npm install
```

## Configuration

Create configuration files in the `configs/` directory. See example configurations:

- `configs/rcny.json` - Rules of the City of New York
- `configs/nycrr.json` - New York Codes, Rules and Regulations
- `configs/ny_statutes.json` - New York Consolidated Laws

### Configuration Structure

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "legislative_db",
    "user": "dev",
    "password": "dev"
  },
  "metadata": {
    "jurisdiction": "New York City",
    "publisher": "American Legal Publishing",
    "current_edition_date": "2024-01-01"
  },
  "sources": [
    {
      "id": "rcny",
      "label": "Rules of the City of New York",
      "instrument_kind": "regulatory_code",
      "code_key": "rcny",
      "jurisdiction_scope": "city",
      "zip_urls": ["https://example.com/data.zip"],
      "staging_dir": "./data/staging/rcny",
      "ndjson_output": "./data/processed/rcny-statutes.ndjson"
    }
  ],
  "xml_structure": {
    "root_element": "Statute",
    "hierarchy": [
      {
        "level": "title",
        "xpath": "Title",
        "fields": ["number", "title"]
      },
      {
        "level": "section",
        "xpath": "Section",
        "fields": ["number", "title", "text"]
      }
    ]
  }
}
```

## Usage

### Full Pipeline
```bash
node src/main.js --config=configs/rcny.json --step=all --verbose
```

### Individual Steps

1. **Fetch ZIP files**:
```bash
node src/main.js --config=configs/rcny.json --step=fetch --verbose
```

2. **Extract ZIP contents**:
```bash
node src/main.js --config=configs/rcny.json --step=extract --verbose
```

3. **Parse XML to NDJSON**:
```bash
node src/main.js --config=configs/rcny.json --step=parse --verbose
```

4. **Load into database**:
```bash
node src/main.js --config=configs/rcny.json --step=load --verbose
```

### Dry Run Mode
```bash
node src/main.js --config=configs/rcny.json --dry-run --verbose
```

## Database Schema

Each legislative source gets its own PostgreSQL database with the following key tables:

- **`units`** - Hierarchical structure (titles, chapters, sections, etc.)
- **`unit_text_versions`** - Versioned text content with effective dates
- **`citations`** - Cross-references within and between legislative sources
- **`unit_search`** - Full-text search optimization
- **`change_events`** - Amendment tracking

## Integration with co-collection

After processing legislative data:

1. **Register sources** in co-collection's `sources` table
2. **Populate global norms** table for CURIE resolution
3. **Run citation extraction** from legislative texts
4. **Enable unified search** across case and legislative content

## Directory Structure

```
co-data/legislative-extractor/
├── configs/           # Configuration files
├── src/              # Source code
│   ├── main.js       # Main orchestrator
│   ├── config.js     # Configuration constants
│   ├── config_file.js # Config file handling
│   ├── zip_processor.js    # ZIP download/extraction
│   ├── xml_statute_parser.js # XML parsing to NDJSON
│   ├── statute_loader.js    # Database loading
│   └── citation_extractor.js # Citation processing
├── data/             # Data directories
│   ├── staging/      # Extracted XML files
│   ├── processed/    # NDJSON output files
│   └── zips/         # Downloaded ZIP files
├── logs/             # Log files
├── package.json      # Dependencies
└── README.md         # This file
```

## Development

### Adding New Legislative Sources

1. Create a new configuration file in `configs/`
2. Define the XML structure hierarchy
3. Configure database connection
4. Test with `--dry-run` first
5. Run full extraction pipeline

### Extending XML Parsing

Modify `xml_statute_parser.js` to handle new XML formats or hierarchies. The parser uses XPath expressions defined in the configuration to extract hierarchical relationships.

## Troubleshooting

### Common Issues

1. **Configuration validation errors**: Check JSON syntax and required fields
2. **Database connection failures**: Verify PostgreSQL credentials and network access
3. **XML parsing errors**: Check XML structure and XPath expressions in config
4. **Memory issues**: Adjust batch sizes in configuration for large datasets

### Logs and Debugging

- Use `--verbose` flag for detailed output
- Check `logs/` directory for execution logs
- Use `--dry-run` to test without making changes

## Contributing

1. Follow the existing code patterns in the caselaw-extractor
2. Add configuration examples for new legislative sources
3. Update documentation for new features
4. Test with multiple legislative sources before submitting

## Related Projects

- **co-collection**: Main legal data collection and search platform
- **co-data/caselaw-extractor**: Case law data extraction tool
- **co-frontend**: User interface for legal research

## License

MIT License - see LICENSE file for details.
