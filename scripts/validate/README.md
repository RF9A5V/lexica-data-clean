# Validation Scripts (`validate`)

Scripts for validating parsed US Code data for structural and content errors.

## Usage

Typical usage:

```sh
node validate/validate_parsed_data.js
```

- Runs a suite of validation checks on all NDJSON files in `data/parsed/`.
- Logs per-title validation results and a combined log in `all_validation_logs.txt`.
- See each script for check-specific details and configuration.

---
