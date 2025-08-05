# Diagnostic Tool (`diagnose`)

This tool parses `all_validation_logs.txt`, looks up problematic entries in NDJSON and XML, and writes detailed diagnostic logs for downstream review/automation.

## Usage

```sh
node diagnose/index.js
```

- Progress is shown in the terminal.
- Results are saved to `data/parsed/diagnostic_logs.txt`.
- No arguments required; all paths are hardcoded for current project structure.

---
