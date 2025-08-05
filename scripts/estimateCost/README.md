# Estimate Cost Script for Markdown Token Counting

This script recursively scans the `lexica-data/data/keywords/` directory for Markdown files, counts tokens in each file using the `tiktoken` package, and generates a cost report per directory in `lexica-data/data/costEstimate/`.

## Usage

1. Install dependencies (already done):
   ```bash
   npm install tiktoken
   ```

2. Run the script:
   ```bash
   node estimateCost.js
   ```

- The script will output report files in `lexica-data/data/costEstimate/`, mirroring the structure of `keywords/`.
- Each report file will be named `cost_report.txt` and will list token counts for each markdown file in the directory, plus a directory total.
