# Embed Scripts (`embed`)

Scripts for generating and handling text embeddings for US Code sections.

## Usage

Typical usage:

```sh
node embed/embed_sections_to_pg.js
```

- Reads parsed/processed sections and generates embeddings.
- Embeddings are stored in Postgres or another target DB.
- See the script for configuration details and dependencies (e.g., OpenAI API key, DB connection).

---
