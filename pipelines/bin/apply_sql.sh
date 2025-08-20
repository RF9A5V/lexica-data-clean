#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || "$1" != "--db" ]]; then
  echo "Usage: $0 --db <DATABASE_URL|connection_string>" >&2
  exit 1
fi
DB_URL="$2"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
SQL_DIR="$DIR/sql"

export PSQLRC=/dev/null

for f in $(ls -1 "$SQL_DIR"/*.sql | sort); do
  echo "[apply_sql] Applying $(basename "$f")"
  PGPASSWORD="" psql "$DB_URL" -v ON_ERROR_STOP=1 -f "$f"
done

echo "[apply_sql] Done."
