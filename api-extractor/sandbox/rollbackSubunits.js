import pg from "pg";

const { Pool } = pg;

// DB config mirrors createSubunits.js
const DB_CONFIG = {
  host: process.env.DB_HOST || "localhost",
  port: Number(process.env.DB_PORT || 5432),
  database: process.env.DB_NAME || "nysenate_legislative",
  user: process.env.DB_USER || "dev",
  password: process.env.DB_PASSWORD || "dev",
};

function parseCliArgs(argv) {
  const opts = { dryRun: true, lawId: null, dropColumns: false, help: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--write" || a === "-w") {
      opts.dryRun = false;
    } else if (a === "--dry" || a === "--dry-run") {
      opts.dryRun = true;
    } else if (a === "--law" || a === "-l") {
      const v = argv[++i];
      if (v) opts.lawId = String(v).toUpperCase();
    } else if (a.startsWith("--law=")) {
      opts.lawId = a.split("=")[1].toUpperCase();
    } else if (a === "--drop-columns") {
      opts.dropColumns = true;
    } else if (a === "--help" || a === "-h") {
      opts.help = true;
    }
  }
  return opts;
}

function buildWhereLaw(lawId, alias = "u") {
  if (!lawId) return { sql: "", params: [] };
  return { sql: ` AND ${alias}.law_id = $1 `, params: [lawId] };
}

async function runRollback({ dryRun, lawId, dropColumns }) {
  const pool = new Pool(DB_CONFIG);
  try {
    const where = buildWhereLaw(lawId, "u");

    // Counts for subunits
    const subunitCountSql = `
      WITH subunits AS (
        SELECT u.id
        FROM units u
        WHERE u.unit_type IN ('subdivision','paragraph','subparagraph')
          AND u.source_id = 'nysenate'
          ${where.sql}
      )
      SELECT
        (SELECT COUNT(*)::int FROM subunits) AS units_count,
        (SELECT COUNT(*)::int FROM unit_text_versions t WHERE t.unit_id IN (SELECT id FROM subunits)) AS utv_count
    `;
    const subCounts = await pool.query(subunitCountSql, where.params);

    // Counts for section UTVs to clear
    const sectionFieldsCountSql = `
      WITH sections AS (
        SELECT u.id
        FROM units u
        WHERE u.unit_type = 'section'
          ${where.sql}
      )
      SELECT COUNT(*)::int AS utv_to_clear
      FROM unit_text_versions t
      WHERE t.unit_id IN (SELECT id FROM sections)
        AND (t.text_tokenized IS NOT NULL OR t.citations_extracted IS NOT NULL OR t.tokenization_checksum IS NOT NULL)
    `;
    const secCounts = await pool.query(sectionFieldsCountSql, where.params);

    console.log(`Rollback plan (${dryRun ? 'dry-run' : 'write mode'})${lawId ? ` for law ${lawId}` : ''}:`);
    console.log(`- Subunit units to delete: ${subCounts.rows[0].units_count}`);
    console.log(`- Subunit unit_text_versions to delete: ${subCounts.rows[0].utv_count}`);
    console.log(`- Section unit_text_versions rows with tokenized/citations to clear: ${secCounts.rows[0].utv_to_clear}`);
    if (dropColumns) {
      console.log(`- Will DROP columns text_tokenized, citations_extracted, tokenization_checksum from unit_text_versions`);
    }

    if (dryRun) return;

    await pool.query('BEGIN');
    try {
      // Delete subunit UTVs
      const deleteUtvSql = `
        DELETE FROM unit_text_versions t
        USING (
          SELECT u.id
          FROM units u
          WHERE u.unit_type IN ('subdivision','paragraph','subparagraph')
            AND u.source_id = 'nysenate'
            ${where.sql}
        ) s
        WHERE t.unit_id = s.id
      `;
      await pool.query(deleteUtvSql, where.params);

      // Delete subunit units
      const deleteUnitsSql = `
        DELETE FROM units u
        WHERE u.unit_type IN ('subdivision','paragraph','subparagraph')
          AND u.source_id = 'nysenate'
          ${where.sql}
      `;
      await pool.query(deleteUnitsSql, where.params);

      // Null out tokenized/citations on section UTVs
      const clearSectionUtvSql = `
        UPDATE unit_text_versions t
        SET text_tokenized = NULL,
            citations_extracted = NULL,
            tokenization_checksum = NULL
        WHERE t.unit_id IN (
          SELECT u.id
          FROM units u
          WHERE u.unit_type = 'section'
            ${where.sql}
        )
      `;
      await pool.query(clearSectionUtvSql, where.params);

      // Optional drop columns
      if (dropColumns) {
        await pool.query(`ALTER TABLE unit_text_versions
          DROP COLUMN IF EXISTS text_tokenized,
          DROP COLUMN IF EXISTS citations_extracted,
          DROP COLUMN IF EXISTS tokenization_checksum`);
      }

      await pool.query('COMMIT');
      console.log('Rollback completed.');
    } catch (e) {
      await pool.query('ROLLBACK');
      console.error('Rollback failed:', e.message);
      throw e;
    }
  } finally {
    await pool.end();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const cli = parseCliArgs(process.argv.slice(2));
  if (cli.help) {
    console.log(`Usage: node rollbackSubunits.js [options]\n\n` +
      `Options:\n` +
      `  --law, -l <LAW>    Restrict to a specific law (e.g., PEN, ABC)\n` +
      `  --write, -w        Execute (default is dry-run)\n` +
      `  --dry, --dry-run   Dry-run only (default)\n` +
      `  --drop-columns     Also drop tokenization columns from unit_text_versions\n` +
      `  --help, -h         Show this help\n`);
    process.exit(0);
  }

  runRollback({ dryRun: cli.dryRun !== false, lawId: cli.lawId || null, dropColumns: !!cli.dropColumns })
    .catch(err => { console.error(err); process.exit(1); });
}
