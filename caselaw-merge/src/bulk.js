// Chunked parameterized INSERT helper. For tables under ~100k rows
// (phase 2 keywords ≈ 33k total). Phase 3+ should switch to COPY via
// pg-copy-streams once we hit the cases table (~940k rows).
//
// Usage:
//   await bulkInsert(client, '_stage_keywords',
//     ['source_ref','old_id','keyword_text','tier','frequency'],
//     rows,
//     { chunkSize: 1000 });
//
// `rows` is an array of plain objects keyed by the same names as `columns`.

export async function bulkInsert(client, table, columns, rows, { chunkSize = 1000 } = {}) {
  if (rows.length === 0) return 0;
  let written = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const values = [];
    const placeholders = chunk
      .map((row, ri) => {
        const cells = columns.map((c, ci) => `$${ri * columns.length + ci + 1}`);
        for (const c of columns) values.push(row[c]);
        return `(${cells.join(', ')})`;
      })
      .join(', ');
    const sql = `INSERT INTO ${table} (${columns.join(', ')}) VALUES ${placeholders}`;
    await client.query(sql, values);
    written += chunk.length;
  }
  return written;
}
