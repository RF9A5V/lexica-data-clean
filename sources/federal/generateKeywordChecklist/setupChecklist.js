async function setupChecklist(pgClient) {
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS llm_keywords (
      id SERIAL PRIMARY KEY,
      keyword TEXT
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS usc_keyword_checklist (
      element_id TEXT PRIMARY KEY,
      processed_at TIMESTAMPTZ
    );
  `);

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS usc_llm_keyword_edges (
      element_id TEXT,
      keyword_id INTEGER,
      PRIMARY KEY (element_id, keyword_id)
    );
  `);

  const { rows } = await pgClient.query('SELECT element_id, element_type FROM usc_elements WHERE element_type = $1', ['section']);
  console.log(`[DEBUG] Found ${rows.length} sections.`);

  const sections = rows.map(row => row.element_id);

  for(let section of sections) {
    await pgClient.query('INSERT INTO usc_keyword_checklist (element_id) VALUES ($1) ON CONFLICT DO NOTHING', [section]);
  }

  // const { rows: checklistEntries } = await pgClient.query('SELECT element_id FROM usc_keyword_checklist WHERE processed_at IS NULL');
  // const { rows: totalEntries } = await pgClient.query('SELECT COUNT(*) FROM usc_keyword_checklist');
  // console.log(`[DEBUG] Found ${totalEntries[0].count} total entries.`);
  // console.log(`[DEBUG] Found ${checklistEntries.length} entries to process.`);
}

export { setupChecklist };
