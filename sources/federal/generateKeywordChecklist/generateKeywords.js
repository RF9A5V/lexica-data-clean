import { reconstituteSection } from "./reconstituteSection.js";

async function generateKeywords(pgClient, title = null) {
  let rows;

  if (title) {
    let results = await pgClient.query('SELECT element_id FROM usc_keyword_checklist WHERE element_id LIKE $1 AND processed_at IS NULL', [`/us/usc/t${title}/%`]);
    rows = results.rows;
  } else {
    let results = await pgClient.query('SELECT element_id FROM usc_keyword_checklist WHERE processed_at IS NULL');
    rows = results.rows;
  }

  const elements = rows.map(row => row.element_id);

  const { rows: totalEntries } = await pgClient.query('SELECT COUNT(*) FROM usc_keyword_checklist WHERE processed_at IS NULL');
  console.log(`[DEBUG] Found ${totalEntries[0].count} total entries.`);
  console.log(`[DEBUG] Found ${elements.length} entries to process.`);

  for(let element of elements) {
    const sectionText = await reconstituteSection(pgClient, element);
    console.log(sectionText);
  }
}

export { generateKeywords };
