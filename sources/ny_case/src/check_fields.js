#!/usr/bin/env node

import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.NY_STATE_APPEALS_DB || 'postgresql://localhost/ny_court_of_appeals',
  ssl: false
});

async function checkFields() {
  try {
    const result = await pool.query(
      "SELECT id, keyword_text FROM keywords WHERE tier = 'field_of_law' ORDER BY keyword_text"
    );
    
    console.log(`Found ${result.rows.length} field of law keywords:`);
    result.rows.forEach((row, i) => {
      console.log(`${i+1}. "${row.keyword_text}" (ID: ${row.id})`);
    });
    
  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    await pool.end();
  }
}

checkFields();