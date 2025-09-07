import pg from "pg";
const { Client } = pg;

const DB_CONFIG = {
  host: 'localhost',
  port: 5432,
  database: 'nysenate_legislative',
  user: 'dev',
  password: 'dev'
};

/**
 * Retrieves the text_plain field for a specific unit from the NYSenate legislative database
 * @param {string} unitId - The unit ID to fetch
 * @returns {Promise<Object|null>} Object with id and text_plain, or null if not found
 */
async function getSectionText(unitId) {
    const client = new Client(DB_CONFIG);
  
    try {
      await client.connect();
  
      const query = `
        SELECT u.id, text_plain
        FROM units AS u
        JOIN unit_text_versions AS utv ON u.id = utv.unit_id
        WHERE unit_type = 'section' AND u.id = $1
      `;
  
      const result = await client.query(query, [unitId]);
  
      if (result.rows.length === 0) {
        console.log(`No section found with ID: ${unitId}`);
        return null;
      }
  
      const firstResult = result.rows[0];
  
      if (!firstResult.text_plain) {
        console.log(`Section with ID: ${unitId} does not contain text`);
        return null;
      }
  
      return firstResult.text_plain;
    } catch (error) {
      console.error('Database query error:', error);
      throw error;
    } finally {
      await client.end();
    }
  }

  export default getSectionText;