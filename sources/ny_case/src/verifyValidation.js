import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.NY_STATE_APPEALS_DB || 'postgresql://localhost/ny_court_of_appeals',
  ssl: false
});

async function verifyValidation() {
  console.log('🔍 Verifying keyword validation results...');
  
  try {
    // Summary statistics
    const summary = await pool.query(`
      SELECT 
        COUNT(*) as total_validations,
        COUNT(DISTINCT doctrine_or_concept_keyword_id) as validated_doctrines,
        COUNT(DISTINCT field_of_law_keyword_id) as referenced_fields
      FROM keyword_validation
    `);
    
    console.log('📊 Validation Summary:');
    console.log(`   Total validations: ${summary.rows[0].total_validations}`);
    console.log(`   Validated doctrines/concepts: ${summary.rows[0].validated_doctrines}`);
    console.log(`   Referenced field of laws: ${summary.rows[0].referenced_fields}`);
    
    // Sample validations
    const sample = await pool.query(`
      SELECT * FROM validated_keyword_relationships 
      ORDER BY doctrine_or_concept, field_of_law
      LIMIT 20
    `);
    
    console.log('\n📋 Sample validations:');
    sample.rows.forEach(row => {
      console.log(`   ${row.doctrine_or_concept} → ${row.field_of_law}`);
    });
    
    // Quick validation check
    const validationCount = await pool.query(`
      SELECT COUNT(*) as count FROM keyword_validation
    `);
    
    if (validationCount.rows[0].count === '0') {
      console.log('\n⚠️  No validations found. Run the validation script first:');
      console.log('   node validateKeywordFields.js run');
    }
    
    // Top fields by doctrine count
    const topFields = await pool.query(`
      SELECT field_of_law, COUNT(*) as doctrine_count
      FROM validated_keyword_relationships
      GROUP BY field_of_law
      ORDER BY doctrine_count DESC
      LIMIT 10
    `);
    
    console.log('\n🏆 Top 10 fields by doctrine count:');
    topFields.rows.forEach(row => {
      console.log(`   ${row.field_of_law}: ${row.doctrine_count} doctrines`);
    });
    
    // Doctrines with most fields
    const topDoctrines = await pool.query(`
      SELECT doctrine_or_concept, COUNT(*) as field_count
      FROM validated_keyword_relationships
      GROUP BY doctrine_or_concept
      ORDER BY field_count DESC
      LIMIT 10
    `);
    
    console.log('\n🏆 Top 10 doctrines by field count:');
    topDoctrines.rows.forEach(row => {
      console.log(`   ${row.doctrine_or_concept}: ${row.field_count} fields`);
    });
    
    // Check for unvalidated keywords
    const unvalidated = await pool.query(`
      SELECT COUNT(*) as unvalidated_count
      FROM keywords
      WHERE tier IN ('major_doctrine', 'legal_concept')
        AND id NOT IN (SELECT doctrine_or_concept_keyword_id FROM keyword_validation)
    `);
    
    console.log(`\n📊 Unvalidated doctrine/concept keywords: ${unvalidated.rows[0].unvalidated_count}`);
    
  } catch (error) {
    console.error('❌ Error during verification:', error);
  } finally {
    await pool.end();
  }
}

if (process.argv[2] === 'verify') {
  verifyValidation();
}
