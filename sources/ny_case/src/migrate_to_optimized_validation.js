#!/usr/bin/env node

/**
 * Migration script to help transition from the original validation script
 * to the optimized version with minimal data loss.
 */

import pg from 'pg';
import fs from 'fs';

const pool = new pg.Pool({
  connectionString: process.env.NY_STATE_APPEALS_DB || 'postgresql://localhost/ny_court_of_appeals',
  ssl: false
});

async function analyzeCurrentProgress() {
  console.log('🔍 Analyzing current validation progress...\n');
  
  try {
    // Check total keywords to validate
    const totalResult = await pool.query(
      "SELECT COUNT(*) FROM keywords WHERE tier IN ('major_doctrine', 'legal_concept')"
    );
    const totalKeywords = parseInt(totalResult.rows[0].count);
    
    // Check already validated keywords
    const validatedResult = await pool.query(`
      SELECT COUNT(DISTINCT doctrine_or_concept_keyword_id) 
      FROM keyword_validation kv
      JOIN keywords k ON kv.doctrine_or_concept_keyword_id = k.id
      WHERE k.tier IN ('major_doctrine', 'legal_concept')
    `);
    const validatedKeywords = parseInt(validatedResult.rows[0].count);
    
    // Check total validations created
    const validationsResult = await pool.query(
      "SELECT COUNT(*) FROM keyword_validation"
    );
    const totalValidations = parseInt(validationsResult.rows[0].count);
    
    // Calculate progress
    const progressPercentage = ((validatedKeywords / totalKeywords) * 100).toFixed(1);
    const remainingKeywords = totalKeywords - validatedKeywords;
    
    console.log('📊 Current Progress Analysis');
    console.log('=' .repeat(40));
    console.log(`Total keywords to validate: ${totalKeywords.toLocaleString()}`);
    console.log(`Keywords already validated: ${validatedKeywords.toLocaleString()}`);
    console.log(`Remaining keywords: ${remainingKeywords.toLocaleString()}`);
    console.log(`Progress: ${progressPercentage}%`);
    console.log(`Total validations created: ${totalValidations.toLocaleString()}`);
    
    // Time estimates
    console.log('\n⏱️  Time Estimates');
    console.log('=' .repeat(40));
    
    // Original script estimates
    const originalRatePerMin = 11; // Based on observed performance
    const originalTimeRemaining = Math.ceil(remainingKeywords / originalRatePerMin);
    const originalHours = Math.floor(originalTimeRemaining / 60);
    const originalMinutes = originalTimeRemaining % 60;
    
    console.log(`Original script remaining time: ${originalHours}h ${originalMinutes}m`);
    
    // Optimized script estimates
    const optimizedRatePerMin = 200; // Conservative estimate
    const optimizedTimeRemaining = Math.ceil(remainingKeywords / optimizedRatePerMin);
    const optimizedHours = Math.floor(optimizedTimeRemaining / 60);
    const optimizedMinutesRemaining = optimizedTimeRemaining % 60;
    
    console.log(`Optimized script estimated time: ${optimizedHours}h ${optimizedMinutesRemaining}m`);
    
    const timeSavings = originalTimeRemaining - optimizedTimeRemaining;
    const savingsHours = Math.floor(timeSavings / 60);
    const savingsMinutes = timeSavings % 60;
    
    console.log(`Time savings: ${savingsHours}h ${savingsMinutes}m (${Math.round((timeSavings/originalTimeRemaining)*100)}% faster)`);
    
    // Generate progress file for optimized script
    if (validatedKeywords > 0) {
      console.log('\n📝 Generating progress file for optimized script...');
      
      const processedResult = await pool.query(`
        SELECT DISTINCT doctrine_or_concept_keyword_id as id
        FROM keyword_validation kv
        JOIN keywords k ON kv.doctrine_or_concept_keyword_id = k.id
        WHERE k.tier IN ('major_doctrine', 'legal_concept')
      `);
      
      const processedKeywords = processedResult.rows.map(row => row.id);
      
      const progressData = {
        processedKeywords: processedKeywords,
        processed: validatedKeywords,
        total: totalKeywords,
        timestamp: new Date().toISOString(),
        migrated_from_original: true
      };
      
      fs.writeFileSync('validation_progress.json', JSON.stringify(progressData, null, 2));
      console.log(`✅ Created validation_progress.json with ${processedKeywords.length} processed keywords`);
    }
    
    // Recommendations
    console.log('\n💡 Recommendations');
    console.log('=' .repeat(40));
    
    if (progressPercentage < 10) {
      console.log('🔄 RECOMMENDED: Stop current script and start with optimized version');
      console.log('   - Very little progress lost');
      console.log('   - Massive time savings');
    } else if (progressPercentage < 50) {
      console.log('🔄 RECOMMENDED: Stop current script and resume with optimized version');
      console.log('   - Progress file created to resume from current point');
      console.log('   - Still significant time savings');
    } else {
      console.log('⚖️  CONSIDER: Current script has made significant progress');
      console.log('   - You can continue with current script');
      console.log('   - Or stop and resume with optimized version');
      console.log('   - Optimized version will still be faster');
    }
    
    console.log('\n🚀 To use optimized script:');
    console.log('   1. Stop current script (Ctrl+C)');
    console.log('   2. Run: node validateKeywordFields_optimized.js resume');
    console.log('   3. Monitor progress with real-time metrics');
    
  } catch (error) {
    console.error('❌ Error analyzing progress:', error.message);
  } finally {
    await pool.end();
  }
}

async function createBackup() {
  console.log('💾 Creating backup of current validation data...\n');
  
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupFile = `keyword_validation_backup_${timestamp}.sql`;
    
    // Export current validation data
    const result = await pool.query('SELECT * FROM keyword_validation ORDER BY id');
    
    let sqlContent = `-- Keyword Validation Backup - ${new Date().toISOString()}\n`;
    sqlContent += `-- Total records: ${result.rows.length}\n\n`;
    sqlContent += `-- Restore with: psql -d ny_court_of_appeals -f ${backupFile}\n\n`;
    
    if (result.rows.length > 0) {
      sqlContent += `INSERT INTO keyword_validation (field_of_law_keyword_id, doctrine_or_concept_keyword_id) VALUES\n`;
      
      const values = result.rows.map(row => 
        `(${row.field_of_law_keyword_id}, ${row.doctrine_or_concept_keyword_id})`
      ).join(',\n');
      
      sqlContent += values + '\nON CONFLICT DO NOTHING;\n';
    }
    
    fs.writeFileSync(backupFile, sqlContent);
    console.log(`✅ Backup created: ${backupFile}`);
    console.log(`📊 Backed up ${result.rows.length} validation records`);
    
  } catch (error) {
    console.error('❌ Error creating backup:', error.message);
  }
}

async function validateDataIntegrity() {
  console.log('🔍 Validating data integrity...\n');
  
  try {
    // Check for orphaned validations
    const orphanedResult = await pool.query(`
      SELECT COUNT(*) FROM keyword_validation kv
      LEFT JOIN keywords k1 ON kv.field_of_law_keyword_id = k1.id
      LEFT JOIN keywords k2 ON kv.doctrine_or_concept_keyword_id = k2.id
      WHERE k1.id IS NULL OR k2.id IS NULL
    `);
    
    const orphanedCount = parseInt(orphanedResult.rows[0].count);
    
    if (orphanedCount > 0) {
      console.log(`⚠️  Found ${orphanedCount} orphaned validation records`);
    } else {
      console.log('✅ No orphaned validation records found');
    }
    
    // Check tier distribution
    const tierResult = await pool.query(`
      SELECT 
        k.tier,
        COUNT(DISTINCT k.id) as total_keywords,
        COUNT(DISTINCT kv.doctrine_or_concept_keyword_id) as validated_keywords
      FROM keywords k
      LEFT JOIN keyword_validation kv ON k.id = kv.doctrine_or_concept_keyword_id
      WHERE k.tier IN ('major_doctrine', 'legal_concept')
      GROUP BY k.tier
      ORDER BY k.tier
    `);
    
    console.log('\n📊 Validation by Tier:');
    tierResult.rows.forEach(row => {
      const percentage = ((row.validated_keywords / row.total_keywords) * 100).toFixed(1);
      console.log(`   ${row.tier}: ${row.validated_keywords}/${row.total_keywords} (${percentage}%)`);
    });
    
    // Check field distribution
    const fieldResult = await pool.query(`
      SELECT 
        k.keyword_text as field_name,
        COUNT(kv.id) as validation_count
      FROM keywords k
      JOIN keyword_validation kv ON k.id = kv.field_of_law_keyword_id
      WHERE k.tier = 'field_of_law'
      GROUP BY k.id, k.keyword_text
      ORDER BY validation_count DESC
      LIMIT 10
    `);
    
    console.log('\n📊 Top Fields by Validation Count:');
    fieldResult.rows.forEach(row => {
      console.log(`   ${row.field_name}: ${row.validation_count} validations`);
    });
    
  } catch (error) {
    console.error('❌ Error validating data integrity:', error.message);
  }
}

// CLI interface
const command = process.argv[2];

switch (command) {
  case 'analyze':
    analyzeCurrentProgress();
    break;
  case 'backup':
    createBackup().then(() => process.exit(0));
    break;
  case 'validate':
    validateDataIntegrity().then(() => pool.end());
    break;
  case 'full':
    console.log('🔄 Running full migration analysis...\n');
    Promise.resolve()
      .then(() => analyzeCurrentProgress())
      .then(() => createBackup())
      .then(() => validateDataIntegrity())
      .then(() => {
        console.log('\n✅ Migration analysis complete!');
        console.log('📋 Review the analysis above and follow the recommendations.');
      });
    break;
  default:
    console.log(`
🔄 Keyword Validation Migration Tool

Usage:
  node migrate_to_optimized_validation.js analyze   # Analyze current progress
  node migrate_to_optimized_validation.js backup    # Create data backup
  node migrate_to_optimized_validation.js validate  # Check data integrity
  node migrate_to_optimized_validation.js full      # Run all checks

This tool helps you:
  ✅ Assess current validation progress
  ✅ Create backups before switching
  ✅ Generate resume files for optimized script
  ✅ Validate data integrity
  ✅ Provide time/cost estimates

Recommended workflow:
  1. Run: node migrate_to_optimized_validation.js full
  2. Stop current validation script (Ctrl+C)
  3. Run: node validateKeywordFields_optimized.js resume
    `);
    process.exit(0);
}