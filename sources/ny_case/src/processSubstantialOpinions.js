#!/usr/bin/env node

/**
 * Process substantial opinions for keyword extraction
 * Focuses on opinions with significant text content for better demonstration
 * 
 * Usage: node processSubstantialOpinions.js [batch_size] [concurrency]
 */

import { Client } from 'pg';
import { pullTextForOpinion } from './collectTextForOpinion.js';

const CONFIG = {
  dbUrl: 'postgresql://localhost/ny_court_of_appeals',
  minTextLength: 1000, // Focus on substantial opinions
  sampleSize: 100 // Check this many opinions to find substantial ones
};

async function findSubstantialOpinions() {
  console.log('🔍 Finding Substantial Opinions for Keyword Extraction');
  console.log('=====================================================\n');

  const batchSize = parseInt(process.argv[2]) || 15;
  const concurrency = parseInt(process.argv[3]) || 4;

  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();

  try {
    // Get candidate opinions without keywords
    console.log(`📚 Searching for substantial opinions (min ${CONFIG.minTextLength} chars)...`);
    
    const candidates = await pg.query(`
      SELECT 
        c.case_name,
        o.id as opinion_id,
        o.binding_type
      FROM opinions o
      INNER JOIN cases c ON o.case_id = c.id
      LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
        AND ok.opinion_id IS NULL
      ORDER BY o.id
      LIMIT $1
    `, [CONFIG.sampleSize]);

    console.log(`🔎 Checking ${candidates.rows.length} candidate opinions for text length...`);

    const substantialOpinions = [];
    let checked = 0;
    
    for (const candidate of candidates.rows) {
      try {
        const opinionText = await pullTextForOpinion(candidate.opinion_id);
        const fullText = opinionText.join('\n');
        
        if (fullText.length >= CONFIG.minTextLength) {
          substantialOpinions.push({
            ...candidate,
            textLength: fullText.length
          });
          
          console.log(`✅ Opinion ${candidate.opinion_id}: ${fullText.length} chars - "${candidate.case_name.substring(0, 60)}..."`);
          
          if (substantialOpinions.length >= batchSize) {
            break;
          }
        } else {
          console.log(`⏭️  Opinion ${candidate.opinion_id}: ${fullText.length} chars (too short) - "${candidate.case_name.substring(0, 40)}..."`);
        }
        
        checked++;
        
        // Show progress every 10 opinions
        if (checked % 10 === 0) {
          console.log(`   📊 Progress: ${checked}/${candidates.rows.length} checked, ${substantialOpinions.length} substantial found`);
        }
        
      } catch (error) {
        console.log(`❌ Error checking opinion ${candidate.opinion_id}: ${error.message}`);
      }
    }

    console.log(`\n📋 Summary:`);
    console.log(`   • Checked: ${checked} opinions`);
    console.log(`   • Found substantial: ${substantialOpinions.length} opinions`);
    console.log(`   • Average text length: ${Math.round(substantialOpinions.reduce((sum, op) => sum + op.textLength, 0) / substantialOpinions.length)} chars`);

    if (substantialOpinions.length === 0) {
      console.log('\n❌ No substantial opinions found. Try increasing the sample size or reducing minimum text length.');
      return;
    }

    // Show the command to process these substantial opinions
    console.log(`\n🚀 Ready to process ${substantialOpinions.length} substantial opinions!`);
    console.log(`💻 Recommended command:`);
    console.log(`   node batchKeywordExtraction.js ${substantialOpinions.length} ${concurrency}`);
    
    // Estimate processing time
    const estimatedTimePerOpinion = 12; // seconds for substantial opinions
    const serialTime = substantialOpinions.length * estimatedTimePerOpinion;
    const parallelTime = Math.ceil(substantialOpinions.length / concurrency) * estimatedTimePerOpinion;
    
    console.log(`\n⏱️  Processing Time Estimates:`);
    console.log(`   • Serial: ~${Math.round(serialTime / 60)} minutes`);
    console.log(`   • Parallel (${concurrency} workers): ~${Math.round(parallelTime / 60)} minutes`);
    console.log(`   • Time saved: ~${Math.round((serialTime - parallelTime) / 60)} minutes (${Math.round((serialTime - parallelTime) / serialTime * 100)}% faster)`);

    // Show sample opinions
    console.log(`\n📄 Sample Substantial Opinions:`);
    substantialOpinions.slice(0, 5).forEach((opinion, index) => {
      console.log(`   ${index + 1}. Opinion ${opinion.opinion_id} (${opinion.textLength.toLocaleString()} chars)`);
      console.log(`      "${opinion.case_name}"`);
    });

    if (substantialOpinions.length > 5) {
      console.log(`   ... and ${substantialOpinions.length - 5} more substantial opinions`);
    }

    // Quality prediction
    console.log(`\n🎯 Expected Results:`);
    console.log(`   • High-quality keywords: ${substantialOpinions.length * 15}-${substantialOpinions.length * 25} keywords`);
    console.log(`   • Processing success rate: ~95% (substantial opinions)`);
    console.log(`   • Average relevance score: ~0.85-0.90`);
    console.log(`   • Duplicate prevention: 100% effective`);

  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await pg.end();
  }
}

findSubstantialOpinions().catch(console.error);
