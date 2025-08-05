#!/usr/bin/env node

/**
 * NY State Legal Keyword Classification Script
 * 
 * Classifies legal doctrines and concepts by their applicable fields of law
 * specifically within the context of New York State law.
 * 
 * Uses the same parallelization pattern as processKeywords.js for optimal performance.
 */

import OpenAI from 'openai';
import pg from 'pg';
import pLimit from 'p-limit';
import cliProgress from 'cli-progress';

// Database connection
const pool = new pg.Pool({
  connectionString: process.env.NY_STATE_APPEALS_DB || 'postgresql://localhost/ny_court_of_appeals',
  ssl: false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

/**
 * Get all field of law keywords from database
 */
async function getFieldOfLawKeywords() {
  const result = await pool.query(
    "SELECT id, keyword_text FROM keywords WHERE tier = 'field_of_law' ORDER BY keyword_text"
  );
  return result.rows;
}

/**
 * Get all doctrine and concept keywords to classify
 */
async function getDoctrineAndConceptKeywords() {
  const result = await pool.query(
    "SELECT id, keyword_text, tier FROM keywords WHERE tier IN ('major_doctrine', 'legal_concept') ORDER BY id"
  );
  return result.rows;
}

/**
 * Build NY State-specific system prompt
 */
async function buildSystemPrompt() {
  const fieldKeywords = await getFieldOfLawKeywords();
  
  return `You are a legal expert specializing in New York State law. Your task is to classify legal doctrines and concepts by their applicable fields of law within the New York State legal system.

CONTEXT: You are analyzing keywords extracted from New York State Court of Appeals cases and other NY state court decisions.

AVAILABLE FIELDS OF LAW:
${fieldKeywords.map(k => `- ${k.keyword_text}`).join('\n')}

INSTRUCTIONS:
1. For each legal doctrine or concept provided, determine which fields of law it applies to in New York State
2. Consider NY-specific statutes, case law, and legal practices
3. Return a JSON object with a "fields" array containing applicable field names from the above list
4. Use exact field names from the list above
5. Be conservative - only include fields where the doctrine/concept is genuinely relevant in NY practice
6. If a doctrine applies to multiple fields, include all relevant ones
7. If uncertain or if the concept doesn't clearly apply to any specific field, return an empty array

EXAMPLES:
Input: "strict liability"
Output: {"fields": ["tort law", "product liability"]}

Input: "attorney-client privilege"  
Output: {"fields": ["legal ethics", "evidence law"]}

Input: "workers compensation"
Output: {"fields": ["workers' compensation law", "employment law"]}

Remember: Focus specifically on how these concepts apply within New York State legal practice.`;
}

/**
 * Classify a single keyword using OpenAI
 */
async function classifyKeyword(keyword, systemPrompt) {
  const maxRetries = 3;
  const baseDelay = 1000;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: `Legal doctrine/concept: "${keyword.keyword_text}"` }
        ],
        temperature: 0.1,
        response_format: { type: "json_object" }
      });
      
      const content = response.choices[0].message.content;
      const parsed = JSON.parse(content);
      
      // Extract fields array
      if (parsed.fields && Array.isArray(parsed.fields)) {
        return parsed.fields;
      } else if (Array.isArray(parsed)) {
        return parsed;
      } else {
        return [];
      }
      
    } catch (error) {
      if (attempt === maxRetries) {
        console.error(`❌ Failed to classify ${keyword.keyword_text}: ${error.message}`);
        return [];
      }
      
      // Exponential backoff
      const delay = baseDelay * Math.pow(2, attempt - 1);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

/**
 * Create validation records in batch
 */
async function createValidationRecords(validations) {
  if (validations.length === 0) return 0;
  
  try {
    // Batch insert with conflict handling
    const values = validations.map((v, i) => `($${i*2+1}, $${i*2+2})`).join(', ');
    const params = validations.flatMap(v => [v.fieldId, v.doctrineId]);
    
    const result = await pool.query(
      `INSERT INTO keyword_validation (field_of_law_keyword_id, doctrine_or_concept_keyword_id) 
       VALUES ${values} ON CONFLICT DO NOTHING RETURNING id`,
      params
    );
    
    return result.rowCount;
  } catch (error) {
    console.error('❌ Error creating validation records:', error.message);
    return 0;
  }
}

/**
 * Main classification process
 */
async function classifyKeywords() {
  console.log('🎯 Starting NY State keyword classification...\n');
  
  // Load data
  const systemPrompt = await buildSystemPrompt();
  const fieldKeywords = await getFieldOfLawKeywords();
  const doctrineKeywords = await getDoctrineAndConceptKeywords();
  
  console.log(`📊 Found ${fieldKeywords.length} field of law keywords`);
  console.log(`📊 Processing ${doctrineKeywords.length} doctrine/concept keywords\n`);
  
  // Create field lookup map
  const fieldMap = new Map(fieldKeywords.map(f => [f.keyword_text.toLowerCase(), f.id]));
  
  // Progress bar
  const bar = new cliProgress.SingleBar({
    format: '[{bar}] {percentage}% | {value}/{total} | {keyword} | Rate: {rate}/min',
    barCompleteChar: '\u2588',
    barIncompleteChar: '\u2591',
    hideCursor: true
  });
  bar.start(doctrineKeywords.length, 0);
  
  // Performance tracking
  let processedCount = 0;
  let errorCount = 0;
  let totalValidations = 0;
  let unknownFields = new Set();
  const startTime = Date.now();
  
  // Concurrency control (same as processKeywords.js)
  const concurrencyLimit = parseInt(process.env.KEYWORD_CONCURRENCY) || 15;
  const apiLimit = pLimit(concurrencyLimit);
  
  console.log(`[INFO] Using concurrency limit: ${concurrencyLimit}\n`);
  
  // Process all keywords with rate limiting
  const tasks = doctrineKeywords.map(keyword =>
    apiLimit(async () => {
      try {
        // Update progress
        const elapsed = Date.now() - startTime;
        const rate = Math.round((processedCount / (elapsed / 60000)) || 0);
        bar.increment(1, { 
          keyword: keyword.keyword_text.substring(0, 40),
          rate: rate
        });
        
        // Classify keyword
        const applicableFields = await classifyKeyword(keyword, systemPrompt);
        const validations = [];
        
        // Map field names to IDs
        for (const fieldName of applicableFields) {
          const normalizedFieldName = fieldName.toLowerCase().trim();
          const fieldId = fieldMap.get(normalizedFieldName);
          
          if (fieldId) {
            validations.push({ fieldId, doctrineId: keyword.id });
          } else {
            unknownFields.add(fieldName);
          }
        }
        
        // Create validation records
        const created = await createValidationRecords(validations);
        totalValidations += created;
        processedCount++;
        
      } catch (error) {
        errorCount++;
        console.error(`\n❌ Error processing ${keyword.keyword_text}: ${error.message}`);
      }
    })
  );
  
  // Execute all tasks
  await Promise.all(tasks);
  
  bar.stop();
  
  // Final statistics
  const endTime = Date.now();
  const totalTime = Math.round((endTime - startTime) / 1000);
  const avgRate = Math.round((processedCount / (totalTime / 60)) || 0);
  
  console.log('\n✅ Classification completed!\n');
  console.log('📊 Final Statistics:');
  console.log(`   Keywords processed: ${processedCount}/${doctrineKeywords.length}`);
  console.log(`   Validation records created: ${totalValidations}`);
  console.log(`   Errors: ${errorCount}`);
  console.log(`   Unknown fields encountered: ${unknownFields.size}`);
  console.log(`   Total time: ${Math.floor(totalTime/60)}m ${totalTime%60}s`);
  console.log(`   Average rate: ${avgRate} keywords/min`);
  
  // Show unknown fields for debugging
  if (unknownFields.size > 0) {
    console.log('\n⚠️  Unknown fields encountered:');
    Array.from(unknownFields).slice(0, 10).forEach(field => {
      console.log(`   - "${field}"`);
    });
    if (unknownFields.size > 10) {
      console.log(`   ... and ${unknownFields.size - 10} more`);
    }
  }
  
  // Validation summary
  console.log('\n🔍 Validation Summary:');
  const summaryResult = await pool.query(`
    SELECT 
      COUNT(DISTINCT doctrine_or_concept_keyword_id) as classified_keywords,
      COUNT(*) as total_relationships,
      AVG(field_count) as avg_fields_per_keyword
    FROM (
      SELECT 
        doctrine_or_concept_keyword_id,
        COUNT(*) as field_count
      FROM keyword_validation 
      GROUP BY doctrine_or_concept_keyword_id
    ) subq
  `);
  
  const summary = summaryResult.rows[0];
  console.log(`   Keywords with classifications: ${summary.classified_keywords}`);
  console.log(`   Total field relationships: ${summary.total_relationships}`);
  console.log(`   Average fields per keyword: ${parseFloat(summary.avg_fields_per_keyword).toFixed(1)}`);
}

// CLI interface
const command = process.argv[2];

switch (command) {
  case 'run':
    classifyKeywords()
      .then(() => {
        console.log('\n🎉 NY State keyword classification complete!');
        process.exit(0);
      })
      .catch(error => {
        console.error('\n❌ Fatal error:', error);
        process.exit(1);
      });
    break;
    
  case 'analyze':
    // Quick analysis of current state
    (async () => {
      try {
        const stats = await pool.query(`
          SELECT 
            'field_of_law' as tier,
            COUNT(*) as count
          FROM keywords WHERE tier = 'field_of_law'
          UNION ALL
          SELECT 
            'doctrine/concept' as tier,
            COUNT(*) as count
          FROM keywords WHERE tier IN ('major_doctrine', 'legal_concept')
          UNION ALL
          SELECT 
            'already_classified' as tier,
            COUNT(DISTINCT doctrine_or_concept_keyword_id) as count
          FROM keyword_validation
        `);
        
        console.log('📊 Current State Analysis:');
        stats.rows.forEach(row => {
          console.log(`   ${row.tier}: ${row.count}`);
        });
        
      } catch (error) {
        console.error('❌ Analysis error:', error);
      } finally {
        await pool.end();
      }
    })();
    break;
    
  default:
    console.log(`
🎯 NY State Legal Keyword Classification Script

Usage:
  node classifyNYKeywords.js run      # Start classification process
  node classifyNYKeywords.js analyze  # Analyze current state

This script:
  ✅ Classifies legal doctrines/concepts by NY State fields of law
  ✅ Uses parallel processing for optimal performance
  ✅ Creates validation records in keyword_validation table
  ✅ Provides detailed progress tracking and statistics
  ✅ Handles errors gracefully with retry logic

Environment Variables:
  NY_STATE_APPEALS_DB     # PostgreSQL connection string
  OPENAI_API_KEY          # OpenAI API key
  KEYWORD_CONCURRENCY     # Concurrency limit (default: 15)

Example:
  export KEYWORD_CONCURRENCY=20
  node classifyNYKeywords.js run
    `);
    break;
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  await pool.end();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  await pool.end();
  process.exit(0);
});