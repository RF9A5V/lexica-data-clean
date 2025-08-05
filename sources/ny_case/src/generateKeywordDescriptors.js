import { Client, Pool } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { OpenAI } from 'openai';
import pLimit from 'p-limit';
import cliProgress from 'cli-progress';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

// Load system prompt for descriptor generation
const systemPrompt = fs.readFileSync(path.join(__dirname, 'keyword_descriptor_prompt.md'), 'utf-8');

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost/ny_court_of_appeals'
});

// OpenAI client
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Rate limiting
const limit = pLimit(parseInt(process.env.CONCURRENCY_LIMIT || '5'));

// Progress bar
const progressBar = new cliProgress.SingleBar({
  format: 'Progress |{bar}| {percentage}% | {value}/{total} | ETA: {eta}s',
  barCompleteChar: '█',
  barIncompleteChar: '░',
  hideCursor: true
});

/**
 * Generate plain language descriptors for a legal keyword
 */
async function generateDescriptors(keywordText) {
  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: keywordText }
      ],
      temperature: 0.1,
      response_format: { type: "json_object" }
    });

    const content = response.choices[0].message.content;
    let descriptors = JSON.parse(content);
    
    // Expect object format with descriptors key
    if (!descriptors || typeof descriptors !== 'object') {
      throw new Error('Response is not a JSON object');
    }
    
    if (!Array.isArray(descriptors.descriptors)) {
      throw new Error('Response does not contain descriptors array');
    }
    
    // Clean and validate the descriptors
    const actualDescriptors = descriptors.descriptors
      .filter(d => typeof d === 'string' && d.trim().length > 0)
      .map(d => d.trim());
    
    if (actualDescriptors.length === 0) {
      throw new Error('No valid descriptors found');
    }
    
    descriptors = actualDescriptors;
    
    return descriptors;
  } catch (error) {
    console.error(`[ERROR] Failed to generate descriptors for "${keywordText}": ${error.message}`);
    return null;
  }
}

/**
 * Check if a keyword already has descriptors
 */
async function hasDescriptors(keywordId) {
  const result = await pool.query(
    'SELECT COUNT(*) FROM keyword_descriptors WHERE keyword_id = $1',
    [keywordId]
  );
  return parseInt(result.rows[0].count) > 0;
}

/**
 * Insert descriptors for a keyword
 */
async function insertDescriptors(keywordId, descriptors) {
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    for (const descriptor of descriptors) {
      await client.query(
        'INSERT INTO keyword_descriptors (keyword_id, descriptor_text) VALUES ($1, $2) ON CONFLICT DO NOTHING',
        [keywordId, descriptor.trim()]
      );
    }
    
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Process a single keyword
 */
async function processKeyword(keyword) {
  try {
    // Skip if already has descriptors
    if (await hasDescriptors(keyword.id)) {
      return { processed: false, reason: 'already_has_descriptors' };
    }

    const descriptors = await generateDescriptors(keyword.keyword_text);
    
    if (!descriptors || descriptors.length === 0) {
      return { processed: false, reason: 'no_descriptors_generated' };
    }

    await insertDescriptors(keyword.id, descriptors);
    
    return { 
      processed: true, 
      keyword: keyword.keyword_text,
      tier: keyword.tier,
      descriptor_count: descriptors.length 
    };
  } catch (error) {
    console.error(`[ERROR] Failed to process keyword "${keyword.keyword_text}": ${error.message}`);
    return { processed: false, reason: error.message };
  }
}

/**
 * Main function to generate descriptors for all relevant keywords
 */
async function generateAllDescriptors(batchSize = 50, concurrency = 3) {
  console.log('🚀 Starting keyword descriptor generation...');
  
  try {
    // Get keywords that need descriptors (major_doctrine and legal_concept tiers)
    const keywordsResult = await pool.query(`
      SELECT k.id, k.keyword_text, k.tier
      FROM keywords k
      WHERE k.tier IN ('major_doctrine', 'legal_concept')
      AND NOT EXISTS (
        SELECT 1 FROM keyword_descriptors kd 
        WHERE kd.keyword_id = k.id
      )
      ORDER BY k.id
      LIMIT $1
    `, [batchSize]);

    const keywords = keywordsResult.rows;
    
    if (keywords.length === 0) {
      console.log('✅ All keywords already have descriptors!');
      return;
    }

    console.log(`📋 Processing ${keywords.length} keywords...`);
    progressBar.start(keywords.length, 0);

    const limit = pLimit(concurrency);
    let processed = 0;
    let skipped = 0;
    let errors = 0;

    const results = await Promise.all(
      keywords.map(keyword => 
        limit(async () => {
          const result = await processKeyword(keyword);
          progressBar.increment();
          
          if (result.processed) {
            processed++;
          } else if (result.reason === 'already_has_descriptors') {
            skipped++;
          } else {
            errors++;
          }
          
          return result;
        })
      )
    );

    progressBar.stop();

    // Summary
    console.log('\n📊 Summary:');
    console.log(`✅ Processed: ${processed} keywords`);
    console.log(`⏭️  Skipped: ${skipped} keywords (already have descriptors)`);
    console.log(`❌ Errors: ${errors} keywords`);

    // Show sample results
    const successful = results.filter(r => r.processed);
    if (successful.length > 0) {
      console.log('\n🔍 Sample results:');
      successful.slice(0, 3).forEach(r => {
        console.log(`  • "${r.keyword}" (${r.tier}): ${r.descriptor_count} descriptors`);
      });
    }

  } catch (error) {
    console.error('💥 Fatal error:', error.message);
  } finally {
    await pool.end();
  }
}

// CLI handling
const [,, batchSize = '50', concurrency = '3'] = process.argv;

if (import.meta.url === `file://${process.argv[1]}`) {
  generateAllDescriptors(parseInt(batchSize), parseInt(concurrency));
}

export { generateAllDescriptors, generateDescriptors };