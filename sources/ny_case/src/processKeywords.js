import { pullTextForOpinion, cleanText } from './collectTextForOpinion.js';
import cliProgress from 'cli-progress';
import pLimit from 'p-limit';
import { Client, Pool } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { OpenAI } from 'openai';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

// Load system prompt for keyword extraction
const systemPrompt = fs.readFileSync(path.join(__dirname, 'keyword_extraction.md'), 'utf-8');

async function getOpinionText(opinionId) {
  const text = await pullTextForOpinion(opinionId);
  const cleanedText = cleanText(text.join('\n'));
  return cleanedText;
}

async function extractKeywords(opinionText, maxRetries = 5, baseDelayMs = 1000) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await openai.chat.completions.create({
        model: 'gpt-4o-mini', // More cost-effective for keyword extraction
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: opinionText }
        ],
        temperature: 0.1, // Low temperature for consistent keyword extraction
        response_format: { type: "json_object" } // Ensure JSON response
      });
      
      const content = response.choices[0].message.content;
      return JSON.parse(content);
    } catch (error) {
      const isRateLimit = error.status === 429 || (error.message && error.message.includes('429'));
      const isParseError = error instanceof SyntaxError;
      
      if (attempt === maxRetries || (!isRateLimit && !isParseError)) {
        throw error;
      }
      
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.floor(Math.random() * 1000);
      console.warn(`[WARN] OpenAI API error (attempt ${attempt + 1}/${maxRetries}): ${error.message || error}. Retrying in ${delay}ms...`);
      await new Promise(res => setTimeout(res, delay));
    }
  }
  throw new Error('extractKeywords: All retries failed');
}

async function insertKeywords(pool, opinionId, keywordData) {
  const keywordsToInsert = [];
  
  // Tier categories matching database constraint exactly
  const tierCategories = [
    'field_of_law',
    'major_doctrine',
    'legal_concept',
    'distinguishing_factor',
    'procedural_posture',
    'case_outcome'
  ];
  
  for (const tier of tierCategories) {
    if (keywordData[tier] && Array.isArray(keywordData[tier])) {
      for (const keywordText of keywordData[tier]) {
        if (keywordText && keywordText.trim()) {
          keywordsToInsert.push({
            keyword_text: keywordText.toLowerCase().trim(),
            tier: tier,
            category: tier // Keep category for opinion_keywords tracking
          });
        }
      }
    }
  }
  
  if (keywordsToInsert.length === 0) {
    console.warn(`[WARN] No valid keywords extracted for opinion ${opinionId}`);
    return;
  }
  
  // Get a client from the pool for this transaction
  const client = await pool.connect();
  
  try {
    // Begin transaction
    await client.query('BEGIN');
    
    for (const keywordData of keywordsToInsert) {
      // Update keyword with tier information
      const keywordResult = await client.query(
        `INSERT INTO keywords (keyword_text, tier) 
         VALUES ($1, $2) 
         ON CONFLICT (keyword_text) 
         DO UPDATE SET tier = EXCLUDED.tier 
         RETURNING id`,
        [keywordData.keyword_text, keywordData.tier]
      );
      const keywordId = keywordResult.rows[0].id;
      
      // Insert opinion-keyword relationship
      await client.query(`
        INSERT INTO opinion_keywords (opinion_id, keyword_id, extraction_method)
        VALUES ($1, $2, 'llm_generated')
        ON CONFLICT (opinion_id, keyword_id) DO NOTHING
      `, [opinionId, keywordId]);
    }
    
    await client.query('COMMIT');
    
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

async function retryKeywordExtraction(opinionText, maxRetries = 3) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const keywords = await extractKeywords(opinionText);
      
      // Validate the response structure
      if (!keywords || typeof keywords !== 'object') {
        throw new Error('Invalid keyword response structure');
      }
      
      return keywords;
    } catch (error) {
      console.warn(`[WARN] Keyword extraction attempt ${attempt + 1} failed: ${error.message}`);
      if (attempt === maxRetries - 1) {
        console.error(`[ERROR] All keyword extraction attempts failed for opinion`);
        return null;
      }
    }
  }
}

async function main() {
  // Use connection pool for better concurrency
  const poolConfig = {
    connectionString: 'postgresql://localhost/ny_court_of_appeals',
    max: 10, // Maximum number of clients in the pool
    idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
    connectionTimeoutMillis: 2000, // Return error after 2 seconds if connection could not be established
  };
  const pool = new Pool(poolConfig);

  // Get opinions that don't have keywords yet
  const batchSize = process.argv[2] || 100;
  const opinionsQuery = `
    SELECT
      c.case_name,
      o.id as opinion_id,
      c.citation_count
    FROM opinions o
    INNER JOIN cases c ON o.case_id = c.id
    LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
    WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
      AND ok.opinion_id IS NULL  -- Only opinions without keywords
    ORDER BY c.citation_count DESC, o.id
    LIMIT ${batchSize}  -- Process in batches, configurable via command line
  `;
  
  const opinionsResult = await pool.query(opinionsQuery);
  console.log(`Found ${opinionsResult.rows.length} opinions to process for keyword extraction`);

  if (opinionsResult.rows.length === 0) {
    console.log('No opinions found that need keyword processing');
    await pool.end();
    return;
  }

  // Progress bar setup
  const bar = new cliProgress.SingleBar({
    format: 'Processing |{bar}| {percentage}% | {value}/{total} Opinions | {case_name}',
    hideCursor: true
  }, cliProgress.Presets.shades_classic);
  bar.start(opinionsResult.rows.length, 0);

  // Advanced concurrency optimization:
  // - OpenAI API: Can handle 10-20 concurrent requests with exponential backoff
  // - Database Pool: 10 connections available for parallel transactions
  // - Text processing: CPU-bound but relatively fast
  // - Memory: Each opinion text ~50KB, 15 concurrent = ~750KB memory usage
  const concurrencyLimit = parseInt(process.env.KEYWORD_CONCURRENCY) || 15;
  console.log(`[INFO] Using concurrency limit: ${concurrencyLimit} (pool max: ${poolConfig.max})`);
  
  // Performance monitoring
  let processedCount = 0;
  let errorCount = 0;
  const startTime = Date.now();
  
  // Separate limits for different bottlenecks
  const apiLimit = pLimit(concurrencyLimit); // OpenAI API calls
  const dbLimit = pLimit(poolConfig.max);     // Database operations

  const tasks = opinionsResult.rows.map(row =>
    apiLimit(async () => {
      const { case_name, opinion_id } = row;
      
      try {
        bar.increment(1, { case_name: case_name.substring(0, 50) + '...' });
        
        const opinionText = await getOpinionText(opinion_id);
        
        // Skip very short opinions (likely not substantial)
        if (opinionText.length < 500) {
          console.log(`[DEBUG] Skipping short opinion ${opinion_id} (${opinionText.length} chars)`);
          return;
        }
        
        // Use API limit for OpenAI calls
        const keywordData = await retryKeywordExtraction(opinionText);
        if (!keywordData) return;
        
        // Use database limit for database operations
        await dbLimit(() => insertKeywords(pool, opinion_id, keywordData));
        processedCount++;
        
      } catch (error) {
        console.error(`[ERROR] Failed to process opinion ${opinion_id}: ${error.message}`);
        errorCount++;
      }
    })
  );

  await Promise.all(tasks);

  bar.stop();
  
  // Performance summary
  const endTime = Date.now();
  const totalTime = (endTime - startTime) / 1000;
  const avgTimePerOpinion = totalTime / processedCount;
  const throughput = processedCount / (totalTime / 60); // opinions per minute
  
  console.log('\n=== Performance Summary ===');
  console.log(`Total time: ${totalTime.toFixed(2)}s`);
  console.log(`Processed: ${processedCount} opinions`);
  console.log(`Errors: ${errorCount} opinions`);
  console.log(`Success rate: ${((processedCount / (processedCount + errorCount)) * 100).toFixed(1)}%`);
  console.log(`Average time per opinion: ${avgTimePerOpinion.toFixed(2)}s`);
  console.log(`Throughput: ${throughput.toFixed(1)} opinions/minute`);
  
  // Print summary statistics
  const statsQuery = `
    SELECT 
      COUNT(DISTINCT ok.opinion_id) as opinions_with_keywords,
      COUNT(*) as total_keyword_assignments,
      AVG(ok.relevance_score) as avg_relevance,
      COUNT(DISTINCT k.keyword_text) as unique_keywords
    FROM opinion_keywords ok
    JOIN keywords k ON ok.keyword_id = k.id
  `;
  
  const stats = await pool.query(statsQuery);
  console.log('\n=== Keyword Extraction Summary ===');
  console.log(`Opinions with keywords: ${stats.rows[0].opinions_with_keywords}`);
  console.log(`Total keyword assignments: ${stats.rows[0].total_keyword_assignments}`);
  console.log(`Average relevance score: ${parseFloat(stats.rows[0].avg_relevance).toFixed(3)}`);
  console.log(`Unique keywords: ${stats.rows[0].unique_keywords}`);
  
  await pool.end();
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n[INFO] Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

main().catch(console.error);