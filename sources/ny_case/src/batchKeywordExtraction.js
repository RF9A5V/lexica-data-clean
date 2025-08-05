import { pullTextForOpinion, cleanText } from './collectTextForOpinion.js';
import cliProgress from 'cli-progress';
import pLimit from 'p-limit';
import { Client } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { OpenAI } from 'openai';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../lexica_backend/.env') });

// Load system prompt for keyword extraction
const systemPrompt = fs.readFileSync(path.join(__dirname, 'keyword_extraction.md'), 'utf-8');

// Configuration
const CONFIG = {
  batchSize: parseInt(process.argv[2]) || 50,
  concurrency: parseInt(process.argv[3]) || 3, // Increased default concurrency
  minTextLength: 500,
  maxRetries: 3,
  baseDelayMs: 1000,
  dbUrl: 'postgresql://localhost/ny_court_of_appeals',
  // Parallel processing options
  enableParallel: true,
  duplicateCheckInterval: 100, // Check for duplicates every N opinions
  maxWorkers: parseInt(process.env.MAX_WORKERS) || 5 // Maximum parallel workers (configurable via env)
};

class KeywordExtractionService {
  constructor() {
    this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    this.stats = {
      processed: 0,
      successful: 0,
      failed: 0,
      skipped: 0,
      lowQuality: 0,
      totalKeywords: 0,
      duplicatesSkipped: 0,
      startTime: Date.now()
    };
    // Track processed opinions to prevent duplicates
    this.processedOpinions = new Set();
    this.processingOpinions = new Set(); // Currently being processed
  }

  // Check if opinion already has keywords to prevent duplicates
  async hasExistingKeywords(pg, opinionId) {
    try {
      const result = await pg.query(
        'SELECT COUNT(*) as count FROM opinion_keywords WHERE opinion_id = $1',
        [opinionId]
      );
      return parseInt(result.rows[0].count) > 0;
    } catch (error) {
      console.warn(`[WARN] Could not check existing keywords for opinion ${opinionId}: ${error.message}`);
      return false;
    }
  }

  // Get opinions that need keyword extraction (excluding those already processed)
  async getOpinionsToProcess(pg, limit) {
    const result = await pg.query(`
      SELECT 
        c.case_name,
        o.id as opinion_id,
        o.binding_type
      FROM opinions o
      INNER JOIN cases c ON o.case_id = c.id
      LEFT JOIN opinion_keywords ok ON o.id = ok.opinion_id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
        AND ok.opinion_id IS NULL  -- Only opinions without existing keywords
      ORDER BY o.id
      LIMIT $1
    `, [limit]);
    
    return result.rows;
  }

  // Pre-process text to clean OCR artifacts and assess quality
  preprocessText(text) {
    // Clean common OCR artifacts
    let cleaned = text
      // Remove excessive whitespace and normalize
      .replace(/\s+/g, ' ')
      // Remove obvious OCR artifacts (random single characters, malformed words)
      .replace(/\b[a-zA-Z]\s+[a-zA-Z]\s+[a-zA-Z]\b/g, '') // scattered letters
      .replace(/[^\w\s.,;:!?()\[\]"'-]/g, ' ') // remove special characters except punctuation
      // Remove lines that are mostly numbers or single characters
      .split('\n')
      .filter(line => {
        const words = line.trim().split(/\s+/);
        const validWords = words.filter(word => word.length > 2 && /^[a-zA-Z]/.test(word));
        return validWords.length > words.length * 0.5; // At least 50% valid words
      })
      .join('\n')
      .trim();

    return cleaned;
  }

  // Assess text quality before extraction
  assessTextQuality(text) {
    const lines = text.split('\n').filter(line => line.trim().length > 10);
    const words = text.split(/\s+/).filter(word => word.length > 2);
    const sentences = text.split(/[.!?]+/).filter(s => s.trim().length > 20);
    
    // Quality indicators
    const hasLegalTerms = /\b(court|judge|opinion|ruling|case|law|legal|statute|defendant|plaintiff|appeal|motion|judgment)\b/i.test(text);
    const hasSubstantiveContent = sentences.length >= 5;
    const hasProperStructure = lines.length >= 3;
    const wordDensity = words.length / Math.max(text.length, 1) * 1000; // words per 1000 chars
    const avgWordLength = words.reduce((sum, word) => sum + word.length, 0) / Math.max(words.length, 1);
    
    // OCR quality indicators
    const hasExcessiveNumbers = (text.match(/\d/g) || []).length > text.length * 0.3;
    const hasFragmentedWords = (text.match(/\b[a-zA-Z]{1,2}\b/g) || []).length > words.length * 0.4;
    const hasRepeatedChars = /([a-zA-Z])\1{4,}/.test(text);
    
    const qualityScore = {
      hasLegalTerms,
      hasSubstantiveContent,
      hasProperStructure,
      wordDensity: wordDensity > 3 && wordDensity < 8, // reasonable word density
      avgWordLength: avgWordLength > 3 && avgWordLength < 12, // reasonable word length
      notExcessiveNumbers: !hasExcessiveNumbers,
      notFragmented: !hasFragmentedWords,
      notRepeated: !hasRepeatedChars
    };
    
    const positiveIndicators = Object.values(qualityScore).filter(Boolean).length;
    const isGoodQuality = positiveIndicators >= 6; // At least 6 out of 8 quality indicators
    
    return {
      isGoodQuality,
      score: positiveIndicators / 8,
      details: qualityScore,
      stats: {
        lines: lines.length,
        words: words.length,
        sentences: sentences.length,
        wordDensity: wordDensity.toFixed(2),
        avgWordLength: avgWordLength.toFixed(1)
      }
    };
  }

  async extractKeywords(opinionText, maxRetries = CONFIG.maxRetries) {
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const response = await this.openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: opinionText }
          ],
          temperature: 0.1,
          response_format: { type: "json_object" }
        });
        
        const content = response.choices[0].message.content;
        const parsed = JSON.parse(content);
        
        // Validate response structure
        if (!this.validateKeywordResponse(parsed)) {
          throw new Error('Invalid keyword response structure');
        }
        
        return parsed;
      } catch (error) {
        const isRateLimit = error.status === 429 || (error.message && error.message.includes('429'));
        const isParseError = error instanceof SyntaxError;
        
        if (attempt === maxRetries || (!isRateLimit && !isParseError)) {
          throw error;
        }
        
        const delay = CONFIG.baseDelayMs * Math.pow(2, attempt) + Math.floor(Math.random() * 1000);
        console.warn(`[WARN] OpenAI API error (attempt ${attempt + 1}/${maxRetries}): ${error.message}. Retrying in ${delay}ms...`);
        await new Promise(res => setTimeout(res, delay));
      }
    }
  }

  validateKeywordResponse(data) {
    const requiredCategories = [
      'legal_doctrines',
      'causes_of_action', 
      'procedural_terms',
      'subject_matter',
      'factual_contexts',
      'remedies_and_relief'
    ];

    // Check if this is a low-quality content response
    if (data.content_quality === 'insufficient') {
      // Validate that all categories are empty arrays
      for (const category of requiredCategories) {
        if (!data[category] || !Array.isArray(data[category]) || data[category].length > 0) {
          return false;
        }
      }
      return true;
    }

    // Standard validation for normal content
    for (const category of requiredCategories) {
      if (!data[category] || !Array.isArray(data[category])) {
        return false;
      }
    }
    return true;
  }

  async insertKeywords(pg, opinionId, keywordData) {
    // Handle low-quality content response
    if (keywordData.content_quality === 'insufficient') {
      console.log(`[INFO] Opinion ${opinionId}: Low-quality content detected, skipping keyword extraction`);
      return 0;
    }
    
    const keywordsToInsert = [];
    
    // Flatten all keyword categories
    const allCategories = [
      'legal_doctrines',
      'causes_of_action', 
      'procedural_terms',
      'subject_matter',
      'factual_contexts',
      'remedies_and_relief'
    ];
    
    for (const category of allCategories) {
      if (keywordData[category] && Array.isArray(keywordData[category])) {
        for (const item of keywordData[category]) {
          if (item.keyword && item.relevance >= 0.5) {
            keywordsToInsert.push({
              keyword_text: item.keyword.toLowerCase().trim(),
              relevance_score: item.relevance,
              context: item.context || '',
              category: category
            });
          }
        }
      }
    }
    
    if (keywordsToInsert.length === 0) {
      console.warn(`[WARN] No valid keywords extracted for opinion ${opinionId}`);
      return 0;
    }
    
    // Begin transaction
    await pg.query('BEGIN');
    
    try {
      let insertedCount = 0;
      for (const keywordData of keywordsToInsert) {
        // Get or create keyword
        const keywordResult = await pg.query(
          'SELECT get_or_create_keyword($1) as keyword_id',
          [keywordData.keyword_text]
        );
        const keywordId = keywordResult.rows[0].keyword_id;
        
        // Insert opinion-keyword relationship
        const insertResult = await pg.query(`
          INSERT INTO opinion_keywords (opinion_id, keyword_id, relevance_score, extraction_method, category, context)
          VALUES ($1, $2, $3, 'llm_generated', $4, $5)
          ON CONFLICT (opinion_id, keyword_id) DO UPDATE SET
            relevance_score = GREATEST(opinion_keywords.relevance_score, EXCLUDED.relevance_score),
            category = EXCLUDED.category,
            context = EXCLUDED.context
          RETURNING id
        `, [opinionId, keywordId, keywordData.relevance_score, keywordData.category, keywordData.context]);
        
        if (insertResult.rows.length > 0) {
          insertedCount++;
        }
      }
      
      await pg.query('COMMIT');
      this.stats.totalKeywords += insertedCount;
      return insertedCount;
      
    } catch (error) {
      await pg.query('ROLLBACK');
      throw error;
    }
  }

  async processOpinion(pg, opinionData, bar) {
    const { case_name, opinion_id } = opinionData;
    
    // Check if already being processed or completed
    if (this.processingOpinions.has(opinion_id) || this.processedOpinions.has(opinion_id)) {
      console.log(`[DEBUG] Opinion ${opinion_id} already processed or in progress, skipping`);
      this.stats.duplicatesSkipped++;
      return;
    }
    
    // Mark as being processed
    this.processingOpinions.add(opinion_id);
    
    try {
      this.stats.processed++;
      
      // Progress bar will be updated by the main loop
      
      // Double-check database for existing keywords
      const hasKeywords = await this.hasExistingKeywords(pg, opinion_id);
      if (hasKeywords) {
        console.log(`[DEBUG] Opinion ${opinion_id} already has keywords in database, skipping`);
        this.stats.duplicatesSkipped++;
        return;
      }
      
      // Get opinion text
      const opinionText = await pullTextForOpinion(opinion_id);
      const fullText = opinionText.join('\n');
      
      // Skip very short opinions
      if (fullText.length < CONFIG.minTextLength) {
        console.log(`[DEBUG] Skipping short opinion ${opinion_id} (${fullText.length} chars)`);
        this.stats.skipped++;
        return;
      }
      
      // Pre-process text to clean OCR artifacts
      const cleanedText = this.preprocessText(fullText);
      
      // Assess text quality
      const qualityAssessment = this.assessTextQuality(cleanedText);
      
      if (!qualityAssessment.isGoodQuality) {
        console.log(`[DEBUG] Skipping low-quality opinion ${opinion_id} (quality score: ${qualityAssessment.score.toFixed(2)})`);
        console.log(`[DEBUG] Quality details:`, qualityAssessment.details);
        this.stats.skipped++;
        return;
      }
      
      // Extract keywords using cleaned text
      const keywordData = await this.extractKeywords(cleanedText);
      if (!keywordData) {
        this.stats.failed++;
        return;
      }
      
      // Handle low-quality content response
      if (keywordData.content_quality === 'insufficient') {
        console.log(`[INFO] Opinion ${opinion_id}: AI detected insufficient content quality`);
        this.stats.lowQuality++;
        return;
      }
      
      // Insert keywords
      const insertedCount = await this.insertKeywords(pg, opinion_id, keywordData);
      
      if (insertedCount > 0) {
        this.stats.successful++;
        console.log(`[SUCCESS] Opinion ${opinion_id}: ${insertedCount} keywords extracted`);
        // Mark as successfully processed
        this.processedOpinions.add(opinion_id);
      } else {
        this.stats.skipped++;
      }
      
    } catch (error) {
      this.stats.failed++;
      console.error(`[ERROR] Failed to process opinion ${opinion_id}: ${error.message}`);
    } finally {
      // Always remove from processing set
      this.processingOpinions.delete(opinion_id);
    }
  }

  printStats() {
    const duration = (Date.now() - this.stats.startTime) / 1000;
    const rate = this.stats.processed / duration;
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 KEYWORD EXTRACTION SUMMARY');
    console.log('='.repeat(60));
    console.log(`⏱️  Duration: ${duration.toFixed(1)}s (${rate.toFixed(2)} opinions/sec)`);
    console.log(`📈 Processed: ${this.stats.processed} opinions`);
    console.log(`✅ Successful: ${this.stats.successful} opinions`);
    console.log(`❌ Failed: ${this.stats.failed} opinions`);
    console.log(`⏭️  Skipped: ${this.stats.skipped} opinions (too short/low quality)`);
    console.log(`🔍 Low Quality: ${this.stats.lowQuality} opinions (AI detected)`);
    console.log(`🔄 Duplicates Skipped: ${this.stats.duplicatesSkipped} opinions (already processed)`);
    console.log(`🔑 Total Keywords: ${this.stats.totalKeywords}`);
    console.log(`📊 Avg Keywords/Opinion: ${(this.stats.totalKeywords / Math.max(this.stats.successful, 1)).toFixed(1)}`);
    console.log('='.repeat(60));
  }
}

async function main() {
  console.log('🚀 Starting Batch Keyword Extraction');
  console.log(`📋 Configuration: ${CONFIG.batchSize} opinions, ${CONFIG.concurrency} concurrent`);
  
  // Validate OpenAI API key
  if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
    console.error('❌ OpenAI API key not configured. Please set OPENAI_API_KEY in your .env file.');
    process.exit(1);
  }

  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();
  
  const service = new KeywordExtractionService();

  try {
    // Use the new method to get opinions without duplicates
    const opinions = await service.getOpinionsToProcess(pg, CONFIG.batchSize);
    console.log(`📚 Found ${opinions.length} opinions to process`);

    if (opinions.length === 0) {
      console.log('✅ No opinions found that need keyword processing');
      await pg.end();
      return;
    }
    
    // Enhanced parallel processing configuration
    const actualConcurrency = Math.min(CONFIG.concurrency, CONFIG.maxWorkers, opinions.length);
    console.log(`⚡ Using ${actualConcurrency} parallel workers for processing`);
    
    if (CONFIG.enableParallel && opinions.length > 1) {
      console.log('🔄 Parallel processing enabled with duplicate prevention');
    }

    // Progress bar setup
    const bar = new cliProgress.SingleBar({
      format: 'Processing |{bar}| {percentage}% | {value}/{total} | {case_name} | ✅{success} ❌{failed} 🔄{duplicates}',
      hideCursor: true
    }, cliProgress.Presets.shades_classic);
    
    bar.start(opinions.length, 0, {
      case_name: 'Initializing...',
      success: 0,
      failed: 0,
      duplicates: 0
    });

    // Set up concurrency limiter with enhanced duplicate prevention
    const limit = pLimit(actualConcurrency);

    const tasks = opinions.map(row =>
      limit(async () => {
        await service.processOpinion(pg, row, bar);
        // Update progress bar with current stats
        bar.update(service.stats.processed, {
          case_name: row.case_name.substring(0, 40) + '...',
          success: service.stats.successful,
          failed: service.stats.failed,
          duplicates: service.stats.duplicatesSkipped
        });
      })
    );

    await Promise.all(tasks);
    bar.stop();
    
    // Print final statistics
    service.printStats();
    
    // Print database statistics
    const dbStats = await pg.query(`
      SELECT 
        COUNT(DISTINCT ok.opinion_id) as opinions_with_keywords,
        COUNT(*) as total_keyword_assignments,
        AVG(ok.relevance_score) as avg_relevance,
        COUNT(DISTINCT k.keyword_text) as unique_keywords
      FROM opinion_keywords ok
      JOIN keywords k ON ok.keyword_id = k.id
    `);
    
    console.log('\n📊 DATABASE STATISTICS');
    console.log(`Opinions with keywords: ${dbStats.rows[0].opinions_with_keywords}`);
    console.log(`Total keyword assignments: ${dbStats.rows[0].total_keyword_assignments}`);
    console.log(`Average relevance score: ${parseFloat(dbStats.rows[0].avg_relevance || 0).toFixed(3)}`);
    console.log(`Unique keywords in database: ${dbStats.rows[0].unique_keywords}`);
    
  } catch (error) {
    console.error('❌ Batch processing failed:', error);
  } finally {
    await pg.end();
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n[INFO] Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

main().catch(console.error);
