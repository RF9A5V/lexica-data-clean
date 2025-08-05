#!/usr/bin/env node

/**
 * Process specific substantial opinions for keyword extraction demonstration
 */

import { Client } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { OpenAI } from 'openai';
import cliProgress from 'cli-progress';
import pLimit from 'p-limit';
import { pullTextForOpinion } from './collectTextForOpinion.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../lexica_backend/.env') });

// Load system prompt for keyword extraction
const systemPrompt = fs.readFileSync(path.join(__dirname, 'keyword_extraction.md'), 'utf-8');

// Configuration
const CONFIG = {
  concurrency: 4,
  minTextLength: 500,
  maxRetries: 3,
  baseDelayMs: 1000,
  dbUrl: 'postgresql://localhost/ny_court_of_appeals',
  // Specific substantial opinions to process
  targetOpinions: [2822, 2824, 2826, 2889, 2964, 3007]
};

class TargetedKeywordExtraction {
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
    this.processedOpinions = new Set();
    this.processingOpinions = new Set();
  }

  // Pre-process text to clean OCR artifacts
  preprocessText(text) {
    let cleaned = text
      .replace(/\s+/g, ' ')
      .replace(/\b[a-zA-Z]\s+[a-zA-Z]\s+[a-zA-Z]\b/g, '')
      .replace(/[^\w\s.,;:!?()\[\]"'-]/g, ' ')
      .split('\n')
      .filter(line => {
        const words = line.trim().split(/\s+/);
        const validWords = words.filter(word => word.length > 2 && /^[a-zA-Z]/.test(word));
        return validWords.length > words.length * 0.5;
      })
      .join('\n')
      .trim();

    return cleaned;
  }

  // Assess text quality
  assessTextQuality(text) {
    const lines = text.split('\n').filter(line => line.trim().length > 10);
    const words = text.split(/\s+/).filter(word => word.length > 2);
    const sentences = text.split(/[.!?]+/).filter(s => s.trim().length > 20);
    
    const hasLegalTerms = /\b(court|judge|opinion|ruling|case|law|legal|statute|defendant|plaintiff|appeal|motion|judgment)\b/i.test(text);
    const hasSubstantiveContent = sentences.length >= 5;
    const hasProperStructure = lines.length >= 3;
    const wordDensity = words.length / Math.max(text.length, 1) * 1000;
    const avgWordLength = words.reduce((sum, word) => sum + word.length, 0) / Math.max(words.length, 1);
    
    const hasExcessiveNumbers = (text.match(/\d/g) || []).length > text.length * 0.3;
    const hasFragmentedWords = (text.match(/\b[a-zA-Z]{1,2}\b/g) || []).length > words.length * 0.4;
    const hasRepeatedChars = /([a-zA-Z])\1{4,}/.test(text);
    
    const qualityScore = {
      hasLegalTerms,
      hasSubstantiveContent,
      hasProperStructure,
      wordDensity: wordDensity > 3 && wordDensity < 8,
      avgWordLength: avgWordLength > 3 && avgWordLength < 12,
      notExcessiveNumbers: !hasExcessiveNumbers,
      notFragmented: !hasFragmentedWords,
      notRepeated: !hasRepeatedChars
    };
    
    const positiveIndicators = Object.values(qualityScore).filter(Boolean).length;
    const isGoodQuality = positiveIndicators >= 6;
    
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

  async extractKeywords(text) {
    for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
      try {
        const response = await this.openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: text }
          ],
          temperature: 0.1,
          response_format: { type: "json_object" }
        });

        const content = response.choices[0].message.content;
        return JSON.parse(content);
      } catch (error) {
        console.warn(`[WARN] Attempt ${attempt}/${CONFIG.maxRetries} failed: ${error.message}`);
        if (attempt < CONFIG.maxRetries) {
          const delay = CONFIG.baseDelayMs * Math.pow(2, attempt - 1);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          throw error;
        }
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
      return true; // Valid low-quality response
    }

    // Validate that all required categories exist
    for (const category of requiredCategories) {
      if (!data.hasOwnProperty(category)) {
        console.warn(`[WARN] Missing category: ${category}`);
        return false;
      }
      if (!Array.isArray(data[category])) {
        console.warn(`[WARN] Category ${category} is not an array`);
        return false;
      }
    }

    return true;
  }

  async insertKeywords(pg, opinionId, keywordData) {
    const keywordsToInsert = [];
    
    // Skip the keywords property wrapper and access categories directly
    const categories = {
      'legal_doctrines': keywordData.legal_doctrines || [],
      'causes_of_action': keywordData.causes_of_action || [],
      'procedural_terms': keywordData.procedural_terms || [],
      'subject_matter': keywordData.subject_matter || [],
      'factual_contexts': keywordData.factual_contexts || [],
      'remedies_and_relief': keywordData.remedies_and_relief || []
    };
    
    for (const [category, keywords] of Object.entries(categories)) {
      if (Array.isArray(keywords)) {
        for (const item of keywords) {
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
    
    await pg.query('BEGIN');
    
    try {
      let insertedCount = 0;
      for (const keywordData of keywordsToInsert) {
        const keywordResult = await pg.query(
          'SELECT get_or_create_keyword($1) as keyword_id',
          [keywordData.keyword_text]
        );
        const keywordId = keywordResult.rows[0].keyword_id;
        
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
    
    if (this.processingOpinions.has(opinion_id) || this.processedOpinions.has(opinion_id)) {
      console.log(`[DEBUG] Opinion ${opinion_id} already processed or in progress, skipping`);
      this.stats.duplicatesSkipped++;
      return;
    }
    
    this.processingOpinions.add(opinion_id);
    
    try {
      this.stats.processed++;
      
      const opinionText = await pullTextForOpinion(opinion_id);
      const fullText = opinionText.join('\n');
      
      console.log(`[INFO] Processing opinion ${opinion_id}: "${case_name}" (${fullText.length} chars)`);
      
      if (fullText.length < CONFIG.minTextLength) {
        console.log(`[DEBUG] Skipping short opinion ${opinion_id} (${fullText.length} chars)`);
        this.stats.skipped++;
        return;
      }
      
      const cleanedText = this.preprocessText(fullText);
      const qualityAssessment = this.assessTextQuality(cleanedText);
      
      console.log(`[INFO] Opinion ${opinion_id} quality score: ${(qualityAssessment.score * 100).toFixed(1)}%`);
      
      if (!qualityAssessment.isGoodQuality) {
        console.log(`[DEBUG] Skipping low-quality opinion ${opinion_id} (quality score: ${qualityAssessment.score.toFixed(2)})`);
        this.stats.skipped++;
        return;
      }
      
      const keywordData = await this.extractKeywords(cleanedText);
      if (!keywordData) {
        this.stats.failed++;
        return;
      }
      
      // Validate response structure
      if (!this.validateKeywordResponse(keywordData)) {
        console.log(`[ERROR] Opinion ${opinion_id}: Invalid keyword response structure`);
        this.stats.failed++;
        return;
      }
      
      if (keywordData.content_quality === 'insufficient') {
        console.log(`[INFO] Opinion ${opinion_id}: AI detected insufficient content quality`);
        this.stats.lowQuality++;
        return;
      }
      
      const insertedCount = await this.insertKeywords(pg, opinion_id, keywordData);
      
      if (insertedCount > 0) {
        this.stats.successful++;
        console.log(`[SUCCESS] Opinion ${opinion_id}: ${insertedCount} keywords extracted`);
        this.processedOpinions.add(opinion_id);
      } else {
        this.stats.skipped++;
      }
      
    } catch (error) {
      this.stats.failed++;
      console.error(`[ERROR] Failed to process opinion ${opinion_id}: ${error.message}`);
    } finally {
      this.processingOpinions.delete(opinion_id);
    }
  }

  printStats() {
    const duration = (Date.now() - this.stats.startTime) / 1000;
    const rate = this.stats.processed / duration;
    
    console.log('\n' + '='.repeat(60));
    console.log('📊 TARGETED KEYWORD EXTRACTION SUMMARY');
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
  console.log('🎯 Targeted Keyword Extraction for Substantial Opinions');
  console.log(`📋 Processing ${CONFIG.targetOpinions.length} specific opinions with ${CONFIG.concurrency} workers`);
  
  if (!process.env.OPENAI_API_KEY || process.env.OPENAI_API_KEY === 'your_openai_api_key_here') {
    console.error('❌ OpenAI API key not configured. Please set OPENAI_API_KEY in your .env file.');
    process.exit(1);
  }

  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();
  
  const service = new TargetedKeywordExtraction();

  try {
    // Get the specific opinions
    const opinions = await pg.query(`
      SELECT 
        c.case_name,
        o.id as opinion_id,
        o.binding_type
      FROM opinions o
      INNER JOIN cases c ON o.case_id = c.id
      WHERE o.id = ANY($1)
      ORDER BY o.id
    `, [CONFIG.targetOpinions]);

    console.log(`📚 Found ${opinions.rows.length} target opinions to process\n`);

    if (opinions.rows.length === 0) {
      console.log('❌ No target opinions found');
      await pg.end();
      return;
    }

    // Progress bar setup
    const bar = new cliProgress.SingleBar({
      format: 'Processing |{bar}| {percentage}% | {value}/{total} | {case_name} | ✅{success} ❌{failed} 🔄{duplicates}',
      hideCursor: true
    }, cliProgress.Presets.shades_classic);
    
    bar.start(opinions.rows.length, 0, {
      case_name: 'Initializing...',
      success: 0,
      failed: 0,
      duplicates: 0
    });

    // Set up concurrency limiter
    const limit = pLimit(CONFIG.concurrency);

    const tasks = opinions.rows.map(row =>
      limit(async () => {
        await service.processOpinion(pg, row, bar);
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
    console.error('❌ Error:', error.message);
  } finally {
    await pg.end();
  }
}

main().catch(console.error);
