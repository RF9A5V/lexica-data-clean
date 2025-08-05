// classifyKeywordTiers.js
// Classifies keywords into tiers using LLM
import { Client } from 'pg';
import { OpenAI } from 'openai';
import cliProgress from 'cli-progress';
import pLimit from 'p-limit';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../lexica_backend/.env') });

const CONFIG = {
  batchSize: parseInt(process.argv[2]) || 100,
  concurrency: parseInt(process.argv[3]) || 5,
  maxRetries: 3,
  baseDelayMs: 1000,
  dbUrl: 'postgresql://localhost/ny_court_of_appeals'
};

const TIER_DEFINITIONS = [
  'field_of_law',
  'major_doctrine',
  'legal_concept',
  'distinguishing_factor',
  'procedural_posture',
  'case_outcome'
];

const CLASSIFICATION_PROMPT = `You are a legal keyword classification specialist. Your task is to classify individual legal keywords into exactly one of these tiers:

## Tier Definitions:
- **field_of_law**: Broad practice areas (tort law, contract law, employment law, criminal law, constitutional law, administrative law)
- **major_doctrine**: Foundational legal principles (respondeat superior, proximate cause, strict liability, due process, consideration)
- **legal_concept**: Specific rules within doctrines (scope of employment, foreseeability test, material breach, offer and acceptance)
- **distinguishing_factor**: Case-specific factual elements (delivery driver personal stop, slip and fall wet floor, medical malpractice surgery)
- **procedural_posture**: Case status/motions (summary judgment, motion to dismiss, trial verdict, appellate decision)
- **case_outcome**: Results (plaintiff verdict, defense verdict, settlement, dismissed with prejudice)

Respond with only the tier name. No explanation needed.`;

class KeywordTierClassifier {
  constructor() {
    this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    this.stats = {
      processed: 0,
      classified: 0,
      failed: 0,
      skipped: 0,
      startTime: Date.now()
    };
  }

  async getKeywordsToClassify(pg, limit) {
    const result = await pg.query(`
      SELECT id, keyword_text 
      FROM keywords 
      WHERE tier IS NULL OR tier = ''
      ORDER BY id
      LIMIT $1
    `, [limit]);
    
    return result.rows;
  }

  async classifyKeyword(keywordText, maxRetries = CONFIG.maxRetries) {
    const prompt = `${CLASSIFICATION_PROMPT}\n\nKeyword: ${keywordText}`;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const response = await this.openai.chat.completions.create({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: prompt },
            { role: 'user', content: keywordText }
          ],
          max_tokens: 20,
          temperature: 0.1
        });

        const tier = response.choices[0].message.content.trim().toLowerCase();
        
        if (TIER_DEFINITIONS.includes(tier)) {
          return tier;
        } else {
          console.warn(`[WARN] Invalid tier "${tier}" for keyword "${keywordText}"`);
        }
      } catch (error) {
        if (attempt === maxRetries) {
          console.error(`[ERROR] Failed to classify "${keywordText}": ${error.message}`);
          throw error;
        }
        
        const delay = CONFIG.baseDelayMs * Math.pow(2, attempt - 1);
        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }
    
    return null;
  }

  async updateKeywordTier(pg, keywordId, tier) {
    await pg.query(
      'UPDATE keywords SET tier = $1 WHERE id = $2',
      [tier, keywordId]
    );
  }

  async processKeyword(pg, keywordData, bar) {
    const { id, keyword_text } = keywordData;
    
    try {
      const tier = await this.classifyKeyword(keyword_text);
      
      if (tier) {
        await this.updateKeywordTier(pg, id, tier);
        this.stats.classified++;
      } else {
        this.stats.failed++;
      }
      
      this.stats.processed++;
      
      bar.increment({
        keyword: keyword_text.substring(0, 30) + '...',
        classified: this.stats.classified,
        failed: this.stats.failed
      });
      
    } catch (error) {
      this.stats.failed++;
      console.error(`[ERROR] Processing keyword "${keyword_text}": ${error.message}`);
    }
  }

  printStats() {
    const duration = (Date.now() - this.stats.startTime) / 1000;
    console.log('\n📊 CLASSIFICATION STATISTICS');
    console.log(`Total keywords processed: ${this.stats.processed}`);
    console.log(`Successfully classified: ${this.stats.classified}`);
    console.log(`Failed classifications: ${this.stats.failed}`);
    console.log(`Processing time: ${duration.toFixed(2)}s`);
    console.log(`Average time per keyword: ${(duration / Math.max(this.stats.processed, 1)).toFixed(2)}s`);
  }
}

async function main() {
  console.log('🚀 Starting Keyword Tier Classification');
  console.log(`📋 Processing up to ${CONFIG.batchSize} keywords with ${CONFIG.concurrency} concurrent workers`);
  
  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();
  
  const classifier = new KeywordTierClassifier();
  
  try {
    const keywords = await classifier.getKeywordsToClassify(pg, CONFIG.batchSize);
    console.log(`📚 Found ${keywords.length} keywords to classify`);
    
    if (keywords.length === 0) {
      console.log('✅ All keywords already classified');
      return;
    }
    
    const bar = new cliProgress.SingleBar({
      format: 'Classifying |{bar}| {percentage}% | {value}/{total} | {keyword} | ✅{classified} ❌{failed}',
      hideCursor: true
    }, cliProgress.Presets.shades_classic);
    
    bar.start(keywords.length, 0, {
      keyword: 'Starting...',
      classified: 0,
      failed: 0
    });
    
    const limit = pLimit(CONFIG.concurrency);
    const tasks = keywords.map(keyword =>
      limit(() => classifier.processKeyword(pg, keyword, bar))
    );
    
    await Promise.all(tasks);
    bar.stop();
    
    classifier.printStats();
    
    const tierStats = await pg.query(`
      SELECT tier, COUNT(*) as count 
      FROM keywords 
      WHERE tier IS NOT NULL 
      GROUP BY tier 
      ORDER BY count DESC
    `);
    
    console.log('\n📈 TIER DISTRIBUTION');
    tierStats.rows.forEach(row => {
      console.log(`${row.tier}: ${row.count} keywords`);
    });
    
  } catch (error) {
    console.error('❌ Classification failed:', error);
  } finally {
    await pg.end();
  }
}

main().catch(console.error);
