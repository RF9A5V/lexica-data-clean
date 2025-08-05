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
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

// Load system prompt for analyzeOpinion from ./analysis.md
const systemPrompt = fs.readFileSync(path.join(__dirname, 'analysis.md'), 'utf-8');

async function getOpinionAnalysis(opinionId) {
  const text = await pullTextForOpinion(opinionId);
  const cleanedText = cleanText(text.join('\n'));

  return cleanedText;
}

async function analyzeOpinion(opinionText, maxRetries = 5, baseDelayMs = 1000) {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await openai.chat.completions.create({
        model: 'gpt-4.1-nano',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: opinionText }
        ],
        temperature: 0.2
      });
      return response.choices[0].message.content;
    } catch (error) {
      const isRateLimit = error.status === 429 || (error.message && error.message.includes('429'));
      if (attempt === maxRetries || !isRateLimit) {
        throw error;
      }
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.floor(Math.random() * 1000);
      console.warn(`[WARN] OpenAI API error (attempt ${attempt + 1}/${maxRetries}): ${error.message || error}. Retrying in ${delay}ms...`);
      await new Promise(res => setTimeout(res, delay));
    }
  }
  throw new Error('analyzeOpinion: All retries failed');
}


async function main() {
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  const citedCaseOpinions = await pg.query(`SELECT 
        cases.id AS case_id,
        cases.case_name,
        cases.citation_count,
        opinions.id AS opinion_id,
        opinions.binding_type,
        COUNT(DISTINCT opinion_paragraphs.id) AS paragraph_count,
        COUNT(DISTINCT opinion_sentences.id) AS sentence_count
    FROM opinions
    INNER JOIN cases ON opinions.case_id = cases.id
    LEFT JOIN opinion_paragraphs ON opinions.id = opinion_paragraphs.opinion_id
    LEFT JOIN opinion_sentences ON opinions.id = opinion_sentences.opinion_id
    WHERE opinions.binding_type IN ('015unanimous', '010combined', '020lead')
    GROUP BY cases.id, cases.citation_count, opinions.id, opinions.binding_type
    HAVING COUNT(DISTINCT opinion_sentences.id) = 0
      AND COUNT(DISTINCT opinion_paragraphs.id) > 1
    ORDER BY cases.citation_count DESC, opinions.binding_type, opinions.id;`);

  console.log(citedCaseOpinions.rows.length);

  // Get classifications
  const classifications = await pg.query('SELECT * FROM firac_classifications');

  let classificationMap = {};

  for (const row of classifications.rows) {
    if(!classificationMap[row.category]) {
      classificationMap[row.category] = {};
    }
    classificationMap[row.category][row.subcategory || ''] = row.id;
  }

  async function retryAnalysis(opinionText, retryCount = 3) {
    if(retryCount === 0) {
      console.log(`[ERROR] Failed to analyze opinion after ${retryCount} retries`);
      return null;
    }
    let analysis;
    try {
      analysis = await analyzeOpinion(opinionText);
      const sentencesToInsert = await processAnalysisMarkdown(analysis);
      return sentencesToInsert;
    } catch (error) {
      console.log(`[ERROR] Failed to analyze opinion: ${error.message}`);
      return retryAnalysis(opinionText, retryCount - 1);
    }
  }

  async function processAnalysisMarkdown(analysisText) {
    const sentences = analysisText.split('\n');
  
    let category = "";
    let subcategory = "";
    const sentencesToInsert = [];
  
    for (const sentence of sentences) {
      if (sentence.startsWith('## ')) {
        category = sentence.slice(3).trim();
        subcategory = "";
      } else if (sentence.startsWith('### ')) {
        subcategory = sentence.slice(4).trim();
      } else if (sentence.startsWith('- ')) {
        let classificationId = classificationMap[category][subcategory];
        if(!classificationId) {
          classificationId = classificationMap[category][''];
        }
        if(!classificationId) {
          console.log("why is this fucked")
          console.log(`classificationMap: ${JSON.stringify(classificationMap, null, 2)}`);
          console.log(`category: ${category}, subcat obj: ${JSON.stringify(classificationMap[category], null, 2)}`);
          console.log(`subcategory: ${subcategory}, subcat id: ${classificationMap[category][subcategory]}`);
          console.log(`[ERROR] No category/subcategory found for sentence: ${sentence}`);
          throw new Error(`No category/subcategory found for sentence: ${sentence}`);
        }
        const cleanedSentence = sentence.slice(2);
        sentencesToInsert.push({
          classification_id: classificationId,
          sentence_text: cleanedSentence
        });
      }
    }
  
    return sentencesToInsert;
  }

  // Progress bar setup
  const bar = new cliProgress.SingleBar({
    format: 'Processing |{bar}| {percentage}% | {value}/{total} Opinions | {case_name}',
    hideCursor: true
  }, cliProgress.Presets.shades_classic);
  bar.start(citedCaseOpinions.rows.length, 0);

  // Set up concurrency limiter for max 4 active requests
  const limit = pLimit(4);

  const tasks = citedCaseOpinions.rows.map(row =>
    limit(async () => {
      const { case_name, opinion_id } = row;
      bar.increment(1, { case_name });
      const text = await getOpinionAnalysis(opinion_id);
      const sentencesToInsert = await retryAnalysis(text);
      if (!sentencesToInsert) return;
      const sentencesWithClassification = sentencesToInsert.map(sentence => {
        const { classification_id, sentence_text } = sentence;
        return [opinion_id, classification_id, sentence_text];
      });
      if (!sentencesWithClassification.length) return; // Guard: skip empty inserts
      const valueStrings = sentencesWithClassification.map((_, i) => `($${i * 3 + 1}, $${i * 3 + 2}, $${i * 3 + 3})`).join(',');
      const insertQuery = `INSERT INTO opinion_sentences (opinion_id, classification_id, sentence_text) VALUES ${valueStrings}`;
      await pg.query(insertQuery, sentencesWithClassification.flat());
      console.log(`[DEBUG] Inserted ${sentencesWithClassification.length} sentences for ${case_name}.`);
    })
  );

  await Promise.all(tasks);

  bar.stop();
  await pg.end();
}

main();