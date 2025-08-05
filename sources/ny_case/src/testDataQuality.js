import { pullTextForOpinion, cleanText } from './collectTextForOpinion.js';
import { Client } from 'pg';

// Import the KeywordExtractionService to test quality assessment
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Simple version of the quality assessment functions for testing
class DataQualityTester {
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
}

async function testDataQuality() {
  console.log('🔍 Testing Data Quality Assessment');
  console.log('==================================\n');

  const pg = new Client({ connectionString: 'postgresql://localhost/ny_court_of_appeals' });
  await pg.connect();
  
  const tester = new DataQualityTester();

  try {
    // Get a sample of opinions to test
    const sampleOpinions = await pg.query(`
      SELECT
        c.case_name,
        o.id as opinion_id,
        o.binding_type
      FROM opinions o
      INNER JOIN cases c ON o.case_id = c.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
      ORDER BY RANDOM()
      LIMIT 10
    `);
    
    console.log(`Testing ${sampleOpinions.rows.length} random opinions for data quality...\n`);

    let goodQuality = 0;
    let poorQuality = 0;
    
    for (const opinion of sampleOpinions.rows) {
      console.log(`\n📄 Opinion ${opinion.opinion_id}: ${opinion.case_name.substring(0, 50)}...`);
      
      try {
        // Get opinion text
        const opinionText = await pullTextForOpinion(opinion.opinion_id);
        const fullText = opinionText.join('\n');
        
        console.log(`   Original length: ${fullText.length} characters`);
        
        // Pre-process text
        const cleanedText = tester.preprocessText(fullText);
        console.log(`   Cleaned length: ${cleanedText.length} characters`);
        
        // Assess quality
        const qualityAssessment = tester.assessTextQuality(cleanedText);
        
        console.log(`   Quality score: ${(qualityAssessment.score * 100).toFixed(1)}%`);
        console.log(`   Is good quality: ${qualityAssessment.isGoodQuality ? '✅ YES' : '❌ NO'}`);
        
        if (qualityAssessment.isGoodQuality) {
          goodQuality++;
        } else {
          poorQuality++;
          console.log(`   Quality issues:`, Object.entries(qualityAssessment.details)
            .filter(([key, value]) => !value)
            .map(([key]) => key)
            .join(', '));
        }
        
        console.log(`   Stats:`, qualityAssessment.stats);
        
        // Show a sample of the cleaned text
        if (cleanedText.length > 200) {
          console.log(`   Sample text: "${cleanedText.substring(0, 200)}..."`);
        }
        
      } catch (error) {
        console.log(`   ❌ Error processing opinion: ${error.message}`);
        poorQuality++;
      }
    }
    
    console.log('\n' + '='.repeat(50));
    console.log('📊 DATA QUALITY SUMMARY');
    console.log('='.repeat(50));
    console.log(`✅ Good Quality: ${goodQuality} opinions (${(goodQuality / sampleOpinions.rows.length * 100).toFixed(1)}%)`);
    console.log(`❌ Poor Quality: ${poorQuality} opinions (${(poorQuality / sampleOpinions.rows.length * 100).toFixed(1)}%)`);
    console.log('='.repeat(50));
    
    if (poorQuality > 0) {
      console.log('\n💡 The enhanced keyword extraction system will:');
      console.log('   • Skip poor quality opinions automatically');
      console.log('   • Clean OCR artifacts before processing');
      console.log('   • Let AI assess content quality as backup');
      console.log('   • Provide detailed quality metrics in logs');
    }

  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await pg.end();
  }
}

testDataQuality().catch(console.error);
