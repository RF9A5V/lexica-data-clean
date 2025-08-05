import OpenAI from 'openai';
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export class NaturalLanguageQueryService {
  constructor(connectionString = 'postgresql://localhost/ny_court_of_appeals') {
    this.connectionString = connectionString;
    this.openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
    this.pg = null;
  }

  async connect() {
    this.pg = new pg.Pool({ connectionString: this.connectionString });
  }

  async disconnect() {
    if (this.pg) {
      await this.pg.end();
    }
  }

  /**
   * Phase 0: Query Type Detection
   * Analyze user query to determine search intent
   */
  async detectQueryType(query) {
    const prompt = `You are a legal search expert. Analyze this query and determine the primary search intent:

Query: "${query}"

Classify this query into ONE of these types:
- factual_similarity: Looking for cases with similar facts/circumstances
- legal_doctrine: Seeking legal principles, rules, or doctrines
- procedural_focus: Interested in court procedures, motions, or process
- outcome_based: Wanting cases with specific results or remedies
- broad_exploration: General legal research without specific focus

Return JSON: {"query_type": "factual_similarity", "confidence": 0.9, "reasoning": "brief explanation"}`;

    try {
      const response = await this.openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        response_format: { type: "json_object" }
      });

      return JSON.parse(response.choices[0].message.content);
    } catch (error) {
      console.error('Error detecting query type:', error);
      return { query_type: 'broad_exploration', confidence: 0.5, reasoning: 'fallback' };
    }
  }

  /**
   * Phase 1: Field of Law Identification
   * Identify relevant legal fields from natural language
   */
  async identifyFieldsOfLaw(query) {
    const fields = await this.getFieldKeywords();
    
    const prompt = `You are a legal field classification expert. Given this user query, identify which legal fields are relevant:

Query: "${query}"

Available Fields of Law:
${fields.map(f => `- ${f.keyword_text}`).join('\n')}

Return JSON with:
- "positive_fields": [array of relevant field names]
- "negative_fields": [array of irrelevant field names to exclude]
- "confidence": 0-1 score

Example: {"positive_fields": ["tort law", "employment law"], "negative_fields": ["criminal law"], "confidence": 0.85}`;

    try {
      const response = await this.openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        response_format: { type: "json_object" }
      });

      const result = JSON.parse(response.choices[0].message.content);
      return {
        positive_fields: result.positive_fields || [],
        negative_fields: result.negative_fields || [],
        confidence: result.confidence || 0.7
      };
    } catch (error) {
      console.error('Error identifying fields:', error);
      return { positive_fields: [], negative_fields: [], confidence: 0.3 };
    }
  }

  /**
   * Phase 2: Legal Doctrine and Concept Extraction
   * Extract legal principles and concepts from natural language
   */
  async extractLegalConcepts(query, queryType) {
    const prompt = `You are a legal concept extraction expert. Extract relevant legal doctrines, concepts, and principles from this query:

Query: "${query}"
Query Type: ${queryType}

Focus on extracting:
- Major legal doctrines
- Specific legal concepts
- Legal standards and tests
- Legal elements
- Procedural terms

Return JSON: {
  "positive_keywords": ["doctrine1", "concept1", "standard1"],
  "negative_keywords": ["exclude_this"],
  "confidence": 0.85
}`;

    try {
      const response = await this.openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        response_format: { type: "json_object" }
      });

      const result = JSON.parse(response.choices[0].message.content);
      return {
        positive_keywords: result.positive_keywords || [],
        negative_keywords: result.negative_keywords || [],
        confidence: result.confidence || 0.7
      };
    } catch (error) {
      console.error('Error extracting concepts:', error);
      return { positive_keywords: [], negative_keywords: [], confidence: 0.3 };
    }
  }

  /**
   * Phase 3: Factual and Procedural Elements
   * Extract case-specific facts and procedural elements
   */
  async extractFactualElements(query, queryType) {
    const prompt = `You are a legal fact extraction expert. Extract factual circumstances and procedural elements from this query:

Query: "${query}"
Query Type: ${queryType}

Extract these elements:
- Distinguishing facts/circumstances
- Procedural posture (motions, appeals, etc.)
- Case outcomes or remedies sought
- Industry or context specifics

Return JSON: {
  "distinguishing_factors": ["fact1", "circumstance1"],
  "procedural_posture": ["motion for summary judgment", "jury trial"],
  "case_outcomes": ["plaintiff verdict", "damages over 500k"],
  "confidence": 0.8
}`;

    try {
      const response = await this.openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        response_format: { type: "json_object" }
      });

      const result = JSON.parse(response.choices[0].message.content);
      return {
        distinguishing_factors: result.distinguishing_factors || [],
        procedural_posture: result.procedural_posture || [],
        case_outcomes: result.case_outcomes || [],
        confidence: result.confidence || 0.7
      };
    } catch (error) {
      console.error('Error extracting factual elements:', error);
      return {
        distinguishing_factors: [],
        procedural_posture: [],
        case_outcomes: [],
        confidence: 0.3
      };
    }
  }

  /**
   * Main processing function - Phase 0-3 combined
   */
  async processNaturalLanguageQuery(query) {
    console.log('🔍 Processing natural language query:', query);
    
    try {
      // Phase 0: Query type detection
      const queryType = await this.detectQueryType(query);
      
      // Phase 1: Field identification
      const fields = await this.identifyFieldsOfLaw(query);
      
      // Phase 2: Legal concepts
      const concepts = await this.extractLegalConcepts(query, queryType.query_type);
      
      // Phase 3: Factual elements
      const facts = await this.extractFactualElements(query, queryType.query_type);
      
      // Convert extracted terms to keyword IDs
      const keywordResults = await this.mapToKeywordIds({
        fields: fields.positive_fields,
        concepts: concepts.positive_keywords,
        facts: {
          distinguishing_factors: facts.distinguishing_factors,
          procedural_posture: facts.procedural_posture,
          case_outcomes: facts.case_outcomes
        }
      });

      return {
        success: true,
        query: query,
        query_types: [queryType.query_type],
        tier_weights: this.calculateTierWeights(queryType.query_type),
        positive_keyword_ids: keywordResults.positive_ids,
        negative_keyword_ids: keywordResults.negative_ids,
        confidence: Math.min(
          queryType.confidence,
          fields.confidence,
          concepts.confidence,
          facts.confidence
        ),
        metadata: {
          fields_identified: fields.positive_fields.length,
          concepts_extracted: concepts.positive_keywords.length,
          facts_extracted: facts.distinguishing_factors.length + facts.procedural_posture.length + facts.case_outcomes.length
        }
      };

    } catch (error) {
      console.error('Error processing query:', error);
      return {
        success: false,
        message: 'Could not process natural language query',
        suggestions: [
          'Try using more specific legal terms',
          'Include the type of legal issue or case',
          'Add factual details about the situation'
        ],
        examples: [
          'employment law cases involving scope of employment',
          'slip and fall accidents in retail stores',
          'breach of contract damages over 100k'
        ]
      };
    }
  }

  /**
   * Calculate tier weights based on query type
   */
  calculateTierWeights(queryType) {
    const weights = {
      field_of_law: 1.0,
      major_doctrine: 1.2,
      legal_concept: 1.2,
      distinguishing_factor: 1.0,
      procedural_posture: 1.0,
      case_outcome: 1.0
    };

    switch (queryType) {
      case 'factual_similarity':
        weights.distinguishing_factor = 1.5;
        weights.case_outcome = 1.3;
        break;
      case 'legal_doctrine':
        weights.major_doctrine = 1.5;
        weights.legal_concept = 1.4;
        break;
      case 'procedural_focus':
        weights.procedural_posture = 1.5;
        break;
      case 'outcome_based':
        weights.case_outcome = 1.5;
        break;
      case 'broad_exploration':
        // Keep default weights
        break;
    }

    return weights;
  }

  /**
   * Map extracted terms to keyword IDs from database
   */
  async mapToKeywordIds(extracted) {
    const positive_ids = [];
    const negative_ids = [];

    // Map fields
    if (extracted.fields?.length > 0) {
      const fieldResult = await this.pg.query(
        'SELECT id FROM keywords WHERE tier = $1 AND keyword_text = ANY($2)',
        ['field_of_law', extracted.fields]
      );
      positive_ids.push(...fieldResult.rows.map(r => r.id));
    }

    // Map concepts
    if (extracted.concepts?.length > 0) {
      const conceptResult = await this.pg.query(
        'SELECT id FROM keywords WHERE tier IN ($1, $2) AND keyword_text = ANY($3)',
        ['major_doctrine', 'legal_concept', extracted.concepts]
      );
      positive_ids.push(...conceptResult.rows.map(r => r.id));
    }

    // Map distinguishing factors
    if (extracted.facts?.distinguishing_factors?.length > 0) {
      const factorResult = await this.pg.query(
        'SELECT id FROM keywords WHERE tier = $1 AND keyword_text = ANY($2)',
        ['distinguishing_factor', extracted.facts.distinguishing_factors]
      );
      positive_ids.push(...factorResult.rows.map(r => r.id));
    }

    // Map procedural posture
    if (extracted.facts?.procedural_posture?.length > 0) {
      const postureResult = await this.pg.query(
        'SELECT id FROM keywords WHERE tier = $1 AND keyword_text = ANY($2)',
        ['procedural_posture', extracted.facts.procedural_posture]
      );
      positive_ids.push(...postureResult.rows.map(r => r.id));
    }

    // Map case outcomes
    if (extracted.facts?.case_outcomes?.length > 0) {
      const outcomeResult = await this.pg.query(
        'SELECT id FROM keywords WHERE tier = $1 AND keyword_text = ANY($2)',
        ['case_outcome', extracted.facts.case_outcomes]
      );
      positive_ids.push(...outcomeResult.rows.map(r => r.id));
    }

    return {
      positive_ids: [...new Set(positive_ids)], // Remove duplicates
      negative_ids: [...new Set(negative_ids)]
    };
  }

  /**
   * Get all field of law keywords for system prompts
   */
  async getFieldKeywords() {
    const result = await this.pg.query(
      "SELECT id, keyword_text FROM keywords WHERE tier = 'field_of_law' ORDER BY keyword_text"
    );
    return result.rows;
  }
}

// CLI interface for testing
if (process.argv[2] === 'test') {
  const service = new NaturalLanguageQueryService();
  
  service.connect()
    .then(() => service.processNaturalLanguageQuery(process.argv.slice(3).join(' ')))
    .then(result => {
      console.log('🎯 Query Processing Result:');
      console.log(JSON.stringify(result, null, 2));
    })
    .catch(error => console.error('❌ Error:', error))
    .finally(() => service.disconnect());
}
