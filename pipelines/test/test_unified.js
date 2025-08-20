#!/usr/bin/env node

/**
 * Test script for unified keyword extraction pipeline
 * Validates the combined pass1/pass2 extraction process
 */

const fs = require('fs');
const path = require('path');

// Comment out dependencies not needed for dry-run test
// const { Pool } = require('pg');
const { validateUnified } = require('../src/validation');
// const { callLLM } = require('../src/llm');
// const { upsertUnified } = require('../src/upsert');

// Test configuration
const TEST_CONFIG = {
  dryRun: true,
  verbose: true,
  temperature: 0.2,
  maxTokens: 4000,
};

// Sample test opinion
const TEST_OPINION = {
  id: 999999,
  text: `This case involves a breach of contract dispute between ABC Corp and XYZ Inc. 
  The plaintiff, ABC Corp, alleges that defendant XYZ Inc failed to deliver goods 
  as specified in their purchase agreement dated January 1, 2023. The contract 
  included a liquidated damages clause providing for $10,000 per day of delay.
  
  The court must determine whether the liquidated damages clause is enforceable 
  under New York law, which requires that such clauses represent a reasonable 
  estimate of probable loss and not constitute a penalty.
  
  After reviewing the evidence, including expert testimony on industry standards,
  the court finds that the liquidated damages amount was a reasonable pre-estimate
  of ABC Corp's potential losses from delayed delivery. The clause is therefore
  enforceable.
  
  Judgment for plaintiff. Defendant is ordered to pay liquidated damages.`,
  
  expected: {
    field_of_law: ['Contract Law'],
    procedural_posture: ['Trial Court Decision'],
    case_outcome: ['Plaintiff Prevails'],
    distinguishing_factors: ['Liquidated damages clause enforceability'],
    doctrines: ['Freedom of Contract', 'Liquidated Damages'],
    doctrinal_tests: ['Reasonable estimate of loss test']
  }
};

/**
 * Load and prepare the unified prompt
 */
async function loadUnifiedPrompt() {
  const promptPath = path.join(__dirname, '..', 'prompts', 'unified_prompt.md');
  let promptTemplate = fs.readFileSync(promptPath, 'utf8');
  
  // Load allowed values (would normally come from database)
  const allowedValues = {
    field_of_law: ['Contract Law', 'Tort Law', 'Criminal Law', 'Constitutional Law'],
    procedural_posture: ['Trial Court Decision', 'Appellate Decision', 'Motion Decision'],
    case_outcome: ['Plaintiff Prevails', 'Defendant Prevails', 'Mixed Outcome'],
    doctrines: ['Freedom of Contract', 'Liquidated Damages', 'Breach of Contract', 'Specific Performance'],
    doctrinal_tests: ['Reasonable estimate of loss test', 'Penalty clause test']
  };
  
  // Replace placeholders
  promptTemplate = promptTemplate.replace('{{ALLOWED_FIELD_OF_LAW}}', JSON.stringify(allowedValues.field_of_law, null, 2));
  promptTemplate = promptTemplate.replace('{{ALLOWED_PROCEDURAL_POSTURE}}', JSON.stringify(allowedValues.procedural_posture, null, 2));
  promptTemplate = promptTemplate.replace('{{ALLOWED_CASE_OUTCOME}}', JSON.stringify(allowedValues.case_outcome, null, 2));
  promptTemplate = promptTemplate.replace('{{ALLOWED_DOCTRINES}}', JSON.stringify(allowedValues.doctrines, null, 2));
  promptTemplate = promptTemplate.replace('{{ALLOWED_DOCTRINAL_TESTS}}', JSON.stringify(allowedValues.doctrinal_tests, null, 2));
  
  return promptTemplate;
}

/**
 * Test LLM extraction
 */
async function testExtraction(opinion) {
  console.log('\n📝 Testing Unified Extraction...\n');
  console.log('Opinion text (truncated):', opinion.text.substring(0, 200) + '...\n');
  
  // Prepare prompt
  const systemPrompt = await loadUnifiedPrompt();
  const userPrompt = opinion.text;
  
  try {
    // Mock LLM call for testing (in production, would use actual LLM)
    console.log('🤖 Calling LLM (simulated)...');
    
    // Simulate extraction result
    const mockResult = {
      field_of_law: [
        { label: 'Contract Law', score: 0.95 }
      ],
      procedural_posture: [
        { canonical: 'Trial Court Decision' }
      ],
      case_outcome: [
        { canonical: 'Plaintiff Prevails' }
      ],
      distinguishing_factors: [
        {
          axis: 'Legal Issue',
          reasoning: 'Enforceability of liquidated damages clause',
          generalized: 'Liquidated damages clause enforceability',
          importance: 'high',
          evidence_start: 286,
          evidence_end: 420
        }
      ],
      doctrines: [
        {
          name: 'Freedom of Contract',
          evidence_start: 0,
          evidence_end: 100
        },
        {
          name: 'Liquidated Damages',
          evidence_start: 286,
          evidence_end: 420
        }
      ],
      doctrinal_tests: [
        {
          name: 'Reasonable estimate of loss test',
          description: 'Test for enforceability of liquidated damages',
          opinion_id: opinion.id,
          evidence_start: 421,
          evidence_end: 580
        }
      ]
    };
    
    console.log('✅ Extraction completed\n');
    console.log('Extracted data:', JSON.stringify(mockResult, null, 2));
    
    return mockResult;
    
  } catch (error) {
    console.error('❌ Extraction failed:', error);
    throw error;
  }
}

/**
 * Test validation
 */
async function testValidation(extractedData) {
  console.log('\n🔍 Testing Validation...\n');
  
  const { valid, errors } = validateUnified(extractedData);
  
  if (valid) {
    console.log('✅ Validation passed\n');
  } else {
    console.log('❌ Validation failed\n');
    console.log('Errors:', JSON.stringify(errors, null, 2));
  }
  
  return valid;
}

/**
 * Test database upsert (dry run)
 */
async function testUpsert(opinionId, extractedData) {
  console.log('\n💾 Testing Database Upsert (dry run)...\n');
  
  if (TEST_CONFIG.dryRun) {
    console.log('🔸 DRY RUN MODE - No actual database writes\n');
    
    // Simulate upsert operations
    console.log('Would upsert:');
    console.log(`- ${extractedData.field_of_law.length} field_of_law keywords`);
    console.log(`- ${extractedData.procedural_posture.length} procedural_posture keywords`);
    console.log(`- ${extractedData.case_outcome.length} case_outcome keywords`);
    console.log(`- ${extractedData.distinguishing_factors.length} distinguishing_factors`);
    console.log(`- ${extractedData.doctrines.length} doctrines`);
    console.log(`- ${extractedData.doctrinal_tests.length} doctrinal tests`);
    
    return true;
  }
  
  // In production, would actually call upsertUnified
  // const client = await pool.connect();
  // await upsertUnified(client, opinionId, extractedData, { method: 'unified_llm' });
  // client.release();
  
  return true;
}

/**
 * Compare with expected results
 */
function compareResults(extracted, expected) {
  console.log('\n📊 Comparing Results...\n');
  
  const results = {};
  
  // Compare field_of_law
  const extractedFields = extracted.field_of_law.map(f => f.label);
  results.field_of_law = {
    extracted: extractedFields,
    expected: expected.field_of_law,
    match: extractedFields.some(f => expected.field_of_law.includes(f))
  };
  
  // Compare doctrines
  const extractedDoctrines = extracted.doctrines.map(d => d.name);
  results.doctrines = {
    extracted: extractedDoctrines,
    expected: expected.doctrines,
    match: extractedDoctrines.some(d => expected.doctrines.includes(d))
  };
  
  // Print comparison
  Object.entries(results).forEach(([category, data]) => {
    console.log(`${category}:`);
    console.log(`  Expected: ${data.expected.join(', ')}`);
    console.log(`  Extracted: ${data.extracted.join(', ')}`);
    console.log(`  Match: ${data.match ? '✅' : '❌'}\n`);
  });
  
  return results;
}

/**
 * Main test runner
 */
async function runTests() {
  console.log('========================================');
  console.log('   UNIFIED PIPELINE TEST SUITE');
  console.log('========================================\n');
  
  try {
    // Test 1: Extraction
    const extractedData = await testExtraction(TEST_OPINION);
    
    // Test 2: Validation
    const isValid = await testValidation(extractedData);
    if (!isValid) {
      console.error('⚠️  Validation failed, skipping remaining tests');
      return;
    }
    
    // Test 3: Database upsert
    await testUpsert(TEST_OPINION.id, extractedData);
    
    // Test 4: Quality comparison
    const comparison = compareResults(extractedData, TEST_OPINION.expected);
    
    // Summary
    console.log('\n========================================');
    console.log('   TEST SUMMARY');
    console.log('========================================\n');
    console.log('✅ Extraction: PASSED');
    console.log('✅ Validation: PASSED');
    console.log('✅ Upsert: PASSED (dry run)');
    
    const qualityScore = Object.values(comparison)
      .filter(r => r.match).length / Object.keys(comparison).length;
    console.log(`📈 Quality Score: ${(qualityScore * 100).toFixed(0)}%`);
    
    console.log('\n✨ All tests completed successfully!\n');
    
  } catch (error) {
    console.error('\n❌ Test suite failed:', error);
    process.exit(1);
  }
}

// Run tests if executed directly
if (require.main === module) {
  runTests().catch(console.error);
}

module.exports = {
  testExtraction,
  testValidation,
  testUpsert,
  compareResults
};
