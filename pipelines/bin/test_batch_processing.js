#!/usr/bin/env node

// Test utility for batch processing error handling and partial failures
// Simulates various error scenarios to validate batch processing robustness

const fs = require('fs');
const path = require('path');
const common = require('../src/common');
const batchPersistence = require('../src/batch_persistence');

function showUsage() {
  console.log(`
Usage: node bin/test_batch_processing.js [options]

Test utility for batch processing error handling and partial failures.
Creates mock batch results with various error scenarios.

Options:
  --db <connection>         Database connection string
  --batch-id <id>          Batch ID to test (creates mock results)
  --scenario <type>        Error scenario to test:
                           - mixed_results (default)
                           - all_api_errors
                           - all_validation_errors
                           - malformed_json
                           - partial_content
                           - network_errors
  --debug                  Enable debug logging

Test Scenarios:
  mixed_results:     Mix of successful, failed, and error responses
  all_api_errors:    All requests return API errors
  all_validation_errors: All requests fail schema validation
  malformed_json:    All requests return malformed JSON
  partial_content:   Some requests missing content
  network_errors:    Simulate network/timeout errors

Examples:
  # Test mixed success/failure scenario
  node bin/test_batch_processing.js --db ny_reporter --batch-id batch_test123

  # Test all validation errors
  node bin/test_batch_processing.js --db ny_reporter --batch-id batch_test123 --scenario all_validation_errors

  # Test malformed JSON responses
  node bin/test_batch_processing.js --db ny_reporter --batch-id batch_test123 --scenario malformed_json
`);
}

// Mock batch result generators for different error scenarios
const mockResultGenerators = {
  mixed_results: (opinionIds) => {
    return opinionIds.map((id, index) => {
      const customId = `opinion_${id}`;
      
      if (index % 4 === 0) {
        // API error
        return {
          custom_id: customId,
          error: {
            code: 'rate_limit_exceeded',
            message: 'Rate limit exceeded. Please try again later.'
          }
        };
      } else if (index % 4 === 1) {
        // Validation error (missing required fields)
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{
                message: {
                  content: JSON.stringify({
                    fl: ['Constitutional Law'], // Missing required fields
                    v: false
                  })
                }
              }]
            }
          }
        };
      } else if (index % 4 === 2) {
        // Valueless opinion
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{
                message: {
                  content: JSON.stringify({
                    v: true,
                    vr: 'Procedural order with no substantive legal analysis'
                  })
                }
              }]
            }
          }
        };
      } else {
        // Successful extraction
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{
                message: {
                  content: JSON.stringify({
                    fl: ['Constitutional Law', 'Civil Rights'],
                    pp: ['Motion to Dismiss'],
                    co: ['Granted'],
                    df: ['Standing'],
                    h: [{
                      i: 'Whether plaintiff has standing to challenge the statute',
                      h: 'Plaintiff lacks standing due to insufficient injury',
                      r: 'Standing requires concrete and particularized injury',
                      rs: 'No evidence of actual harm to plaintiff',
                      pv: 'High',
                      c: 0.85
                    }],
                    oc: [{
                      cn: 'Smith v. Jones',
                      ct: '123 F.3d 456 (2d Cir. 1999)',
                      s: 'Complete',
                      ol: 'We expressly overrule Smith v. Jones and reject its holding'
                    }],
                    v: false
                  })
                }
              }]
            }
          }
        };
      }
    });
  },

  all_api_errors: (opinionIds) => {
    return opinionIds.map(id => ({
      custom_id: `opinion_${id}`,
      error: {
        code: 'invalid_request_error',
        message: 'The model produced invalid output'
      }
    }));
  },

  all_validation_errors: (opinionIds) => {
    return opinionIds.map(id => ({
      custom_id: `opinion_${id}`,
      response: {
        body: {
          choices: [{
            message: {
              content: JSON.stringify({
                // Missing all required fields
                some_random_field: 'invalid data',
                v: false
              })
            }
          }]
        }
      }
    }));
  },

  malformed_json: (opinionIds) => {
    return opinionIds.map(id => ({
      custom_id: `opinion_${id}`,
      response: {
        body: {
          choices: [{
            message: {
              content: '{ "fl": ["Constitutional Law", incomplete json...'
            }
          }]
        }
      }
    }));
  },

  partial_content: (opinionIds) => {
    return opinionIds.map((id, index) => {
      const customId = `opinion_${id}`;
      
      if (index % 2 === 0) {
        // Missing content
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{}] // No message content
            }
          }
        };
      } else {
        // Valid content
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{
                message: {
                  content: JSON.stringify({
                    fl: ['Contract Law'],
                    pp: ['Summary Judgment'],
                    co: ['Denied'],
                    df: ['Material Fact'],
                    h: [],
                    oc: [],
                    v: false
                  })
                }
              }]
            }
          }
        };
      }
    });
  },

  network_errors: (opinionIds) => {
    return opinionIds.map((id, index) => {
      const customId = `opinion_${id}`;
      
      if (index % 3 === 0) {
        return {
          custom_id: customId,
          error: {
            code: 'timeout',
            message: 'Request timed out'
          }
        };
      } else if (index % 3 === 1) {
        return {
          custom_id: customId,
          error: {
            code: 'connection_error',
            message: 'Connection reset by peer'
          }
        };
      } else {
        // Successful
        return {
          custom_id: customId,
          response: {
            body: {
              choices: [{
                message: {
                  content: JSON.stringify({
                    fl: ['Tort Law'],
                    pp: ['Trial'],
                    co: ['Plaintiff Verdict'],
                    df: ['Negligence'],
                    h: [],
                    oc: [],
                    v: false
                  })
                }
              }]
            }
          }
        };
      }
    });
  }
};

async function createMockBatchJob(dbClient, batchId, opinionCount = 10) {
  // Create a test batch job
  const batchJobData = {
    batch_id: batchId,
    status: 'completed',
    total_requests: opinionCount,
    database_name: 'test_database',
    limit_count: opinionCount,
    resume_mode: true,
    dry_run: true,
    input_file_id: 'file-test123',
    output_file_id: 'file-test456',
    description: `Test batch - ${opinionCount} opinions`,
    created_by: 'test_script'
  };

  const batchJob = await batchPersistence.createBatchJob(dbClient, batchJobData);
  
  // Create mock opinion requests
  const opinions = Array.from({ length: opinionCount }, (_, i) => ({
    id: 1000 + i // Use high IDs to avoid conflicts
  }));
  
  await batchPersistence.createBatchOpinionRequests(dbClient, batchJob.id, opinions);
  
  return { batchJob, opinionIds: opinions.map(o => o.id) };
}

async function testBatchProcessing(dbClient, batchId, scenario, debug = false) {
  console.log(`\n🧪 Testing Batch Processing - Scenario: ${scenario}`);
  console.log('=' .repeat(60));
  
  // Check if batch exists, create if not
  let batchJob = await batchPersistence.getBatchJob(dbClient, batchId);
  let opinionIds;
  
  if (!batchJob) {
    console.log(`[test] Creating mock batch job: ${batchId}`);
    const mockData = await createMockBatchJob(dbClient, batchId);
    batchJob = mockData.batchJob;
    opinionIds = mockData.opinionIds;
  } else {
    // Get existing opinion IDs
    const result = await dbClient.query(
      'SELECT opinion_id FROM batch_opinion_requests WHERE batch_job_id = $1 ORDER BY opinion_id',
      [batchJob.id]
    );
    opinionIds = result.rows.map(r => r.opinion_id);
  }
  
  console.log(`[test] Batch job ID: ${batchJob.id}`);
  console.log(`[test] Opinion count: ${opinionIds.length}`);
  
  // Generate mock results based on scenario
  const generator = mockResultGenerators[scenario];
  if (!generator) {
    throw new Error(`Unknown scenario: ${scenario}`);
  }
  
  const mockResults = generator(opinionIds);
  console.log(`[test] Generated ${mockResults.length} mock results`);
  
  // Import the processBatchResults function
  const { processBatchResults } = require('./run_unified_batch');
  
  // Process the mock results
  const args = { dryRun: true, debug };
  
  console.log('\n📊 Processing Mock Results...');
  console.log('-'.repeat(40));
  
  try {
    const stats = await processBatchResults(dbClient, mockResults, args, batchId);
    
    console.log('\n✅ Processing Complete!');
    console.log('📈 Final Statistics:');
    console.log(`  Total Processed: ${stats.processed}`);
    console.log(`  Successful: ${stats.success}`);
    console.log(`  Valueless Flagged: ${stats.valuelessFlagged}`);
    console.log(`  Validation Errors: ${stats.validationErrors}`);
    console.log(`  Database Errors: ${stats.dbErrors}`);
    
    // Get detailed batch statistics
    const batchStats = await batchPersistence.getBatchStats(dbClient, batchId);
    if (batchStats) {
      console.log('\n📋 Database Statistics:');
      console.log(`  Total Opinions: ${batchStats.total_opinions}`);
      console.log(`  Completed: ${batchStats.completed_opinions}`);
      console.log(`  Failed: ${batchStats.failed_opinions}`);
      console.log(`  Successful Extractions: ${batchStats.successful_extractions}`);
      console.log(`  Validation Errors: ${batchStats.validation_errors}`);
      console.log(`  Upsert Errors: ${batchStats.upsert_errors}`);
    }
    
    // Show error breakdown by type
    const errorBreakdown = await dbClient.query(`
      SELECT error_code, COUNT(*) as count, 
             array_agg(opinion_id ORDER BY opinion_id) as opinion_ids
      FROM batch_opinion_requests 
      WHERE batch_job_id = $1 AND error_code IS NOT NULL
      GROUP BY error_code
      ORDER BY count DESC
    `, [batchJob.id]);
    
    if (errorBreakdown.rows.length > 0) {
      console.log('\n🚨 Error Breakdown:');
      for (const row of errorBreakdown.rows) {
        console.log(`  ${row.error_code}: ${row.count} opinions`);
        if (debug) {
          console.log(`    Opinion IDs: ${row.opinion_ids.slice(0, 5).join(', ')}${row.opinion_ids.length > 5 ? '...' : ''}`);
        }
      }
    }
    
    return stats;
    
  } catch (error) {
    console.error('\n❌ Processing Failed:');
    console.error(`  Error: ${error.message}`);
    if (debug) {
      console.error(`  Stack: ${error.stack}`);
    }
    throw error;
  }
}

async function runAllScenarios(dbClient, baseBatchId, debug = false) {
  console.log('\n🧪 Running All Test Scenarios');
  console.log('=' .repeat(60));
  
  const scenarios = Object.keys(mockResultGenerators);
  const results = {};
  
  for (const scenario of scenarios) {
    const batchId = `${baseBatchId}_${scenario}`;
    
    try {
      const stats = await testBatchProcessing(dbClient, batchId, scenario, debug);
      results[scenario] = { success: true, stats };
    } catch (error) {
      results[scenario] = { success: false, error: error.message };
    }
    
    console.log('\n' + '─'.repeat(60));
  }
  
  // Summary report
  console.log('\n📊 Test Summary Report');
  console.log('=' .repeat(60));
  
  for (const [scenario, result] of Object.entries(results)) {
    if (result.success) {
      const stats = result.stats;
      console.log(`✅ ${scenario}:`);
      console.log(`   Processed: ${stats.processed}, Success: ${stats.success}, Errors: ${stats.validationErrors + stats.dbErrors}`);
    } else {
      console.log(`❌ ${scenario}: ${result.error}`);
    }
  }
}

async function main() {
  const args = common.parseCommonArgs(process.argv);
  
  if (args.help) {
    showUsage();
    return;
  }
  
  if (!args.db) {
    console.error('Error: --db is required');
    showUsage();
    process.exit(1);
  }
  
  const dbClient = common.makePgClient(args.db);
  
  try {
    const batchId = args.batchId || `test_batch_${Date.now()}`;
    const scenario = args.scenario || 'mixed_results';
    
    if (args.scenario === 'all') {
      await runAllScenarios(dbClient, batchId, args.debug);
    } else {
      await testBatchProcessing(dbClient, batchId, scenario, args.debug);
    }
    
  } finally {
    await dbClient.end();
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}

module.exports = {
  testBatchProcessing,
  mockResultGenerators,
  createMockBatchJob
};
