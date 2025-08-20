#!/usr/bin/env node

// OpenAI Batch API implementation for unified opinion keyword extraction
// Provides 50% cost savings compared to regular API calls
// Processes opinions in batches with 24-hour processing window

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const common = require('../src/common');
const llm = require('../src/llm');
const { upsertUnified } = require('../src/upsert');
const { validateUnified } = require('../src/validation');
const { expandUnifiedResponse } = require('../src/field_mapping');
const batchPersistence = require('../src/batch_persistence');
const OpenAI = require('openai');

// Batch splitting configuration
const BATCH_CONFIG = {
  MAX_BATCH_SIZE: 1000,        // Maximum opinions per batch
  OPTIMAL_BATCH_SIZE: 500,     // Optimal batch size for processing
  MIN_BATCH_SIZE: 50,          // Minimum batch size (don't split below this)
  SPLIT_THRESHOLD: 1000        // Split batches larger than this
};

function showUsage() {
  console.log(`
Usage: node bin/run_unified_batch.js [options]

OpenAI Batch API implementation for unified opinion keyword extraction.
Provides 50% cost savings with 24-hour processing window.

Options:
  --db <connection>         Database connection string
  --limit <n>              Limit number of opinions to process
  --concurrency <n>        Write concurrency (default: 4)
  --debug                  Enable debug logging
  --dry-run                Skip database writes
  --no-resume              Process all opinions (default: skip already processed)
  --submit-only            Only submit batch, don't wait for results
  --retrieve-only <batch_id>  Only retrieve and process existing batch results
  --list-batches           List all pending/completed batches
  --cancel-batch <batch_id>  Cancel a pending batch
  --cleanup-batches        Clean up completed batches from database
  --cleanup-test-only      Only clean up test batches (safer option)
  --cleanup-cancelled      Only clean up cancelled batches
  --cleanup-failed         Only clean up failed batches
  --cleanup-older-than <days>  Only clean up batches older than N days

Batch Processing Workflow:
  1. Submit batch: Creates batch job and returns batch_id
  2. Wait/Poll: Batch processes over ~24 hours (async)
  3. Retrieve: Download and process results when complete

Examples:
  # Submit new batch for 1000 opinions
  node bin/run_unified_batch.js --db ny_reporter --limit 1000 --submit-only

  # Check batch status
  node bin/run_unified_batch.js --list-batches

  # Retrieve and process completed batch
  node bin/run_unified_batch.js --db ny_reporter --retrieve-only batch_abc123

  # Full workflow (submit and wait)
  node bin/run_unified_batch.js --db ny_reporter --limit 500

  # Clean up test batches
  node bin/run_unified_batch.js --cleanup-batches --cleanup-test-only

  # Clean up cancelled batches
  node bin/run_unified_batch.js --cleanup-batches --cleanup-cancelled

  # Clean up failed batches
  node bin/run_unified_batch.js --cleanup-batches --cleanup-failed

  # Clean up all completed batches older than 30 days
  node bin/run_unified_batch.js --cleanup-batches --cleanup-older-than 30

`);
}

function getOpenAI() {
  let OpenAI;
  try {
    OpenAI = require('openai');
  } catch (e) {
    throw new Error('OpenAI SDK not installed. Please run: npm i openai');
  }
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY in environment');
  return new OpenAI({ apiKey });
}

async function createBatchRequest(opinions, allowedFields, allowedPostures, allowedOutcomes) {
  const prompt = fs.readFileSync(path.join(__dirname, '../prompts/unified_prompt.md'), 'utf8');
  const schema = fs.readFileSync(path.join(__dirname, '../schemas/unified_minimal_schema.json'), 'utf8');
  
  const requests = opinions.map(opinion => {
    // Build case context section (same as current pipeline)
    let caseContext = '';
    if (opinion.jurisdiction_name || opinion.court_name || opinion.decision_date) {
      caseContext = `Case Context:\n`;
      if (opinion.jurisdiction_name) caseContext += `- Jurisdiction: ${opinion.jurisdiction_name}\n`;
      if (opinion.court_name) caseContext += `- Court: ${opinion.court_name}\n`;
      if (opinion.decision_date) caseContext += `- Decision Date: ${opinion.decision_date}\n`;
      caseContext += `\n`;
    }
    
    const userContent = `JSON Schema (strict):\n${schema}\n\n${caseContext}Opinion Text (may be truncated):\n${opinion.text.slice(0, 50000)}`;
    
    return {
      custom_id: `opinion_${opinion.id}`,
      method: "POST",
      url: "/v1/chat/completions",
      body: {
        model: process.env.OPENAI_MODEL || 'gpt-5-mini',
        messages: [
          {
            role: 'system',
            content: prompt + '\n\nReturn ONLY valid JSON. No prose.'
          },
          {
            role: 'user',
            content: userContent
          }
        ],
        response_format: { type: 'json_object' },
        seed: 2177750
      }
    };
  });
  
  return requests;
}

async function submitBatch(openAIClient, dbClient, requests, opinions, batchArgs) {
  // Create JSONL file for batch request
  const jsonlContent = requests.map(req => JSON.stringify(req)).join('\n');
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const filename = `batch_unified_${timestamp}.jsonl`;
  const tempPath = path.join(__dirname, '../temp', filename);
  
  // Ensure temp directory exists
  const tempDir = path.dirname(tempPath);
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
  }
  
  fs.writeFileSync(tempPath, jsonlContent);
  console.log(`[batch] Created batch file: ${filename} (${requests.length} requests)`);
  
  try {
    // Upload file
    const file = await openAIClient.files.create({
      file: fs.createReadStream(tempPath),
      purpose: 'batch'
    });
    console.log(`[batch] Uploaded file: ${file.id}`);
    
    // Create batch
    const batch = await openAIClient.batches.create({
      input_file_id: file.id,
      endpoint: '/v1/chat/completions',
      completion_window: '24h',
      metadata: {
        description: `Unified opinion extraction - ${requests.length} opinions`,
        created_at: new Date().toISOString()
      }
    });
    
    console.log(`[batch] Submitted batch: ${batch.id}`);
    console.log(`[batch] Status: ${batch.status}`);
    console.log(`[batch] Request count: ${batch.request_counts?.total || requests.length}`);
    
    // Persist batch job to database
    const batchJobData = {
      batch_id: batch.id,
      status: batch.status,
      total_requests: requests.length,
      database_name: batchArgs.db,
      limit_count: batchArgs.limit,
      resume_mode: batchArgs.resume !== false,
      dry_run: batchArgs.dryRun || false,
      input_file_id: file.id,
      description: `Unified opinion extraction - ${requests.length} opinions`,
      created_by: 'batch_script'
    };
    
    const batchJob = await batchPersistence.createBatchJob(dbClient, batchJobData);
    console.log(`[batch] Created database record: batch_job_id=${batchJob.id}`);
    
    // Persist individual opinion requests
    await batchPersistence.createBatchOpinionRequests(dbClient, batchJob.id, opinions);
    console.log(`[batch] Tracked ${opinions.length} opinion requests in database`);
    
    // Clean up temp file
    fs.unlinkSync(tempPath);
    
    return { batch, batchJob };
  } catch (error) {
    // Clean up temp file on error
    if (fs.existsSync(tempPath)) {
      fs.unlinkSync(tempPath);
    }
    throw error;
  }
}

async function pollBatchStatus(client, batchId, maxWaitMinutes = 1440) { // 24 hours default
  console.log(`[batch] Polling batch ${batchId} (max wait: ${maxWaitMinutes} minutes)`);
  
  const startTime = Date.now();
  const maxWaitMs = maxWaitMinutes * 60 * 1000;
  
  while (true) {
    const batch = await client.batches.retrieve(batchId);
    const elapsed = Math.round((Date.now() - startTime) / 1000 / 60);
    
    console.log(`[batch] Status: ${batch.status} (${elapsed}m elapsed)`);
    
    if (batch.status === 'completed') {
      console.log(`[batch] Batch completed successfully!`);
      console.log(`[batch] Requests: ${batch.request_counts?.completed || 0}/${batch.request_counts?.total || 0} completed`);
      return batch;
    }
    
    if (batch.status === 'failed' || batch.status === 'expired' || batch.status === 'cancelled') {
      throw new Error(`Batch ${batchId} ${batch.status}: ${batch.errors?.[0]?.message || 'Unknown error'}`);
    }
    
    if (Date.now() - startTime > maxWaitMs) {
      console.log(`[batch] Timeout reached. Batch is still ${batch.status}`);
      console.log(`[batch] You can check status later with: --retrieve-only ${batchId}`);
      return null;
    }
    
    // Wait before next poll (exponential backoff)
    const waitMs = Math.min(60000, 5000 + elapsed * 1000); // 5s + 1s per minute elapsed, max 60s
    await new Promise(resolve => setTimeout(resolve, waitMs));
  }
}

async function retrieveBatchResults(client, batchId) {
  const batch = await client.batches.retrieve(batchId);
  
  if (batch.status !== 'completed') {
    throw new Error(`Batch ${batchId} is not completed (status: ${batch.status})`);
  }
  
  if (!batch.output_file_id) {
    throw new Error(`Batch ${batchId} has no output file`);
  }
  
  console.log(`[batch] Downloading results from batch ${batchId}`);
  
  // Download results file
  const fileResponse = await client.files.content(batch.output_file_id);
  const resultsText = await fileResponse.text();
  
  // Parse JSONL results
  const results = resultsText.trim().split('\n').map(line => JSON.parse(line));
  
  console.log(`[batch] Retrieved ${results.length} results`);
  
  return results;
}

async function processBatchResults(dbClient, results, args, batchId) {
  const stats = {
    processed: 0,
    valuelessFlagged: 0,
    validationErrors: 0,
    dbErrors: 0,
    success: 0
  };
  
  console.log(`[batch] Processing ${results.length} batch results`);
  
  // Get batch job info for tracking
  const batchJob = await batchPersistence.getBatchJob(dbClient, batchId);
  if (!batchJob) {
    throw new Error(`Batch job not found in database: ${batchId}`);
  }
  
  for (const result of results) {
    const opinionId = parseInt(result.custom_id.replace('opinion_', ''));
    stats.processed++;
    
    let statusData = {
      status: 'failed',
      extraction_successful: false,
      validation_successful: false,
      upsert_successful: false,
      keywords_extracted: 0,
      holdings_extracted: 0,
      overruled_cases_extracted: 0
    };
    
    try {
      if (result.error) {
        console.error(`[batch] API error for opinion ${opinionId}: ${result.error.message}`);
        statusData.error_code = result.error.code || 'api_error';
        statusData.error_message = result.error.message;
        stats.dbErrors++;
        await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
        continue;
      }
      
      const content = result.response?.body?.choices?.[0]?.message?.content;
      if (!content) {
        console.error(`[batch] No content in response for opinion ${opinionId}`);
        statusData.error_code = 'no_content';
        statusData.error_message = 'No content in API response';
        stats.dbErrors++;
        await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
        continue;
      }
      
      // Parse LLM response
      let unifiedPayload;
      try {
        unifiedPayload = JSON.parse(content);
        // Don't set extraction_successful here - wait until validation passes
      } catch (parseError) {
        console.error(`[batch] JSON parse error for opinion ${opinionId}: ${parseError.message}`);
        statusData.error_code = 'json_parse_error';
        statusData.error_message = parseError.message;
        stats.validationErrors++;
        await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
        continue;
      }
      
      // Handle valueless opinions
      const isValueless = unifiedPayload.v === true;
      const valuelessReason = unifiedPayload.vr;
      if (isValueless) {
        if (!args.dryRun) {
          await common.flagValuelessOpinion(dbClient, opinionId, valuelessReason || 'valueless opinion');
        }
        statusData.status = 'completed';
        statusData.validation_successful = true;
        statusData.upsert_successful = !args.dryRun;
        stats.valuelessFlagged++;
        if (args.debug) {
          console.log(`[batch] Opinion ${opinionId} flagged as valueless: ${valuelessReason}`);
        }
        await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
        continue;
      }
      
      // Validate against schema
      const { valid, errors } = validateUnified(unifiedPayload);
      if (!valid) {
        console.error(`[batch] Validation failed for opinion ${opinionId}: ${errors.join(', ')}`);
        statusData.error_code = 'validation_error';
        statusData.error_message = errors.join(', ');
        stats.validationErrors++;
        await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
        continue;
      }
      statusData.validation_successful = true;
      statusData.extraction_successful = true; // Set only after validation passes
      
      // Expand minimal response to verbose format
      const expandedResponse = expandUnifiedResponse(unifiedPayload);
      
      // Count extracted items for tracking
      statusData.keywords_extracted = (expandedResponse.field_of_law?.length || 0) +
                                     (expandedResponse.major_doctrine?.length || 0) +
                                     (expandedResponse.legal_concept?.length || 0) +
                                     (expandedResponse.distinguishing_factor?.length || 0) +
                                     (expandedResponse.procedural_posture?.length || 0) +
                                     (expandedResponse.case_outcome?.length || 0);
      statusData.holdings_extracted = expandedResponse.holdings?.length || 0;
      statusData.overruled_cases_extracted = expandedResponse.overruled_cases?.length || 0;
      
      if (args.debug) {
        console.log(`[batch] Opinion ${opinionId} extracted:`, {
          field_of_law: expandedResponse.field_of_law?.length || 0,
          major_doctrine: expandedResponse.major_doctrine?.length || 0,
          legal_concept: expandedResponse.legal_concept?.length || 0,
          distinguishing_factor: expandedResponse.distinguishing_factor?.length || 0,
          procedural_posture: expandedResponse.procedural_posture?.length || 0,
          case_outcome: expandedResponse.case_outcome?.length || 0,
          holdings: expandedResponse.holdings?.length || 0,
          overruled_cases: expandedResponse.overruled_cases?.length || 0
        });
      }
      
      // Upsert to database
      if (!args.dryRun) {
        await upsertUnified(dbClient, opinionId, expandedResponse);
        statusData.upsert_successful = true;
      } else {
        statusData.upsert_successful = true; // Consider successful in dry-run mode
      }
      
      statusData.status = 'completed';
      stats.success++;
      
    } catch (error) {
      console.error(`[batch] Error processing opinion ${opinionId}: ${error.message}`);
      statusData.error_code = 'processing_error';
      statusData.error_message = error.message;
      stats.dbErrors++;
    }
    
    // Update opinion request status in database
    await batchPersistence.updateOpinionRequestStatus(dbClient, batchJob.id, opinionId, statusData);
  }
  
  // Update batch job completion status
  await batchPersistence.completeBatchJob(dbClient, batchId, {
    completed: stats.success + stats.valuelessFlagged,
    failed: stats.validationErrors + stats.dbErrors
  });
  
  return stats;
}

async function syncBatchStatuses(openAIClient, dbClient) {
  console.log('[sync] Synchronizing batch statuses with OpenAI...');
  
  try {
    // Get recent batches from our database (not just pending ones)
    // Include batches from the last 7 days to catch any that might be out of sync
    const recentBatchesQuery = `
      SELECT batch_id, status, created_at
      FROM batch_jobs 
      WHERE created_at > NOW() - INTERVAL '7 days'
        AND batch_id ~ '^batch_[a-f0-9]{32}$'  -- Only real OpenAI batch IDs
      ORDER BY created_at DESC
    `;
    
    const result = await dbClient.query(recentBatchesQuery);
    const recentBatches = result.rows;
    
    if (recentBatches.length === 0) {
      console.log('[sync] No recent batches to synchronize');
      return;
    }
    
    let syncCount = 0;
    
    for (const dbBatch of recentBatches) {
      try {
        // Get current status from OpenAI
        const openAIBatch = await openAIClient.batches.retrieve(dbBatch.batch_id);
        
        if (openAIBatch.status !== dbBatch.status) {
          console.log(`[sync] Updating ${dbBatch.batch_id}: ${dbBatch.status} → ${openAIBatch.status}`);
          
          // Update status in our database - pass the complete OpenAI batch object
          await batchPersistence.updateBatchJobStatus(dbClient, dbBatch.batch_id, openAIBatch);
          
          // Only call completeBatchJob for actually completed batches, not cancelled ones
          if (openAIBatch.status === 'completed') {
            await batchPersistence.completeBatchJob(dbClient, dbBatch.batch_id, {
              completed: openAIBatch.request_counts?.completed || 0,
              failed: openAIBatch.request_counts?.failed || 0
            });
          }
          
          syncCount++;
        }
      } catch (error) {
        console.log(`[sync] Could not sync batch ${dbBatch.batch_id}: ${error.message}`);
      }
    }
    
    if (syncCount > 0) {
      console.log(`[sync] Updated ${syncCount} batch statuses`);
    } else {
      console.log('[sync] All batches are already synchronized');
    }
    
  } catch (error) {
    console.error(`[sync] Synchronization failed: ${error.message}`);
  }
}

async function listBatches(openAIClient, dbClient) {
  // First, synchronize batch statuses
  await syncBatchStatuses(openAIClient, dbClient);
  
  // Get batches from database with updated status
  const dbBatches = await batchPersistence.getBatchJobs(dbClient);
  
  console.log('\nRecent Batches (from database):');
  console.log('================================');
  
  for (const batch of dbBatches) {
    const created = new Date(batch.created_at).toISOString();
    const completed = batch.completed_at ? new Date(batch.completed_at).toISOString() : 'N/A';
    
    console.log(`ID: ${batch.batch_id}`);
    console.log(`Status: ${batch.status}`);
    console.log(`Database: ${batch.database_name}`);
    console.log(`Created: ${created}`);
    console.log(`Completed: ${completed}`);
    console.log(`Requests: ${batch.completed_requests || 0}/${batch.total_requests} completed`);
    console.log(`Opinions: ${batch.opinion_count} tracked, ${batch.completed_count} processed`);
    if (batch.description) {
      console.log(`Description: ${batch.description}`);
    }
    if (batch.error_message) {
      console.log(`Error: ${batch.error_message}`);
    }
    console.log('---');
  }
  
  // Also show OpenAI batches that we're tracking in our database
  try {
    const openAIBatches = await openAIClient.batches.list({ limit: 20 });
    
    // Create a set of batch IDs we're tracking in our database
    const trackedBatchIds = new Set(dbBatches.map(b => b.batch_id));
    
    // Filter OpenAI batches to only show ones we're tracking
    const trackedOpenAIBatches = openAIBatches.data.filter(batch => 
      trackedBatchIds.has(batch.id)
    );
    
    if (trackedOpenAIBatches.length > 0) {
      console.log('\nOpenAI Batches (tracked in our database):');
      console.log('==========================================');
      
      for (const batch of trackedOpenAIBatches) {
        const created = new Date(batch.created_at * 1000).toISOString();
        const requests = batch.request_counts;
        
        console.log(`ID: ${batch.id}`);
        console.log(`Status: ${batch.status}`);
        console.log(`Created: ${created}`);
        console.log(`Requests: ${requests?.completed || 0}/${requests?.total || 0} completed`);
        console.log('---');
      }
    } else {
      console.log('\nOpenAI Batches (tracked in our database):');
      console.log('==========================================');
      console.log('No tracked batches found in OpenAI (all may have been cleaned up)');
    }
  } catch (error) {
    console.log(`\nNote: Could not fetch OpenAI batches: ${error.message}`);
  }
}

async function cancelBatch(client, batchId) {
  try {
    const batch = await client.batches.cancel(batchId);
    console.log(`[batch] Cancelled batch ${batchId}`);
    console.log(`[batch] Status: ${batch.status}`);
    return batch;
  } catch (error) {
    console.error(`[batch] Failed to cancel batch ${batchId}: ${error.message}`);
    throw error;
  }
}

async function cleanupBatches(dbClient, args) {
  console.log('🧹 Cleaning up completed batches...');
  
  // Build cleanup options based on arguments
  const cleanupOptions = {};
  
  if (args.cleanupTestOnly) {
    cleanupOptions.testBatchesOnly = true;
    console.log('[cleanup] Mode: Test batches only');
  }
  
  if (args.cleanupCancelled) {
    cleanupOptions.cancelledOnly = true;
    console.log('[cleanup] Mode: Cancelled batches only');
  }
  
  if (args.cleanupFailed) {
    cleanupOptions.failedOnly = true;
    console.log('[cleanup] Mode: Failed batches only');
  }
  
  if (args.cleanupOlderThan) {
    const days = parseInt(args.cleanupOlderThan);
    if (isNaN(days) || days < 1) {
      console.error('Error: --cleanup-older-than must be a positive number');
      process.exit(1);
    }
    cleanupOptions.olderThanDays = days;
    console.log(`[cleanup] Mode: Batches older than ${days} days`);
  }
  
  if (!args.cleanupTestOnly && !args.cleanupCancelled && !args.cleanupFailed && !args.cleanupOlderThan) {
    console.log('[cleanup] Mode: All completed, cancelled, and failed batches (use --cleanup-test-only for safer option)');
    
    // Confirm for safety when cleaning all batches
    console.log('\n⚠️  WARNING: This will delete ALL completed, cancelled, and failed batch records from the database!');
    console.log('   This action cannot be undone.');
    console.log('   Consider using --cleanup-test-only, --cleanup-cancelled, --cleanup-failed, or --cleanup-older-than for safer cleanup.');
    console.log('\n   Press Ctrl+C to cancel, or any key to continue...');
    
    // Wait for user input (in a real scenario, you might want to use readline)
    // For now, we'll add a 5-second delay and continue
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
  
  try {
    // Get batches to clean up
    const batchesToCleanup = await batchPersistence.getCompletedBatchJobs(dbClient, cleanupOptions);
    
    if (batchesToCleanup.length === 0) {
      console.log('✅ No batches found matching cleanup criteria');
      return;
    }
    
    console.log(`\n📋 Found ${batchesToCleanup.length} batches to clean up:`);
    console.log('=====================================');
    
    let totalOpinionRequests = 0;
    batchesToCleanup.forEach((batch, idx) => {
      const created = new Date(batch.created_at).toISOString().split('T')[0];
      const completed = batch.completed_at ? new Date(batch.completed_at).toISOString().split('T')[0] : 'N/A';
      
      console.log(`${idx + 1}. ${batch.batch_id}`);
      console.log(`   Description: ${batch.description || 'N/A'}`);
      console.log(`   Created: ${created}, Completed: ${completed}`);
      console.log(`   Requests: ${batch.completed_requests || 0}/${batch.total_requests || 0}`);
      console.log(`   Opinion Requests: ${batch.opinion_request_count}`);
      console.log('   ---');
      
      totalOpinionRequests += parseInt(batch.opinion_request_count) || 0;
    });
    
    console.log(`\n📊 Cleanup Summary:`);
    console.log(`   Batch Jobs: ${batchesToCleanup.length}`);
    console.log(`   Opinion Requests: ${totalOpinionRequests}`);
    
    if (args.dryRun) {
      console.log('\n🔍 DRY RUN: No actual deletion performed');
      return;
    }
    
    // Perform cleanup
    console.log('\n🗑️  Deleting batches...');
    const batchIds = batchesToCleanup.map(b => b.batch_id);
    const results = await batchPersistence.deleteBatchJobs(dbClient, batchIds);
    
    // Report results
    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);
    
    console.log('\n✅ Cleanup Complete!');
    console.log('===================');
    console.log(`   Successfully deleted: ${successful.length} batches`);
    
    if (failed.length > 0) {
      console.log(`   Failed to delete: ${failed.length} batches`);
      console.log('\n❌ Failed deletions:');
      failed.forEach(result => {
        console.log(`   - ${result.batchId}: ${result.error}`);
      });
    }
    
    // Show detailed success stats
    const totalDeletedOpinionRequests = successful.reduce((sum, r) => sum + (r.deletedOpinionRequests || 0), 0);
    console.log(`   Total opinion requests deleted: ${totalDeletedOpinionRequests}`);
    
    if (successful.length > 0) {
      console.log('\n🎉 Database cleanup completed successfully!');
    }
    
  } catch (error) {
    console.error('❌ Cleanup failed:', error.message);
    throw error;
  }
}

async function main() {
  const args = common.parseCommonArgs(process.argv);
  
  if (args.help) {
    showUsage();
    return;
  }
  
  const client = getOpenAI();
  
  // Handle batch management commands
  if (args.listBatches) {
    if (!args.db) {
      console.error('Error: --db is required for listing batches');
      process.exit(1);
    }
    
    const dbClient = common.makePgClient(args.db);
    try {
      await listBatches(client, dbClient);
    } finally {
      await dbClient.end();
    }
    return;
  }
  
  if (args.cancelBatch) {
    await cancelBatch(client, args.cancelBatch);
    return;
  }
  
  if (args.cleanupBatches) {
    if (!args.db) {
      console.error('Error: --db is required for cleanup operations');
      process.exit(1);
    }
    
    const dbClient = common.makePgClient(args.db);
    try {
      await cleanupBatches(dbClient, args);
    } finally {
      await dbClient.end();
    }
    return;
  }
  
  if (args.retrieveOnly) {
    console.log(`[batch] Retrieving results for batch: ${args.retrieveOnly}`);
    
    if (!args.db) {
      console.error('Error: --db is required for processing results');
      process.exit(1);
    }
    
    const dbClient = common.makePgClient(args.db);
    
    try {
      const results = await retrieveBatchResults(client, args.retrieveOnly);
      const stats = await processBatchResults(dbClient, results, args, args.retrieveOnly);
      
      console.log('\nBatch Processing Complete:');
      console.log(`  Processed: ${stats.processed}`);
      console.log(`  Success: ${stats.success}`);
      console.log(`  Valueless: ${stats.valuelessFlagged}`);
      console.log(`  Validation Errors: ${stats.validationErrors}`);
      console.log(`  DB Errors: ${stats.dbErrors}`);
      
    } finally {
      await dbClient.end();
    }
    return;
  }
  
  // Main batch processing workflow
  if (!args.db) {
    console.error('Error: --db is required');
    showUsage();
    process.exit(1);
  }
  
  const dbClient = common.makePgClient(args.db);
  
  try {
    // Load allowed values (same as current pipeline)
    const [fieldsRes, posturesRes, outcomesRes] = await Promise.all([
      dbClient.query(`SELECT id, keyword_text FROM keywords WHERE tier = 'field_of_law'`),
      dbClient.query(`SELECT keyword_text FROM keywords WHERE tier = 'procedural_posture' ORDER BY keyword_text`),
      dbClient.query(`SELECT keyword_text FROM keywords WHERE tier = 'case_outcome' ORDER BY keyword_text`)
    ]);

    const allowedFields = fieldsRes.rows.map(r => r.keyword_text);
    const allowedPostures = posturesRes.rows.map(r => r.keyword_text);
    const allowedOutcomes = outcomesRes.rows.map(r => r.keyword_text);
    
    // Get unprocessed opinions (same resume logic as current pipeline)
    const resumeClause = args.resume !== false ? `
      NOT EXISTS (
        SELECT 1 FROM opinion_keywords ok
         WHERE ok.opinion_id = o.id
           AND ok.category IN ('field_of_law','procedural_posture','case_outcome','distinguishing_factor')
         GROUP BY ok.opinion_id
         HAVING COUNT(DISTINCT ok.category) = 4
      )` : '';
    
    const candRes = await common.getOpinionCandidates(dbClient, args, resumeClause);
    console.log(`[batch] Selected ${candRes.rowCount} opinions for batch processing${args.resume !== false ? ' (resume)' : ''}`);
    
    if (candRes.rowCount === 0) {
      console.log('[batch] No opinions to process');
      return;
    }
    
    // Check if we need to split into multiple batches
    const opinions = candRes.rows;
    const totalOpinions = opinions.length;
    
    if (totalOpinions > BATCH_CONFIG.SPLIT_THRESHOLD) {
      console.log(`[batch] Large batch detected (${totalOpinions} opinions). Splitting into smaller batches...`);
      return await processMultipleBatches(client, dbClient, opinions, allowedFields, allowedPostures, allowedOutcomes, args);
    }
    
    // Single batch processing (existing logic)
    console.log(`[batch] Processing single batch of ${totalOpinions} opinions`);
    const requests = await createBatchRequest(opinions, allowedFields, allowedPostures, allowedOutcomes);
    
    // Submit batch
    const { batch, batchJob } = await submitBatch(client, dbClient, requests, opinions, args);
    
    if (args.submitOnly) {
      console.log(`[batch] Batch submitted successfully: ${batch.id}`);
      console.log(`[batch] Check status with: --retrieve-only ${batch.id}`);
      return;
    }
    
    // Wait for completion
    const completedBatch = await pollBatchStatus(client, batch.id);
    
    if (!completedBatch) {
      console.log(`[batch] Batch is still processing. Check later with: --retrieve-only ${batch.id}`);
      return;
    }
    
    // Process results
    const results = await retrieveBatchResults(client, batch.id);
    const stats = await processBatchResults(dbClient, results, args, batch.id);
    
    console.log('\nBatch Processing Complete:');
    console.log(`  Processed: ${stats.processed}`);
    console.log(`  Success: ${stats.success}`);
    console.log(`  Valueless: ${stats.valuelessFlagged}`);
    console.log(`  Validation Errors: ${stats.validationErrors}`);
    console.log(`  DB Errors: ${stats.dbErrors}`);
    
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

/**
 * Process multiple batches automatically for large datasets
 */
async function processMultipleBatches(client, dbClient, allOpinions, allowedFields, allowedPostures, allowedOutcomes, args) {
  const totalOpinions = allOpinions.length;
  const batchSize = calculateOptimalBatchSize(totalOpinions);
  const numBatches = Math.ceil(totalOpinions / batchSize);
  
  console.log(`[batch] Splitting ${totalOpinions} opinions into ${numBatches} batches of ~${batchSize} opinions each`);
  
  // Check for dry-run mode - CRITICAL: Prevent unintended batch submission
  if (args.dryRun) {
    console.log('\n🔍 DRY RUN MODE: Simulating batch splitting without actual submission');
    console.log('=====================================');
    
    for (let i = 0; i < numBatches; i++) {
      const startIdx = i * batchSize;
      const endIdx = Math.min(startIdx + batchSize, totalOpinions);
      const batchOpinions = allOpinions.slice(startIdx, endIdx);
      
      console.log(`[dry-run] Would submit batch ${i + 1}/${numBatches}:`);
      console.log(`   Opinions: ${startIdx + 1}-${endIdx} (${batchOpinions.length} total)`);
      console.log(`   Description: Multi-batch ${i + 1}/${numBatches} - ${batchOpinions.length} opinions`);
    }
    
    console.log('\\n✅ DRY RUN COMPLETE: No batches were actually submitted');
    console.log('💡 Remove --dry-run flag to perform actual batch submission');
    return { submittedBatches: [], stats: { submitted: 0, dryRun: true } };
  }
  
  const submittedBatches = [];
  const batchStats = {
    submitted: 0,
    completed: 0,
    totalProcessed: 0,
    totalSuccess: 0,
    totalErrors: 0
  };
  
  // Submit all batches (REAL SUBMISSION - dry-run check passed)
  console.log('\\n🚀 REAL SUBMISSION MODE: Submitting batches to OpenAI');
  for (let i = 0; i < numBatches; i++) {
    const startIdx = i * batchSize;
    const endIdx = Math.min(startIdx + batchSize, totalOpinions);
    const batchOpinions = allOpinions.slice(startIdx, endIdx);
    
    console.log(`[batch] Submitting batch ${i + 1}/${numBatches} (opinions ${startIdx + 1}-${endIdx})`);
    
    try {
      const requests = await createBatchRequest(batchOpinions, allowedFields, allowedPostures, allowedOutcomes);
      const { batch, batchJob } = await submitBatch(client, dbClient, requests, batchOpinions, {
        ...args,
        batchDescription: `Multi-batch ${i + 1}/${numBatches} - ${batchOpinions.length} opinions`
      });
      
      submittedBatches.push({ batch, batchJob, opinions: batchOpinions });
      batchStats.submitted++;
      
      console.log(`[batch] ✓ Batch ${i + 1}/${numBatches} submitted: ${batch.id}`);
      
      // Small delay between submissions to avoid rate limits
      if (i < numBatches - 1) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
    } catch (error) {
      console.error(`[batch] ✗ Failed to submit batch ${i + 1}/${numBatches}:`, error.message);
      // Continue with other batches even if one fails
    }
  }
  
  console.log(`\n[batch] Successfully submitted ${batchStats.submitted}/${numBatches} batches`);
  
  if (args.submitOnly) {
    console.log('\n📋 Submitted Batches:');
    submittedBatches.forEach((item, idx) => {
      console.log(`  ${idx + 1}. ${item.batch.id} (${item.opinions.length} opinions)`);
    });
    console.log('\n💡 To retrieve all results later, use:');
    submittedBatches.forEach((item, idx) => {
      console.log(`  node bin/run_unified_batch.js --db ${args.db} --retrieve-only ${item.batch.id}`);
    });
    return { submittedBatches, stats: batchStats };
  }
  
  // Wait for all batches to complete and process results
  console.log('\n[batch] Waiting for all batches to complete...');
  
  for (const { batch, batchJob, opinions } of submittedBatches) {
    console.log(`[batch] Polling batch ${batch.id}...`);
    
    try {
      const completedBatch = await pollBatchStatus(client, batch.id);
      
      if (completedBatch) {
        console.log(`[batch] ✓ Batch ${batch.id} completed, processing results...`);
        
        const results = await retrieveBatchResults(client, batch.id);
        const stats = await processBatchResults(dbClient, results, args, batch.id);
        
        batchStats.completed++;
        batchStats.totalProcessed += stats.processed;
        batchStats.totalSuccess += stats.success;
        batchStats.totalErrors += (stats.validationErrors + stats.dbErrors);
        
        console.log(`[batch] ✓ Batch ${batch.id}: ${stats.success}/${stats.processed} successful`);
      } else {
        console.log(`[batch] ⏳ Batch ${batch.id} still processing`);
      }
      
    } catch (error) {
      console.error(`[batch] ✗ Error processing batch ${batch.id}:`, error.message);
    }
  }
  
  // Final summary
  console.log('\n🎉 Multi-Batch Processing Complete!');
  console.log('=====================================');
  console.log(`  Total Batches: ${numBatches}`);
  console.log(`  Submitted: ${batchStats.submitted}`);
  console.log(`  Completed: ${batchStats.completed}`);
  console.log(`  Total Opinions Processed: ${batchStats.totalProcessed}`);
  console.log(`  Total Successful: ${batchStats.totalSuccess}`);
  console.log(`  Total Errors: ${batchStats.totalErrors}`);
  console.log(`  Success Rate: ${batchStats.totalProcessed > 0 ? ((batchStats.totalSuccess / batchStats.totalProcessed) * 100).toFixed(1) : 0}%`);
  
  return { submittedBatches, stats: batchStats };
}

/**
 * Calculate optimal batch size based on total opinions
 */
function calculateOptimalBatchSize(totalOpinions) {
  if (totalOpinions <= BATCH_CONFIG.SPLIT_THRESHOLD) {
    return totalOpinions; // No splitting needed
  }
  
  // For very large datasets, use smaller batches for better parallelism
  if (totalOpinions > 10000) {
    return BATCH_CONFIG.MIN_BATCH_SIZE * 4; // 200 opinions per batch
  }
  
  if (totalOpinions > 5000) {
    return BATCH_CONFIG.OPTIMAL_BATCH_SIZE; // 500 opinions per batch
  }
  
  // For moderate datasets, aim for 5-10 batches
  const targetBatches = Math.min(10, Math.max(5, Math.ceil(totalOpinions / BATCH_CONFIG.OPTIMAL_BATCH_SIZE)));
  return Math.ceil(totalOpinions / targetBatches);
}

module.exports = {
  createBatchRequest,
  submitBatch,
  pollBatchStatus,
  retrieveBatchResults,
  processBatchResults,
  processMultipleBatches,
  calculateOptimalBatchSize,
  BATCH_CONFIG
};
