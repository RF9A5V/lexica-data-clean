// Batch persistence module for OpenAI Batch API tracking
// Provides database persistence for batch jobs and individual opinion requests

/**
 * Create a new batch job record in the database
 */
async function createBatchJob(client, batchData) {
  const query = `
    INSERT INTO batch_jobs (
      batch_id, status, total_requests, database_name, 
      limit_count, resume_mode, dry_run, input_file_id, 
      description, created_by, submitted_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
    RETURNING id, batch_id, created_at
  `;
  
  const values = [
    batchData.batch_id,
    batchData.status || 'submitted',
    batchData.total_requests,
    batchData.database_name,
    batchData.limit_count || null,
    batchData.resume_mode !== false,
    batchData.dry_run || false,
    batchData.input_file_id || null,
    batchData.description || null,
    batchData.created_by || 'system'
  ];
  
  const result = await client.query(query, values);
  return result.rows[0];
}

/**
 * Create batch opinion request records
 */
async function createBatchOpinionRequests(client, batchJobId, opinions) {
  if (opinions.length === 0) return [];
  
  const values = [];
  const placeholders = [];
  
  opinions.forEach((opinion, index) => {
    const baseIndex = index * 3;
    placeholders.push(`($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3})`);
    values.push(batchJobId, opinion.id, `opinion_${opinion.id}`);
  });
  
  const query = `
    INSERT INTO batch_opinion_requests (batch_job_id, opinion_id, custom_id)
    VALUES ${placeholders.join(', ')}
    RETURNING id, opinion_id, custom_id
  `;
  
  const result = await client.query(query, values);
  return result.rows;
}

/**
 * Update batch job status from OpenAI batch response
 */
async function updateBatchJobStatus(client, batchId, openAIBatch) {
  const query = `
    SELECT update_batch_job_status($1, $2, $3, $4, $5, $6, $7)
  `;
  
  const values = [
    batchId,
    openAIBatch.status,
    openAIBatch.request_counts?.completed || null,
    openAIBatch.request_counts?.failed || null,
    openAIBatch.output_file_id || null,
    openAIBatch.error_file_id || null,
    openAIBatch.errors?.[0]?.message || null
  ];
  
  await client.query(query, values);
}

/**
 * Get batch job by batch_id
 */
async function getBatchJob(client, batchId) {
  const query = `
    SELECT * FROM batch_jobs 
    WHERE batch_id = $1
  `;
  
  const result = await client.query(query, [batchId]);
  return result.rows[0] || null;
}

/**
 * Get all batch jobs with optional status filter
 */
async function getBatchJobs(client, statusFilter = null, limit = 20) {
  let query = `
    SELECT 
      bj.*,
      COUNT(bor.id) as opinion_count,
      COUNT(CASE WHEN bor.status = 'completed' THEN 1 END) as completed_count,
      COUNT(CASE WHEN bor.status = 'failed' THEN 1 END) as failed_count
    FROM batch_jobs bj
    LEFT JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
  `;
  
  const values = [];
  let paramIndex = 1;
  
  if (statusFilter) {
    query += ` WHERE bj.status = $${paramIndex}`;
    values.push(statusFilter);
    paramIndex++;
  }
  
  query += `
    GROUP BY bj.id
    ORDER BY bj.created_at DESC
    LIMIT $${paramIndex}
  `;
  values.push(limit);
  
  const result = await client.query(query, values);
  return result.rows;
}

/**
 * Update individual opinion request status
 */
async function updateOpinionRequestStatus(client, batchJobId, opinionId, statusData) {
  const query = `
    UPDATE batch_opinion_requests 
    SET 
      status = $3,
      processed_at = NOW(),
      error_code = $4,
      error_message = $5,
      extraction_successful = $6,
      validation_successful = $7,
      upsert_successful = $8,
      keywords_extracted = $9,
      holdings_extracted = $10,
      overruled_cases_extracted = $11
    WHERE batch_job_id = $1 AND opinion_id = $2
  `;
  
  const values = [
    batchJobId,
    opinionId,
    statusData.status,
    statusData.error_code || null,
    statusData.error_message || null,
    statusData.extraction_successful || null,
    statusData.validation_successful || null,
    statusData.upsert_successful || null,
    statusData.keywords_extracted || 0,
    statusData.holdings_extracted || 0,
    statusData.overruled_cases_extracted || 0
  ];
  
  await client.query(query, values);
}

/**
 * Get batch processing statistics
 */
async function getBatchStats(client, batchId) {
  const query = `SELECT * FROM get_batch_stats($1)`;
  const result = await client.query(query, [batchId]);
  return result.rows[0] || null;
}

/**
 * Get failed opinion requests for retry
 */
async function getFailedOpinionRequests(client, batchId) {
  const query = `
    SELECT bor.opinion_id, bor.error_code, bor.error_message
    FROM batch_jobs bj
    JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
    WHERE bj.batch_id = $1 AND bor.status = 'failed'
    ORDER BY bor.opinion_id
  `;
  
  const result = await client.query(query, [batchId]);
  return result.rows;
}

/**
 * Mark batch job as completed and update final statistics
 */
async function completeBatchJob(client, batchId, finalStats) {
  const query = `
    UPDATE batch_jobs 
    SET 
      status = 'completed',
      completed_at = NOW(),
      completed_requests = $2,
      failed_requests = $3
    WHERE batch_id = $1
  `;
  
  await client.query(query, [
    batchId,
    finalStats.completed || 0,
    finalStats.failed || 0
  ]);
}

/**
 * Get pending batches that might need status updates
 */
async function getPendingBatches(client) {
  const query = `
    SELECT batch_id, status, created_at
    FROM batch_jobs 
    WHERE status IN ('submitted', 'validating', 'in_progress', 'finalizing')
    ORDER BY created_at ASC
  `;
  
  const result = await client.query(query);
  return result.rows;
}

/**
 * Check if opinions are already in a pending batch
 */
async function checkOpinionsInPendingBatch(client, opinionIds) {
  if (opinionIds.length === 0) return [];
  
  const query = `
    SELECT DISTINCT bor.opinion_id, bj.batch_id, bj.status
    FROM batch_jobs bj
    JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
    WHERE bj.status IN ('submitted', 'validating', 'in_progress', 'finalizing')
      AND bor.opinion_id = ANY($1::int[])
  `;
  
  const result = await client.query(query, [opinionIds]);
  return result.rows;
}

/**
 * Get all completed batch jobs with optional filtering
 */
async function getCompletedBatchJobs(client, options = {}) {
  // Include completed, cancelled, and failed batches by default
  let whereClause = "WHERE bj.status IN ('completed', 'cancelled', 'failed')";
  const params = [];
  
  // Option to include only completed batches (exclude cancelled and failed)
  if (options.completedOnly) {
    whereClause = "WHERE bj.status = 'completed'";
  }
  
  // Option to include only cancelled batches
  if (options.cancelledOnly) {
    whereClause = "WHERE bj.status = 'cancelled'";
  }
  
  // Option to include only failed batches
  if (options.failedOnly) {
    whereClause = "WHERE bj.status = 'failed'";
  }
  
  if (options.olderThanDays) {
    whereClause += ` AND bj.completed_at < NOW() - INTERVAL '${options.olderThanDays} days'`;
  }
  
  if (options.testBatchesOnly) {
    whereClause += ` AND (bj.batch_id LIKE 'test_%' OR bj.description LIKE 'Test batch%')`;
  }
  
  if (options.noOpenAIEquivalent) {
    // Batches that don't exist in OpenAI anymore (test batches or old batches)
    whereClause += ` AND bj.batch_id NOT IN (
      SELECT batch_id FROM batch_jobs 
      WHERE batch_id ~ '^batch_[a-f0-9]{32}$'
    )`;
  }
  
  const query = `
    SELECT 
      bj.id as batch_job_id,
      bj.batch_id,
      bj.status,
      bj.database_name,
      bj.created_at,
      bj.completed_at,
      bj.description,
      bj.total_requests,
      bj.completed_requests,
      bj.failed_requests,
      COUNT(bor.id) as opinion_request_count
    FROM batch_jobs bj
    LEFT JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
    ${whereClause}
    GROUP BY bj.id, bj.batch_id, bj.status, bj.database_name, bj.created_at, 
             bj.completed_at, bj.description, bj.total_requests, bj.completed_requests, bj.failed_requests
    ORDER BY bj.completed_at DESC
  `;
  
  const result = await client.query(query, params);
  return result.rows;
}

/**
 * Delete a batch job and all associated opinion requests
 */
async function deleteBatchJob(client, batchId) {
  const deleteOpinionRequestsQuery = `
    DELETE FROM batch_opinion_requests 
    WHERE batch_job_id = (SELECT id FROM batch_jobs WHERE batch_id = $1)
  `;
  
  const deleteBatchJobQuery = `
    DELETE FROM batch_jobs WHERE batch_id = $1
  `;
  
  // Delete in transaction to ensure consistency
  await client.query('BEGIN');
  
  try {
    const opinionResult = await client.query(deleteOpinionRequestsQuery, [batchId]);
    const batchResult = await client.query(deleteBatchJobQuery, [batchId]);
    
    await client.query('COMMIT');
    
    return {
      deletedOpinionRequests: opinionResult.rowCount,
      deletedBatchJob: batchResult.rowCount > 0
    };
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  }
}

/**
 * Bulk delete multiple batch jobs
 */
async function deleteBatchJobs(client, batchIds) {
  const results = [];
  
  for (const batchId of batchIds) {
    try {
      const result = await deleteBatchJob(client, batchId);
      results.push({ batchId, success: true, ...result });
    } catch (error) {
      results.push({ batchId, success: false, error: error.message });
    }
  }
  
  return results;
}

module.exports = {
  createBatchJob,
  createBatchOpinionRequests,
  updateBatchJobStatus,
  getBatchJob,
  getBatchJobs,
  updateOpinionRequestStatus,
  getBatchStats,
  getFailedOpinionRequests,
  completeBatchJob,
  getPendingBatches,
  checkOpinionsInPendingBatch,
  getCompletedBatchJobs,
  deleteBatchJob,
  deleteBatchJobs
};
