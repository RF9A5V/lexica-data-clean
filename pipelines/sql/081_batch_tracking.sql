-- Batch tracking for OpenAI Batch API operations
-- Provides persistence and recovery capabilities for batch processing

-- Table to track batch jobs
CREATE TABLE IF NOT EXISTS batch_jobs (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(255) NOT NULL UNIQUE, -- OpenAI batch ID
    status VARCHAR(50) NOT NULL DEFAULT 'submitted', -- submitted, validating, in_progress, finalizing, completed, failed, expired, cancelled
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Batch metadata
    total_requests INTEGER NOT NULL,
    completed_requests INTEGER DEFAULT 0,
    failed_requests INTEGER DEFAULT 0,
    
    -- Processing parameters
    database_name VARCHAR(255) NOT NULL,
    limit_count INTEGER,
    resume_mode BOOLEAN NOT NULL DEFAULT true,
    dry_run BOOLEAN NOT NULL DEFAULT false,
    
    -- OpenAI file IDs
    input_file_id VARCHAR(255),
    output_file_id VARCHAR(255),
    error_file_id VARCHAR(255),
    
    -- Error tracking
    error_message TEXT,
    
    -- Metadata
    description TEXT,
    created_by VARCHAR(255) DEFAULT 'system'
);

-- Table to track individual opinions in batches
CREATE TABLE IF NOT EXISTS batch_opinion_requests (
    id SERIAL PRIMARY KEY,
    batch_job_id INTEGER NOT NULL REFERENCES batch_jobs(id) ON DELETE CASCADE,
    opinion_id INTEGER NOT NULL,
    custom_id VARCHAR(255) NOT NULL, -- opinion_123 format
    
    -- Request status
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- pending, completed, failed
    processed_at TIMESTAMP WITH TIME ZONE,
    
    -- Error tracking for individual requests
    error_code VARCHAR(50),
    error_message TEXT,
    
    -- Results tracking
    extraction_successful BOOLEAN,
    validation_successful BOOLEAN,
    upsert_successful BOOLEAN,
    
    -- Extracted data summary
    keywords_extracted INTEGER DEFAULT 0,
    holdings_extracted INTEGER DEFAULT 0,
    overruled_cases_extracted INTEGER DEFAULT 0,
    
    UNIQUE(batch_job_id, opinion_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_batch_jobs_batch_id ON batch_jobs(batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_status ON batch_jobs(status);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_created_at ON batch_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_batch_opinion_requests_batch_job_id ON batch_opinion_requests(batch_job_id);
CREATE INDEX IF NOT EXISTS idx_batch_opinion_requests_opinion_id ON batch_opinion_requests(opinion_id);
CREATE INDEX IF NOT EXISTS idx_batch_opinion_requests_status ON batch_opinion_requests(status);

-- Function to update batch job status
CREATE OR REPLACE FUNCTION update_batch_job_status(
    p_batch_id VARCHAR(255),
    p_status VARCHAR(50),
    p_completed_requests INTEGER DEFAULT NULL,
    p_failed_requests INTEGER DEFAULT NULL,
    p_output_file_id VARCHAR(255) DEFAULT NULL,
    p_error_file_id VARCHAR(255) DEFAULT NULL,
    p_error_message TEXT DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE batch_jobs 
    SET 
        status = p_status,
        completed_requests = COALESCE(p_completed_requests, completed_requests),
        failed_requests = COALESCE(p_failed_requests, failed_requests),
        output_file_id = COALESCE(p_output_file_id, output_file_id),
        error_file_id = COALESCE(p_error_file_id, error_file_id),
        error_message = COALESCE(p_error_message, error_message),
        completed_at = CASE 
            WHEN p_status IN ('completed', 'failed', 'expired', 'cancelled') 
            THEN NOW() 
            ELSE completed_at 
        END
    WHERE batch_id = p_batch_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get batch processing statistics
CREATE OR REPLACE FUNCTION get_batch_stats(p_batch_id VARCHAR(255))
RETURNS TABLE(
    total_opinions INTEGER,
    pending_opinions INTEGER,
    completed_opinions INTEGER,
    failed_opinions INTEGER,
    successful_extractions INTEGER,
    validation_errors INTEGER,
    upsert_errors INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COUNT(*)::INTEGER as total_opinions,
        COUNT(CASE WHEN bor.status = 'pending' THEN 1 END)::INTEGER as pending_opinions,
        COUNT(CASE WHEN bor.status = 'completed' THEN 1 END)::INTEGER as completed_opinions,
        COUNT(CASE WHEN bor.status = 'failed' THEN 1 END)::INTEGER as failed_opinions,
        COUNT(CASE WHEN bor.extraction_successful = true THEN 1 END)::INTEGER as successful_extractions,
        COUNT(CASE WHEN bor.validation_successful = false THEN 1 END)::INTEGER as validation_errors,
        COUNT(CASE WHEN bor.upsert_successful = false THEN 1 END)::INTEGER as upsert_errors
    FROM batch_jobs bj
    JOIN batch_opinion_requests bor ON bj.id = bor.batch_job_id
    WHERE bj.batch_id = p_batch_id;
END;
$$ LANGUAGE plpgsql;
