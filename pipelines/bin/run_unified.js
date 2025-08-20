#!/usr/bin/env node

/*
  NY Reporter Keywording Pipeline - Unified Single-Pass
  Single LLM extraction for unified categories
  Extracts: field_of_law, procedural_posture, case_outcome, distinguishing_factors, doctrines, doctrinal_tests
*/

const { Pool } = require('pg');
const pLimit = require('p-limit');
const cliProgress = require('cli-progress');
const common = require('../src/common');
const llm = require('../src/llm');
const { validateUnified } = require('../src/validation');
const { upsertUnified } = require('../src/upsert');
const { expandUnifiedResponse } = require('../src/field_mapping');

function parseArgs(argv) {
  return common.parseCommonArgs(argv, {
    '--unified': (args) => { args.unified = true; } // For compatibility
  });
}

function usage() {
  common.printCommonUsage('run_unified', `
  Unified Single-Pass Extraction:
    Extracts keyword categories in a single LLM call:
    - field_of_law, procedural_posture, case_outcome, distinguishing_factors, doctrines, doctrinal_tests
    
  Benefits:
    - Single LLM call per opinion (cost reduction)
    - Simpler pipeline architecture
    - Better contextual understanding
    
  Examples:
    node bin/run_unified.js --db postgres://dev:dev@localhost:5432/ny_reporter --limit 1000
    node bin/run_unified.js --db ny_reporter --concurrency 8 --debug --resume
`);
}

async function main() {
  common.setupGracefulShutdown();
  
  const args = parseArgs(process.argv);
  const db = args.db || process.env.DATABASE_URL;
  if (!db) { usage(); process.exit(1); }

  const concurrency = args.concurrency || common.DEFAULT_CONCURRENCY;
  const writeConcurrency = args.writeConcurrency || common.DEFAULT_WRITE_CONCURRENCY;
  const optimalPoolSize = args.poolSize || Math.max(common.DEFAULT_POOL_SIZE, concurrency + writeConcurrency + 2);

  console.log('[unified] Starting Unified Single-Pass Extraction');
  console.log(`[unified] DB: ${db}`);
  console.log(`[unified] Concurrency: LLM=${concurrency}, Write=${writeConcurrency}, Pool=${optimalPoolSize}`);

  const client = common.makePgClient(db, optimalPoolSize);
  
  try {
    // Pre-pass: report statistics
    await common.reportOpinionStats(client, 'Unified');

    // Note: No longer flagging blank opinions as valueless - they will be processed with empty arrays

    // Get candidates (exclude already fully processed)
    // Completion is based on the 4 core categories. Doctrines/tests may be legitimately empty
    // for some opinions and should not block completion.
    const resumeClause = args.resume ? `
      NOT EXISTS (
        SELECT 1 FROM opinion_keywords ok
         WHERE ok.opinion_id = o.id
           AND ok.category IN ('field_of_law','procedural_posture','case_outcome','distinguishing_factor')
         GROUP BY ok.opinion_id
         HAVING COUNT(DISTINCT ok.category) = 4
      )` : '';
    
    const candRes = await common.getOpinionCandidates(client, args, resumeClause);
    console.log(`[pre] Selected ${candRes.rowCount} opinions for unified extraction${args.resume ? ' (resume)' : ''}`);
    
    if (candRes.rowCount === 0) {
      console.log('[unified] No opinions to process');
      return;
    }

    // Load allowed values for validation (lists used by the prompt/schema)
    const [fieldsRes, posturesRes, outcomesRes] = await Promise.all([
      client.query(`SELECT id, keyword_text FROM keywords WHERE tier = 'field_of_law'`),
      client.query(`SELECT keyword_text FROM keywords WHERE tier = 'procedural_posture' ORDER BY keyword_text`),
      client.query(`SELECT keyword_text FROM keywords WHERE tier = 'case_outcome' ORDER BY keyword_text`)
    ]);

    const allowedFields = fieldsRes.rows.map(r => r.keyword_text);
    const allowedPostures = posturesRes.rows.map(r => r.keyword_text);
    const allowedOutcomes = outcomesRes.rows.map(r => r.keyword_text);

    // Statistics tracking
    const stats = {
      processed: 0,
      llmErrors: 0,
      validationFailed: 0,
      totalExtracted: 0
    };

    // Progress bars
    const multibar = new common.cliProgress.MultiBar({
      clearOnComplete: true,
      hideCursor: true,
      format: ' {bar} | {percentage}% | {value}/{total} | {task}'
    }, common.cliProgress.Presets.shades_classic);

    const extractBar = multibar.create(candRes.rowCount, 0, { task: 'LLM Extraction' });
    const dbBar = multibar.create(candRes.rowCount, 0, { task: 'DB Writes' });

    // Process opinions with concurrency control
    const limitLLM = pLimit(concurrency);
    const limitWrite = pLimit(writeConcurrency);
    const writePromises = [];

    await Promise.all(candRes.rows.map(row => limitLLM(async () => {
      const opinionId = row.id;
      const opinionText = row.text;
      const caseContext = {
        jurisdiction_name: row.jurisdiction_name,
        court_name: row.court_name,
        decision_date: row.decision_date
      };

      // Process even blank opinions - LLM will return empty arrays for minimal content
      const processText = opinionText && opinionText.trim() !== '' ? opinionText : 'No opinion text available';

      try {
        // Produce unified minimal payload
        // If --no-llm is provided, load from sample instead of calling the LLM
        const unifiedPayload = args.noLlm
          ? (function() {
              const p = common.loadSamplePayload(args.samplesDir, 'unified_minimal');
              if (args.debug) console.log('[unified] Using sample payload (no-llm)');
              return p;
            })()
          : await llm.generateUnified(processText, {
              allowedFields,
              allowedPostures,
              allowedOutcomes,
              caseContext,
              // Force unified_prompt; prompt returns minimal keys by design
              verboseFields: true,
            });

        // No longer checking for valueless flag - all opinions are processed

        // Validate minimal unified payload against unified_minimal_schema
        const { valid, errors } = validateUnified(unifiedPayload);
        if (!valid) {
          console.warn(`[unified] Validation failed for opinion ${opinionId}:`, errors);
          stats.validationFailed++;
          extractBar.increment();
          return;
        }

        // Expand minimal format after successful validation
        const expandedPayload = expandUnifiedResponse(unifiedPayload);

        if (args.debug) {
          console.log(`[unified] Opinion ${opinionId} extracted:`,
            `fields=${expandedPayload.field_of_law?.length || 0}`,
            `postures=${expandedPayload.procedural_posture?.length || 0}`,
            `outcomes=${expandedPayload.case_outcome?.length || 0}`,
            `factors=${expandedPayload.distinguishing_factors?.length || 0}`,
            `doctrines=${expandedPayload.doctrines?.length || 0}`,
            `tests=${expandedPayload.doctrinal_tests?.length || 0}`,
            `holdings=${expandedPayload.holdings?.length || 0}`,
            `overruled=${expandedPayload.overruled_cases?.length || 0}`,
            `citations=${expandedPayload.citations?.length || 0}`
          );
          
          // Show detailed expanded output when using both debug and dry-run flags
          if (args.dryRun) {
            console.log(`[unified] Expanded LLM output for opinion ${opinionId}:`);
            console.log(JSON.stringify(expandedPayload, null, 2));
          }
        }

        // Queue database write
        const writePromise = limitWrite(async () => {
          const operation = common.retryDbOperation(async () => {
            const cx = await client.connect();
            try {
              await cx.query('BEGIN');
              await upsertUnified(cx, opinionId, expandedPayload, {
                method: 'unified_llm',
                opinionText: processText
              });
              if (args.dryRun) {
                await cx.query('ROLLBACK');
              } else {
                await cx.query('COMMIT');
              }
              stats.totalExtracted++;
            } catch (e) {
              await cx.query('ROLLBACK');
              throw e;
            } finally {
              cx.release();
            }
          });
          common.pendingOperations.add(operation);

          try {
            await operation;
          } catch (e) {
            console.error(`[unified] DB error for opinion ${opinionId}:`, e.message);
          } finally {
            common.pendingOperations.delete(operation);
            dbBar.increment();
          }
        });
        
        writePromises.push(writePromise);

      } catch (e) {
        console.error(`[unified] LLM error for opinion ${opinionId}:`, e.message);
        stats.llmErrors++;
      } finally {
        extractBar.increment();
        stats.processed++;
      }
    })));

    // Wait for all writes to complete
    await Promise.allSettled(writePromises);
    
    extractBar.stop();
    dbBar.stop();
    multibar.stop();

    // Print summary
    console.log('\n[unified] Extraction complete!');
    console.log(`[unified] Statistics:`);
    console.log(`  - Processed: ${stats.processed}`);
    console.log(`  - Successfully extracted: ${stats.totalExtracted}`);
    console.log(`  - LLM errors: ${stats.llmErrors}`);
    console.log(`  - Validation failures: ${stats.validationFailed}`);

    // Report remaining if resume mode
    if (args.resume) {
      const remainingRes = await client.query(
        `SELECT COUNT(*)::int AS remaining
         FROM opinions o
         WHERE o.is_valueless = false
           AND NOT EXISTS (
             SELECT 1 FROM opinion_keywords ok
             WHERE ok.opinion_id = o.id
               AND ok.category IN ('field_of_law','procedural_posture','case_outcome','distinguishing_factor')
             GROUP BY ok.opinion_id
             HAVING COUNT(DISTINCT ok.category) = 4
           )`
      );
      console.log(`[unified] Remaining opinions: ${remainingRes.rows[0]?.remaining || 0}`);
    }

  } finally {
    await client.end();
  }

  console.log('[unified] Pipeline complete');
}

main().catch(err => { 
  console.error('[unified] Fatal error:', err); 
  process.exit(1); 
});
