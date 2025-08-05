import { Client } from "pg";
import { fetchEmbeddings, toPgvectorString } from "../../federal/embed/embedding.js";
import cliProgress from "cli-progress";
import dotenv from "dotenv";
import path, { dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../.env') });

/**
 * Embed keyword descriptors for semantic search
 * This script generates embeddings for keyword descriptors to enable
 * semantic similarity search across legal keywords
 */

async function embedKeywordDescriptors(options = {}) {
  const {
    batchSize = 50,
    concurrency = 3,
    startId = 0,
    endId = null,
    force = false
  } = options;

  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  try {
    // Check if embedding column exists, add if not
    await ensureEmbeddingColumn(pg);

    // Get keywords with descriptors that need embedding
    const whereClause = force 
      ? "WHERE kd.descriptor_text IS NOT NULL"
      : "WHERE kd.descriptor_text IS NOT NULL AND k.descriptor_embedding IS NULL";
    
    const idRangeClause = endId 
      ? `AND k.id BETWEEN ${startId} AND ${endId}`
      : startId > 0 ? `AND k.id >= ${startId}` : "";

    const countQuery = `
      SELECT COUNT(DISTINCT k.id) as total
      FROM keywords k
      JOIN keyword_descriptors kd ON k.id = kd.keyword_id
      ${whereClause} ${idRangeClause}
    `;

    const { rows: [{ total }] } = await pg.query(countQuery);
    
    if (total === 0) {
      console.log("✅ All keyword descriptors already have embeddings or no descriptors found");
      await pg.end();
      return;
    }

    console.log(`📊 Found ${total} keywords with descriptors to embed`);

    // Get keywords with their descriptors
    const keywordsQuery = `
      SELECT 
        k.id,
        k.keyword_text,
        k.tier,
        ARRAY_AGG(kd.descriptor_text) as descriptors,
        STRING_AGG(kd.descriptor_text, ' | ') as combined_descriptors
      FROM keywords k
      JOIN keyword_descriptors kd ON k.id = kd.keyword_id
      ${whereClause} ${idRangeClause}
      GROUP BY k.id, k.keyword_text, k.tier
      ORDER BY k.id
    `;

    const { rows: keywords } = await pg.query(keywordsQuery);

    // Progress bar setup
    const bar = new cliProgress.SingleBar({
      format: 'Embedding Progress |{bar}| {percentage}% || {value}/{total} keywords',
      barCompleteChar: '█',
      barIncompleteChar: '░',
      hideCursor: true
    });
    bar.start(total, 0);

    let processed = 0;
    let errors = 0;
    let skipped = 0;

    // Process in batches
    for (let i = 0; i < keywords.length; i += batchSize) {
      const batch = keywords.slice(i, i + batchSize);
      
      try {
        // Generate embeddings for combined descriptors
        const texts = batch.map(k => k.combined_descriptors);
        const embeddings = await fetchEmbeddings(texts);

        // Update keywords with embeddings
        const updateValues = batch.map((keyword, idx) => 
          `(${keyword.id}, '${toPgvectorString(embeddings[idx])}')`
        ).join(', ');

        const updateQuery = `
          UPDATE keywords AS k
          SET descriptor_embedding = v.embedding::vector
          FROM (VALUES ${updateValues}) AS v(id, embedding)
          WHERE k.id = v.id::integer
        `;

        await pg.query(updateQuery);
        processed += batch.length;
        bar.update(processed);

        // Log progress every 100 keywords
        if (processed % 100 === 0) {
          console.log(`\n🔄 Processed ${processed}/${total} keywords...`);
        }

      } catch (error) {
        console.error(`\n❌ Error processing batch starting at index ${i}:`, error.message);
        errors += batch.length;
        
        // Continue with next batch instead of failing entirely
        processed += batch.length;
        bar.update(processed);
      }
    }

    bar.stop();

    // Final summary
    console.log('\n📊 Embedding Summary:');
    console.log(`✅ Successfully embedded: ${processed - errors} keywords`);
    console.log(`❌ Errors: ${errors} keywords`);
    console.log(`⏭️  Skipped: ${skipped} keywords`);

    // Show sample results
    const sampleQuery = `
      SELECT k.keyword_text, k.tier, k.descriptor_embedding IS NOT NULL as has_embedding
      FROM keywords k
      WHERE k.descriptor_embedding IS NOT NULL
      LIMIT 5
    `;
    
    const { rows: samples } = await pg.query(sampleQuery);
    if (samples.length > 0) {
      console.log('\n🔍 Sample embedded keywords:');
      samples.forEach(k => {
        console.log(`  • "${k.keyword_text}" (${k.tier}) - ${k.has_embedding ? '✅' : '❌'}`);
      });
    }

    // Show embedding statistics
    const statsQuery = `
      SELECT 
        tier,
        COUNT(*) as total_keywords,
        COUNT(descriptor_embedding) as embedded_keywords,
        ROUND(COUNT(descriptor_embedding) * 100.0 / COUNT(*), 2) as embedding_percentage
      FROM keywords k
      WHERE EXISTS (
        SELECT 1 FROM keyword_descriptors kd 
        WHERE kd.keyword_id = k.id
      )
      GROUP BY tier
      ORDER BY tier
    `;

    const { rows: stats } = await pg.query(statsQuery);
    console.log('\n📈 Embedding Statistics by Tier:');
    stats.forEach(stat => {
      console.log(`  ${stat.tier}: ${stat.embedded_keywords}/${stat.total_keywords} (${stat.embedding_percentage}%)`);
    });

  } catch (error) {
    console.error('💥 Fatal error:', error);
    throw error;
  } finally {
    await pg.end();
  }
}

/**
 * Ensure the descriptor_embedding column exists in keywords table
 */
async function ensureEmbeddingColumn(pg) {
  try {
    // Check if column exists
    const checkQuery = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'keywords' AND column_name = 'descriptor_embedding'
    `;
    
    const { rows } = await pg.query(checkQuery);
    
    if (rows.length === 0) {
      console.log('🔧 Adding descriptor_embedding column to keywords table...');
      await pg.query(`
        ALTER TABLE keywords 
        ADD COLUMN descriptor_embedding vector(1536)
      `);
      console.log('✅ Column added successfully');
    } else {
      console.log('✅ descriptor_embedding column already exists');
    }
  } catch (error) {
    console.error('❌ Error ensuring embedding column:', error);
    throw error;
  }
}

/**
 * Reset embeddings for all keywords (use with caution)
 */
async function resetEmbeddings() {
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  try {
    console.log('🔄 Resetting all keyword descriptor embeddings...');
    await pg.query('UPDATE keywords SET descriptor_embedding = NULL');
    console.log('✅ All embeddings reset');
  } catch (error) {
    console.error('❌ Error resetting embeddings:', error);
  } finally {
    await pg.end();
  }
}

/**
 * Check embedding status
 */
async function checkEmbeddingStatus() {
  const pg = new Client({ connectionString: process.env.NY_STATE_APPEALS_DB });
  await pg.connect();

  try {
    const statusQuery = `
      SELECT 
        COUNT(DISTINCT k.id) as total_keywords_with_descriptors,
        COUNT(k.descriptor_embedding) as embedded_keywords,
        ROUND(COUNT(k.descriptor_embedding) * 100.0 / COUNT(DISTINCT k.id), 2) as completion_percentage
      FROM keywords k
      WHERE EXISTS (
        SELECT 1 FROM keyword_descriptors kd 
        WHERE kd.keyword_id = k.id
      )
    `;

    const { rows: [status] } = await pg.query(statusQuery);
    
    console.log('\n📊 Current Embedding Status:');
    console.log(`Total keywords with descriptors: ${status.total_keywords_with_descriptors}`);
    console.log(`Embedded keywords: ${status.embedded_keywords}`);
    console.log(`Completion: ${status.completion_percentage}%`);

    return status;
  } catch (error) {
    console.error('❌ Error checking status:', error);
  } finally {
    await pg.end();
  }
}

// CLI handling
const [,, command = 'embed', ...args] = process.argv;

if (import.meta.url === `file://${process.argv[1]}`) {
  switch (command) {
    case 'embed':
      const batchSize = parseInt(args[0]) || 50;
      const concurrency = parseInt(args[1]) || 3;
      embedKeywordDescriptors({ batchSize, concurrency });
      break;
    
    case 'reset':
      resetEmbeddings();
      break;
    
    case 'status':
      checkEmbeddingStatus();
      break;
    
    case 'help':
    default:
      console.log(`
Usage: node embedKeywordDescriptors.js [command] [options]

Commands:
  embed [batchSize] [concurrency]  - Embed keyword descriptors (default: 50, 3)
  reset                            - Reset all keyword descriptor embeddings
  status                           - Check current embedding status
  help                             - Show this help message

Examples:
  node embedKeywordDescriptors.js embed 100 5
  node embedKeywordDescriptors.js embed 25 2
  node embedKeywordDescriptors.js status
  node embedKeywordDescriptors.js reset
      `);
      break;
  }
}

export { embedKeywordDescriptors, resetEmbeddings, checkEmbeddingStatus };
