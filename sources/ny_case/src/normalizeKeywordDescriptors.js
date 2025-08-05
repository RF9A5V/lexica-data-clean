import { Client } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import cliProgress from 'cli-progress';
import fetch from 'node-fetch';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../lexica_backend/.env') });

// Configuration
const CONFIG = {
  dbUrl: 'postgresql://localhost/ny_court_of_appeals',
  batchSize: 1000, // Process descriptors in batches
  embeddingBatchSize: 32, // Match server's SUB_BATCH_SIZE for optimal performance
  maxRetries: 3,
  baseDelayMs: 1000,
  embeddingServerUrl: 'http://izanagi:8000', // Local embedding server
  embeddingDimension: 768, // LegalBERT hidden size
  timeout: 30000 // 30 second timeout
};

class KeywordDescriptorNormalizer {
  constructor() {
    this.stats = {
      totalDescriptors: 0,
      uniqueDescriptors: 0,
      migratedAssociations: 0,
      embeddingsGenerated: 0,
      errors: 0,
      startTime: Date.now()
    };
  }

  async createDescriptorsTable(pg) {
    console.log('📋 Creating descriptors table...');
    
    await pg.query(`
      CREATE TABLE IF NOT EXISTS descriptors (
        id SERIAL PRIMARY KEY,
        descriptor_text TEXT NOT NULL UNIQUE,
        embedding vector(768), -- LegalBERT embedding dimension
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Create index for efficient similarity search
    await pg.query(`
      CREATE INDEX IF NOT EXISTS idx_descriptors_text 
      ON descriptors USING gin(to_tsvector('english', descriptor_text));
    `);

    // Create index for embedding similarity search
    await pg.query(`
      CREATE INDEX IF NOT EXISTS idx_descriptors_embedding 
      ON descriptors USING ivfflat (embedding vector_cosine_ops) 
      WITH (lists = 100);
    `);

    console.log('✅ Descriptors table created successfully');
  }

  async migrateDescriptorsData(pg) {
    console.log('🔄 Migrating unique descriptors to descriptors table...');

    // Get all unique descriptors from keyword_descriptors
    const uniqueDescriptorsResult = await pg.query(`
      SELECT DISTINCT descriptor_text 
      FROM keyword_descriptors 
      ORDER BY descriptor_text
    `);

    this.stats.uniqueDescriptors = uniqueDescriptorsResult.rows.length;
    console.log(`📊 Found ${this.stats.uniqueDescriptors} unique descriptors to migrate`);

    // Insert unique descriptors in batches
    const progressBar = new cliProgress.SingleBar({
      format: 'Migrating Descriptors |{bar}| {percentage}% | {value}/{total} | Errors: {errors}',
      hideCursor: true
    }, cliProgress.Presets.shades_classic);

    progressBar.start(this.stats.uniqueDescriptors, 0, { errors: 0 });

    for (let i = 0; i < uniqueDescriptorsResult.rows.length; i += CONFIG.batchSize) {
      const batch = uniqueDescriptorsResult.rows.slice(i, i + CONFIG.batchSize);
      
      try {
        // Use INSERT ... ON CONFLICT DO NOTHING to handle any potential duplicates
        const values = batch.map((_, index) => `($${index + 1})`).join(',');
        const params = batch.map(row => row.descriptor_text);
        
        await pg.query(`
          INSERT INTO descriptors (descriptor_text) 
          VALUES ${values}
          ON CONFLICT (descriptor_text) DO NOTHING
        `, params);

        progressBar.update(Math.min(i + CONFIG.batchSize, this.stats.uniqueDescriptors), {
          errors: this.stats.errors
        });

      } catch (error) {
        this.stats.errors++;
        console.error(`\n❌ Error migrating batch starting at ${i}:`, error.message);
        progressBar.update(Math.min(i + CONFIG.batchSize, this.stats.uniqueDescriptors), {
          errors: this.stats.errors
        });
      }
    }

    progressBar.stop();
    console.log('✅ Descriptors migration completed');
  }

  async generateEmbeddings(pg) {
    console.log('🧠 Generating embeddings for descriptors...');

    // Get descriptors without embeddings
    const descriptorsResult = await pg.query(`
      SELECT id, descriptor_text 
      FROM descriptors 
      WHERE embedding IS NULL 
      ORDER BY id
    `);

    if (descriptorsResult.rows.length === 0) {
      console.log('✅ All descriptors already have embeddings');
      return;
    }

    console.log(`📊 Generating embeddings for ${descriptorsResult.rows.length} descriptors`);
    console.log(`🔗 Using embedding server: ${CONFIG.embeddingServerUrl}`);

    const progressBar = new cliProgress.SingleBar({
      format: 'Generating Embeddings |{bar}| {percentage}% | {value}/{total} | Errors: {errors}',
      hideCursor: true
    }, cliProgress.Presets.shades_classic);

    progressBar.start(descriptorsResult.rows.length, 0, { errors: 0 });

    for (let i = 0; i < descriptorsResult.rows.length; i += CONFIG.embeddingBatchSize) {
      const batch = descriptorsResult.rows.slice(i, i + CONFIG.embeddingBatchSize);
      
      try {
        // Generate embeddings for the batch using local embedding server
        const texts = batch.map(row => row.descriptor_text);
        const embeddings = await this.generateEmbeddingsBatch(texts);

        // Update each descriptor with its embedding
        for (let j = 0; j < batch.length; j++) {
          const descriptor = batch[j];
          const embedding = embeddings[j];
          
          // Validate embedding dimension
          if (embedding.length !== CONFIG.embeddingDimension) {
            throw new Error(`Invalid embedding dimension: expected ${CONFIG.embeddingDimension}, got ${embedding.length}`);
          }
          
          await pg.query(`
            UPDATE descriptors 
            SET embedding = $1 
            WHERE id = $2
          `, [JSON.stringify(embedding), descriptor.id]);
          
          this.stats.embeddingsGenerated++;
        }

        progressBar.update(Math.min(i + CONFIG.embeddingBatchSize, descriptorsResult.rows.length), {
          errors: this.stats.errors
        });

        // Small delay between batches to be nice to the server
        await new Promise(resolve => setTimeout(resolve, 100));

      } catch (error) {
        this.stats.errors++;
        console.error(`\n❌ Error generating embeddings for batch starting at ${i}:`, error.message);
        
        // Exponential backoff on server errors
        if (error.message.includes('timeout') || error.message.includes('ECONNREFUSED')) {
          const delay = CONFIG.baseDelayMs * Math.pow(2, Math.min(this.stats.errors, 5));
          console.log(`⏳ Server error, waiting ${delay}ms before retry...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
        
        progressBar.update(Math.min(i + CONFIG.embeddingBatchSize, descriptorsResult.rows.length), {
          errors: this.stats.errors
        });
      }
    }

    progressBar.stop();
    console.log('✅ Embedding generation completed');
  }

  // Generate embeddings using the local embedding server (batch endpoint)
  async generateEmbeddingsBatch(texts) {
    if (!texts || texts.length === 0) {
      throw new Error('No texts provided for embedding generation');
    }

    // Filter out empty texts
    const validTexts = texts.filter(text => text && text.trim().length > 0);
    if (validTexts.length === 0) {
      throw new Error('All texts are empty');
    }

    const url = `${CONFIG.embeddingServerUrl}/embed_batch`;
    const requestBody = { texts: validTexts };

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(requestBody),
      timeout: CONFIG.timeout
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP error ${response.status}: ${errorText}`);
    }

    const result = await response.json();
    
    if (!result.embeddings || !Array.isArray(result.embeddings)) {
      throw new Error(`Invalid response format: expected embeddings array, got ${JSON.stringify(result)}`);
    }

    if (result.embeddings.length !== validTexts.length) {
      throw new Error(`Embedding count mismatch: expected ${validTexts.length}, got ${result.embeddings.length}`);
    }

    // Validate each embedding dimension
    for (let i = 0; i < result.embeddings.length; i++) {
      const embedding = result.embeddings[i];
      if (!Array.isArray(embedding) || embedding.length !== CONFIG.embeddingDimension) {
        throw new Error(`Invalid embedding dimension at index ${i}: expected ${CONFIG.embeddingDimension}, got ${embedding?.length || 'not an array'}`);
      }
    }

    return result.embeddings;
  }

  async updateKeywordDescriptorsTable(pg) {
    console.log('🔄 Updating keyword_descriptors table structure...');

    // First, create a backup table
    await pg.query(`
      CREATE TABLE IF NOT EXISTS keyword_descriptors_backup AS 
      SELECT * FROM keyword_descriptors
    `);
    console.log('💾 Created backup table: keyword_descriptors_backup');

    // Add descriptor_id column to keyword_descriptors
    await pg.query(`
      ALTER TABLE keyword_descriptors 
      ADD COLUMN IF NOT EXISTS descriptor_id INTEGER
    `);

    // Populate descriptor_id by joining with descriptors table
    console.log('🔗 Populating descriptor_id references...');
    
    const updateResult = await pg.query(`
      UPDATE keyword_descriptors kd
      SET descriptor_id = d.id
      FROM descriptors d
      WHERE kd.descriptor_text = d.descriptor_text
    `);

    this.stats.migratedAssociations = updateResult.rowCount;
    console.log(`✅ Updated ${this.stats.migratedAssociations} keyword-descriptor associations`);

    // Add foreign key constraint
    await pg.query(`
      ALTER TABLE keyword_descriptors 
      ADD CONSTRAINT fk_keyword_descriptors_descriptor_id 
      FOREIGN KEY (descriptor_id) REFERENCES descriptors(id) ON DELETE CASCADE
    `);

    // Create index on descriptor_id
    await pg.query(`
      CREATE INDEX IF NOT EXISTS idx_keyword_descriptors_descriptor_id 
      ON keyword_descriptors(descriptor_id)
    `);

    console.log('✅ Added foreign key constraint and index');
  }

  async dropOldDescriptorTextColumn(pg) {
    console.log('🗑️  Dropping old descriptor_text column...');
    
    // Verify all rows have descriptor_id populated
    const nullCountResult = await pg.query(`
      SELECT COUNT(*) as null_count 
      FROM keyword_descriptors 
      WHERE descriptor_id IS NULL
    `);

    const nullCount = parseInt(nullCountResult.rows[0].null_count);
    
    if (nullCount > 0) {
      console.warn(`⚠️  Warning: ${nullCount} rows still have NULL descriptor_id. Skipping column drop.`);
      return;
    }

    // Drop the old descriptor_text column
    await pg.query(`
      ALTER TABLE keyword_descriptors 
      DROP COLUMN descriptor_text
    `);

    // Drop the old unique constraint (it's no longer needed)
    await pg.query(`
      ALTER TABLE keyword_descriptors 
      DROP CONSTRAINT IF EXISTS keyword_descriptors_keyword_id_descriptor_text_key
    `);

    // Add new unique constraint on keyword_id, descriptor_id
    await pg.query(`
      ALTER TABLE keyword_descriptors 
      ADD CONSTRAINT keyword_descriptors_keyword_id_descriptor_id_key 
      UNIQUE (keyword_id, descriptor_id)
    `);

    console.log('✅ Dropped old descriptor_text column and updated constraints');
  }

  async validateMigration(pg) {
    console.log('🔍 Validating migration...');

    // Check descriptors table
    const descriptorsCount = await pg.query('SELECT COUNT(*) as count FROM descriptors');
    console.log(`📊 Descriptors table: ${descriptorsCount.rows[0].count} unique descriptors`);

    // Check embeddings
    const embeddingsCount = await pg.query('SELECT COUNT(*) as count FROM descriptors WHERE embedding IS NOT NULL');
    console.log(`🧠 Embeddings generated: ${embeddingsCount.rows[0].count} descriptors`);

    // Check keyword_descriptors associations
    const associationsCount = await pg.query('SELECT COUNT(*) as count FROM keyword_descriptors WHERE descriptor_id IS NOT NULL');
    console.log(`🔗 Valid associations: ${associationsCount.rows[0].count} keyword-descriptor links`);

    // Check for any orphaned records
    const orphanedCount = await pg.query(`
      SELECT COUNT(*) as count 
      FROM keyword_descriptors kd 
      LEFT JOIN descriptors d ON kd.descriptor_id = d.id 
      WHERE d.id IS NULL AND kd.descriptor_id IS NOT NULL
    `);
    console.log(`🚫 Orphaned associations: ${orphanedCount.rows[0].count}`);

    // Sample join query to verify functionality
    const sampleResult = await pg.query(`
      SELECT k.keyword_text, d.descriptor_text 
      FROM keyword_descriptors kd
      JOIN keywords k ON kd.keyword_id = k.id
      JOIN descriptors d ON kd.descriptor_id = d.id
      LIMIT 5
    `);

    console.log('\n📋 Sample joined data:');
    sampleResult.rows.forEach((row, i) => {
      console.log(`${i + 1}. ${row.keyword_text} -> ${row.descriptor_text.substring(0, 80)}...`);
    });
  }

  printStats() {
    const duration = (Date.now() - this.stats.startTime) / 1000;
    console.log('\n' + '='.repeat(60));
    console.log('📊 MIGRATION STATISTICS');
    console.log('='.repeat(60));
    console.log(`⏱️  Total Duration: ${duration.toFixed(1)}s`);
    console.log(`📄 Unique Descriptors: ${this.stats.uniqueDescriptors}`);
    console.log(`🔗 Migrated Associations: ${this.stats.migratedAssociations}`);
    console.log(`🧠 Embeddings Generated: ${this.stats.embeddingsGenerated}`);
    console.log(`❌ Errors: ${this.stats.errors}`);
    console.log('='.repeat(60));
  }
}

async function main() {
  console.log('🚀 Starting Keyword Descriptors Normalization');
  console.log(`🔗 Using embedding server: ${CONFIG.embeddingServerUrl}`);
  
  // Test embedding server connectivity
  try {
    const testResponse = await fetch(`${CONFIG.embeddingServerUrl}/embed`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: 'test connection' }),
      timeout: 5000
    });
    
    if (!testResponse.ok) {
      console.error(`❌ Embedding server not responding: HTTP ${testResponse.status}`);
      process.exit(1);
    }
    
    console.log('✅ Embedding server connection verified');
  } catch (error) {
    console.error(`❌ Cannot connect to embedding server at ${CONFIG.embeddingServerUrl}:`, error.message);
    console.error('   Make sure the embedding server is running and accessible.');
    process.exit(1);
  }

  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();
  
  const normalizer = new KeywordDescriptorNormalizer();

  try {
    // Get initial statistics
    const initialStats = await pg.query(`
      SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT descriptor_text) as unique_descriptors
      FROM keyword_descriptors
    `);
    
    normalizer.stats.totalDescriptors = parseInt(initialStats.rows[0].total_rows);
    console.log(`📊 Initial state: ${normalizer.stats.totalDescriptors} total rows, ${initialStats.rows[0].unique_descriptors} unique descriptors`);

    // Step 1: Create descriptors table
    await normalizer.createDescriptorsTable(pg);

    // Step 2: Migrate unique descriptors
    await normalizer.migrateDescriptorsData(pg);

    // Step 3: Generate embeddings
    await normalizer.generateEmbeddings(pg);

    // Step 4: Update keyword_descriptors table structure
    await normalizer.updateKeywordDescriptorsTable(pg);

    // Step 5: Drop old column (optional - user can run this manually if desired)
    console.log('\n⚠️  Skipping automatic drop of descriptor_text column for safety.');
    console.log('   Run the following manually after verifying the migration:');
    console.log('   ALTER TABLE keyword_descriptors DROP COLUMN descriptor_text;');

    // Step 6: Validate migration
    await normalizer.validateMigration(pg);

    // Print final statistics
    normalizer.printStats();
    
    console.log('\n✅ Migration completed successfully!');
    console.log('\n📋 Next steps:');
    console.log('1. Verify the migration results above');
    console.log('2. Test your application with the new structure');
    console.log('3. If everything works, manually drop the descriptor_text column');
    console.log('4. Drop the backup table: DROP TABLE keyword_descriptors_backup;');
    
  } catch (error) {
    console.error('❌ Migration failed:', error);
    console.log('\n🔄 Rollback instructions:');
    console.log('1. DROP TABLE descriptors;');
    console.log('2. ALTER TABLE keyword_descriptors DROP COLUMN descriptor_id;');
    console.log('3. Restore from backup if needed');
  } finally {
    await pg.end();
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n[INFO] Received SIGINT, shutting down gracefully...');
  process.exit(0);
});

main().catch(console.error);
