import { Client } from 'pg';
import path from 'path';
import dotenv from 'dotenv';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, '../../../lexica_backend/.env') });

const CONFIG = {
  dbUrl: 'postgresql://localhost/ny_court_of_appeals'
};

async function testMigration() {
  console.log('🧪 Testing Keyword Descriptors Migration');
  
  const pg = new Client({ connectionString: CONFIG.dbUrl });
  await pg.connect();

  try {
    // Test 1: Check if descriptors table exists and has data
    console.log('\n1. Testing descriptors table...');
    const descriptorsResult = await pg.query(`
      SELECT 
        COUNT(*) as total_descriptors,
        COUNT(CASE WHEN embedding IS NOT NULL THEN 1 END) as with_embeddings
      FROM descriptors
    `);
    
    console.log(`   ✅ Descriptors table: ${descriptorsResult.rows[0].total_descriptors} unique descriptors`);
    console.log(`   🧠 With embeddings: ${descriptorsResult.rows[0].with_embeddings} descriptors`);

    // Test 2: Check keyword_descriptors structure
    console.log('\n2. Testing keyword_descriptors table structure...');
    const structureResult = await pg.query(`
      SELECT column_name, data_type, is_nullable 
      FROM information_schema.columns 
      WHERE table_name = 'keyword_descriptors' 
      ORDER BY ordinal_position
    `);
    
    console.log('   📋 Current columns:');
    structureResult.rows.forEach(row => {
      console.log(`      - ${row.column_name}: ${row.data_type} (nullable: ${row.is_nullable})`);
    });

    // Test 3: Check data integrity
    console.log('\n3. Testing data integrity...');
    const integrityResult = await pg.query(`
      SELECT 
        COUNT(*) as total_associations,
        COUNT(CASE WHEN descriptor_id IS NOT NULL THEN 1 END) as with_descriptor_id,
        COUNT(CASE WHEN descriptor_text IS NOT NULL THEN 1 END) as with_descriptor_text
      FROM keyword_descriptors
    `);
    
    console.log(`   🔗 Total associations: ${integrityResult.rows[0].total_associations}`);
    console.log(`   🆔 With descriptor_id: ${integrityResult.rows[0].with_descriptor_id}`);
    console.log(`   📝 With descriptor_text: ${integrityResult.rows[0].with_descriptor_text || 0}`);

    // Test 4: Test join functionality
    console.log('\n4. Testing join functionality...');
    const joinResult = await pg.query(`
      SELECT 
        k.keyword_text,
        d.descriptor_text,
        CASE WHEN d.embedding IS NOT NULL THEN 'Yes' ELSE 'No' END as has_embedding
      FROM keyword_descriptors kd
      JOIN keywords k ON kd.keyword_id = k.id
      JOIN descriptors d ON kd.descriptor_id = d.id
      LIMIT 5
    `);
    
    if (joinResult.rows.length > 0) {
      console.log('   ✅ Join working correctly. Sample data:');
      joinResult.rows.forEach((row, i) => {
        console.log(`      ${i + 1}. "${row.keyword_text}" -> "${row.descriptor_text.substring(0, 60)}..." (embedding: ${row.has_embedding})`);
      });
    } else {
      console.log('   ❌ Join failed - no results returned');
    }

    // Test 5: Check for orphaned records
    console.log('\n5. Checking for orphaned records...');
    const orphanedResult = await pg.query(`
      SELECT COUNT(*) as orphaned_count
      FROM keyword_descriptors kd
      LEFT JOIN descriptors d ON kd.descriptor_id = d.id
      WHERE kd.descriptor_id IS NOT NULL AND d.id IS NULL
    `);
    
    const orphanedCount = parseInt(orphanedResult.rows[0].orphaned_count);
    if (orphanedCount === 0) {
      console.log('   ✅ No orphaned records found');
    } else {
      console.log(`   ⚠️  Found ${orphanedCount} orphaned records`);
    }

    // Test 6: Performance test - sample similarity search (if embeddings exist)
    console.log('\n6. Testing embedding similarity search...');
    const embeddingTestResult = await pg.query(`
      SELECT COUNT(*) as embeddings_count 
      FROM descriptors 
      WHERE embedding IS NOT NULL
    `);
    
    const embeddingsCount = parseInt(embeddingTestResult.rows[0].embeddings_count);
    if (embeddingsCount > 0) {
      console.log(`   ✅ ${embeddingsCount} descriptors have embeddings (768-dimensional vectors)`);
      
      // Try a sample similarity search
      const similarityResult = await pg.query(`
        SELECT descriptor_text, 
               1 - (embedding <=> (
          SELECT embedding FROM descriptors WHERE embedding IS NOT NULL LIMIT 1
        )) as similarity
        FROM descriptors 
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> (
          SELECT embedding FROM descriptors WHERE embedding IS NOT NULL LIMIT 1
        )
        LIMIT 3
      `);
      
      console.log('   🔍 Sample similarity search results:');
      similarityResult.rows.forEach((row, i) => {
        console.log(`      ${i + 1}. "${row.descriptor_text.substring(0, 50)}..." (similarity: ${parseFloat(row.similarity).toFixed(3)})`);
      });
    } else {
      console.log('   ⚠️  No embeddings found - run the full migration script to generate embeddings');
      console.log('   📡 Embeddings will be generated using local server: http://izanagi:8000');
    }

    // Test 7: Storage efficiency comparison
    console.log('\n7. Storage efficiency analysis...');
    const efficiencyResult = await pg.query(`
      SELECT 
        'Before (estimated)' as scenario,
        COUNT(*) * AVG(LENGTH(d.descriptor_text)) as estimated_storage_bytes
      FROM keyword_descriptors kd
      JOIN descriptors d ON kd.descriptor_id = d.id
      UNION ALL
      SELECT 
        'After (descriptors table)' as scenario,
        SUM(LENGTH(descriptor_text)) as actual_storage_bytes
      FROM descriptors
    `);
    
    console.log('   💾 Storage comparison:');
    efficiencyResult.rows.forEach(row => {
      const mb = (parseInt(row.estimated_storage_bytes) / 1024 / 1024).toFixed(2);
      console.log(`      ${row.scenario}: ~${mb} MB`);
    });

    console.log('\n✅ Migration test completed successfully!');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await pg.end();
  }
}

testMigration().catch(console.error);
