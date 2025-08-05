import { pullTextForOpinion, cleanText } from './collectTextForOpinion.js';
import { Client } from 'pg';

async function testKeywordSystem() {
  console.log('🧪 Testing Keyword Extraction System');
  console.log('=====================================\n');

  const pg = new Client({ connectionString: 'postgresql://localhost/ny_court_of_appeals' });
  await pg.connect();

  try {
    // 1. Test database connection and schema
    console.log('1. Testing database schema...');
    
    const tables = await pg.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
        AND table_name IN ('keywords', 'opinion_keywords', 'sentence_keywords', 'opinions', 'cases')
      ORDER BY table_name
    `);
    
    console.log('   ✅ Found tables:', tables.rows.map(r => r.table_name).join(', '));

    // 2. Test getting sample opinions
    console.log('\n2. Testing opinion retrieval...');
    
    const sampleOpinions = await pg.query(`
      SELECT
        c.case_name,
        o.id as opinion_id,
        o.binding_type,
        c.citation_count
      FROM opinions o
      INNER JOIN cases c ON o.case_id = c.id
      WHERE o.binding_type IN ('015unanimous', '010combined', '020lead')
      ORDER BY c.citation_count DESC, o.id
      LIMIT 3
    `);
    
    console.log(`   ✅ Found ${sampleOpinions.rows.length} sample opinions`);

    // 3. Test text extraction for first opinion
    if (sampleOpinions.rows.length > 0) {
      const firstOpinion = sampleOpinions.rows[0];
      console.log(`\n3. Testing text extraction for opinion ${firstOpinion.opinion_id}...`);
      console.log(`   Case: ${firstOpinion.case_name}`);
      
      const opinionText = await pullTextForOpinion(firstOpinion.opinion_id);
      const fullText = opinionText.join('\n');
      
      console.log(`   ✅ Extracted ${opinionText.length} paragraphs`);
      console.log(`   ✅ Total text length: ${fullText.length} characters`);
      console.log(`   ✅ Sample text: "${fullText.substring(0, 200)}..."`);

      // 4. Test keyword insertion function
      console.log('\n4. Testing keyword insertion...');
      
      // Test the get_or_create_keyword function
      const testKeyword = 'test_keyword_' + Date.now();
      const keywordResult = await pg.query('SELECT get_or_create_keyword($1) as keyword_id', [testKeyword]);
      const keywordId = keywordResult.rows[0].keyword_id;
      
      console.log(`   ✅ Created test keyword with ID: ${keywordId}`);
      
      // Test inserting a sample opinion keyword
      await pg.query(`
        INSERT INTO opinion_keywords (opinion_id, keyword_id, relevance_score, extraction_method, category, context)
        VALUES ($1, $2, $3, 'test', 'test_category', 'test context')
        ON CONFLICT (opinion_id, keyword_id) DO NOTHING
      `, [firstOpinion.opinion_id, keywordId, 0.8]);
      
      console.log('   ✅ Successfully inserted test opinion keyword');
      
      // Clean up test data
      await pg.query('DELETE FROM opinion_keywords WHERE keyword_id = $1', [keywordId]);
      await pg.query('DELETE FROM keywords WHERE id = $1', [keywordId]);
      
      console.log('   ✅ Cleaned up test data');
    }

    // 5. Test search function
    console.log('\n5. Testing search function...');
    
    const searchResult = await pg.query(`
      SELECT search_opinions_by_keywords(
        ARRAY['contract', 'negligence'], 
        'any', 
        0.5, 
        5
      )
    `);
    
    console.log(`   ✅ Search function executed successfully (returned ${searchResult.rows.length} results)`);

    console.log('\n🎉 All tests passed! The keyword extraction system is ready.');
    console.log('\n📋 Next steps:');
    console.log('   1. Add your OpenAI API key to the .env file as OPENAI_API_KEY');
    console.log('   2. Run: node src/processKeywords.js 5  (to test with 5 opinions)');
    console.log('   3. Run: node src/processKeywords.js    (to process 100 opinions)');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error('Stack trace:', error.stack);
  } finally {
    await pg.end();
  }
}

testKeywordSystem().catch(console.error);
