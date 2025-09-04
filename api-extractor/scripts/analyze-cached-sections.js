#!/usr/bin/env node
import fs from 'fs/promises';
import { TextParser } from '../src/parser/text-parser.js';
import { TokenInterpolator } from '../src/parser/token-interpolator.js';

async function analyzeCachedSections(lawId = 'PEN') {
  try {
    const cacheFile = `data/cache/${lawId.toLowerCase()}-sections.json`;
    const content = await fs.readFile(cacheFile, 'utf8');
    const data = JSON.parse(content);
    
    console.log(`🔍 Analyzing ${data.sectionsCount} cached sections for ${lawId}...\n`);
    
    const parser = new TextParser();
    const interpolator = new TokenInterpolator();
    
    // Look for sections with hierarchical patterns - updated for NYSenate format
    const hierarchicalSections = [];
    const patterns = [
      /\n  \d+\. /m,             // NYSenate format: newline + 2 spaces + number + period + space
      /\n  \([a-z]\) /m,         // NYSenate format: newline + 2 spaces + (letter) + space
      /\n  \([i]+\) /m,          // NYSenate format: newline + 2 spaces + (roman) + space
      /\n  \([A-Z]\) /m,         // NYSenate format: newline + 2 spaces + (LETTER) + space
      /\n  \(\d+\) /m            // NYSenate format: newline + 2 spaces + (number) + space
    ];
    
    for (const section of data.sections) {
      if (section.docType === 'SECTION' && section.text.length > 100) {
        const hasHierarchical = patterns.some(pattern => pattern.test(section.text));
        if (hasHierarchical) {
          hierarchicalSections.push(section);
        }
      }
    }
    
    console.log(`📊 Found ${hierarchicalSections.length} sections with potential hierarchical content`);
    
    if (hierarchicalSections.length === 0) {
      console.log('❌ No sections found with hierarchical patterns. Let me check specific sections:');
      
      // Check specific sections we know have numbered content
      const testSections = ['1.05', '5.05', '5.10', '10.00'];
      for (const sectionId of testSections) {
        const section = data.sections.find(s => s.docId === sectionId);
        if (section) {
          console.log(`\n--- Section ${section.docId} ---`);
          console.log('Raw text with escapes:', JSON.stringify(section.text.substring(0, 300)));
          
          // Test each pattern individually
          patterns.forEach((pattern, i) => {
            if (pattern.test(section.text)) {
              console.log(`  ✅ Pattern ${i} matches: ${pattern}`);
              hierarchicalSections.push(section);
            }
          });
        }
      }
      
      if (hierarchicalSections.length === 0) {
        console.log('\n❌ Still no matches. Showing raw text analysis:');
        const section = data.sections.find(s => s.docId === '10.00');
        if (section) {
          console.log('Section 10.00 analysis:');
          console.log('Contains "1.": ', section.text.includes('1.'));
          console.log('Contains newline + spaces + 1.: ', /\\n\\s*1\\./.test(section.text));
          console.log('First 500 chars:', section.text.substring(0, 500));
        }
        return;
      }
    }
    
    // Test parser on hierarchical sections
    let totalTested = 0;
    let successfulParses = 0;
    let roundTripSuccesses = 0;
    
    for (const section of hierarchicalSections.slice(0, 10)) { // Test first 10
      totalTested++;
      console.log(`\n📄 Testing Section ${section.docId}`);
      console.log(`   Text length: ${section.text.length} chars`);
      console.log(`   Preview: ${section.text.substring(0, 100).replace(/\n/g, ' ')}...`);
      
      try {
        const tokens = parser.tokenizeText(section.text);
        if (tokens.length > 0) {
          successfulParses++;
          console.log(`   ✅ Parsed ${tokens.length} hierarchical elements`);
          
          // Test round-trip
          const reinterpolated = interpolator.reinterpolateTokens(section.text, tokens);
          if (reinterpolated === section.text) {
            roundTripSuccesses++;
            console.log(`   ✅ Round-trip successful`);
          } else {
            console.log(`   ⚠️  Round-trip mismatch`);
            console.log(`   Original length: ${section.text.length}, Reinterpolated: ${reinterpolated.length}`);
          }
          
          // Show first few tokens
          tokens.slice(0, 3).forEach(token => {
            console.log(`   Token: ${token.type} ${token.number} (${token.content.substring(0, 50).replace(/\n/g, ' ')}...)`);
          });
        } else {
          console.log(`   ℹ️  No hierarchical elements parsed`);
        }
      } catch (error) {
        console.log(`   ❌ Parser error: ${error.message}`);
      }
    }
    
    console.log(`\n📊 Final Results:`);
    console.log(`   Sections with hierarchical patterns: ${hierarchicalSections.length}`);
    console.log(`   Sections tested: ${totalTested}`);
    console.log(`   Successful parses: ${successfulParses}`);
    console.log(`   Round-trip successes: ${roundTripSuccesses}`);
    console.log(`   Parse success rate: ${totalTested > 0 ? Math.round((successfulParses / totalTested) * 100) : 0}%`);
    console.log(`   Round-trip success rate: ${totalTested > 0 ? Math.round((roundTripSuccesses / totalTested) * 100) : 0}%`);
    
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
  }
}

const lawId = process.argv[2] || 'PEN';
analyzeCachedSections(lawId);
