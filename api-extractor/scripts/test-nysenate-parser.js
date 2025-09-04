#!/usr/bin/env node
import fs from 'fs/promises';
import { TextParser } from '../src/parser/text-parser.js';
import { TokenInterpolator } from '../src/parser/token-interpolator.js';

async function testNYSenateParser() {
  try {
    const cacheFile = 'data/cache/pen-sections.json';
    const content = await fs.readFile(cacheFile, 'utf8');
    const data = JSON.parse(content);
    
    const parser = new TextParser();
    const interpolator = new TokenInterpolator();
    
    // Test with section 10.00 which we know has numbered content
    const section = data.sections.find(s => s.docId === '10.00');
    if (!section) {
      console.error('❌ Section 10.00 not found');
      return;
    }
    
    console.log('🧪 Testing NYSenate Parser with Section 10.00');
    console.log('📄 Section text preview:');
    console.log(section.text.substring(0, 500));
    console.log('\n' + '='.repeat(50));
    
    // Test line-by-line parsing
    const lines = section.text.split('\n');
    console.log(`\n📊 Analyzing ${lines.length} lines:`);
    console.log('First 10 lines:');
    lines.slice(0, 10).forEach((line, i) => {
      console.log(`  ${i}: "${line}"`);
    });
    
    let foundElements = 0;
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const element = parser.identifyHierarchicalElement(line);
      if (element) {
        foundElements++;
        console.log(`  Line ${i}: "${line}" -> ${element.name}:${element.number}`);
        if (foundElements >= 5) break; // Show first 5 matches
      }
    }
    
    if (foundElements === 0) {
      console.log('❌ No hierarchical elements found. Testing patterns manually:');
      
      // Test specific lines manually
      const testLines = [
        '  1. "Offense" means conduct for which a sentence to a term of',
        '  2. "Traffic infraction" means any offense defined as "traffic',
        '  (a) Application rules apply.',
        '1. To proscribe conduct which unjustifiably and inexcusably causes or'
      ];
      
      testLines.forEach((testLine, i) => {
        console.log(`\nTesting line: "${testLine}"`);
        const element = parser.identifyHierarchicalElement(testLine);
        if (element) {
          console.log(`  ✅ Matched: ${element.name}:${element.number}`);
        } else {
          console.log(`  ❌ No match`);
          
          // Test each pattern individually
          parser.patterns.forEach((pattern, j) => {
            if (pattern.test(testLine)) {
              console.log(`    Pattern ${j} (${pattern.name}) would match`);
            }
          });
        }
      });
    }
    
    // Test full tokenization
    console.log('\n🔄 Testing full tokenization:');
    try {
      const result = parser.tokenizeText(section.text);
      const tokens = result.childElements || [];
      console.log(`✅ Generated ${tokens.length} tokens`);
      
      if (tokens.length > 0) {
        console.log('First few tokens:');
        tokens.slice(0, 3).forEach(token => {
          console.log(`  ${token.type} ${token.number}: ${token.content.substring(0, 80)}...`);
        });
        
        // Test round-trip
        console.log('\n🔄 Testing round-trip interpolation:');
        const reinterpolated = interpolator.reinterpolateTokens(section.text, tokens);
        const matches = reinterpolated === section.text;
        console.log(`Round-trip ${matches ? '✅ SUCCESS' : '❌ FAILED'}`);
        
        if (!matches) {
          console.log(`Original length: ${section.text.length}`);
          console.log(`Reinterpolated length: ${reinterpolated.length}`);
          
          // Find first difference
          for (let i = 0; i < Math.min(section.text.length, reinterpolated.length); i++) {
            if (section.text[i] !== reinterpolated[i]) {
              console.log(`First difference at position ${i}:`);
              console.log(`Original: "${section.text.substring(i-10, i+10)}"`);
              console.log(`Reinterpolated: "${reinterpolated.substring(i-10, i+10)}"`);
              break;
            }
          }
        }
      }
    } catch (error) {
      console.error(`❌ Tokenization error: ${error.message}`);
    }
    
  } catch (error) {
    console.error(`❌ Error: ${error.message}`);
  }
}

testNYSenateParser();
