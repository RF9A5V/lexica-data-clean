#!/usr/bin/env node

/**
 * Test script for dynamic XML structure analyzer
 */

import { analyzeXmlStructure, printAnalysis, generateConfigFromAnalysis } from './src/xml_structure_analyzer.js';

async function testAnalyzer() {
  const xmlFile = process.argv[2] || 'data/staging/nyc-admin/0-0-0-1.xml';

  console.log(`Analyzing XML structure of: ${xmlFile}`);

  const analysis = await analyzeXmlStructure(xmlFile);

  if (analysis) {
    printAnalysis(analysis);

    // Generate configuration
    const config = generateConfigFromAnalysis(analysis, 'nyc-admin');
    if (config) {
      console.log('\n=== Generated Configuration ===');
      console.log(JSON.stringify(config, null, 2));
    } else {
      console.log('\n❌ Could not generate configuration from analysis');
    }
  } else {
    console.log('❌ Failed to analyze XML structure');
  }
}

testAnalyzer().catch(console.error);
