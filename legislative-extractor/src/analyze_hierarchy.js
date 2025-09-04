#!/usr/bin/env node

/**
 * XML Hierarchy Analysis Tool
 * 
 * Usage:
 *   npm run analyze-hierarchy -- --config=configs/nyc_admin.json
 *   npm run analyze-hierarchy -- --config=configs/nyc_admin.json --xml-file=data/staging/nyc-admin/0-0-0-1.xml
 *   npm run analyze-hierarchy -- --config=configs/nyc_admin.json --export-json=analysis.json
 */

import { parseArgs } from 'node:util';
import { loadConfig } from './config_file.js';
import { analyzeXmlStructure, printAnalysis, exportAnalysisToJson } from './xml_structure_analyzer.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Parse command line arguments
 */
function parseCliArgs() {
  const args = process.argv.slice(2);
  const options = {
    config: null,
    xmlFile: null,
    exportJson: null,
    verbose: false,
    help: false
  };

  for (const arg of args) {
    if (arg.startsWith('--config=')) {
      options.config = arg.split('=')[1];
    } else if (arg.startsWith('--xml-file=')) {
      options.xmlFile = arg.split('=')[1];
    } else if (arg.startsWith('--export-json=')) {
      options.exportJson = arg.split('=')[1];
    } else if (arg === '--verbose' || arg === '-v') {
      options.verbose = true;
    } else if (arg === '--help' || arg === '-h') {
      options.help = true;
    }
  }

  return options;
}

/**
 * Display help information
 */
function showHelp() {
  console.log(`
XML Hierarchy Analysis Tool

Usage:
  npm run analyze-hierarchy -- [options]

Options:
  --config=<path>         Configuration file path (required)
  --xml-file=<path>       Specific XML file to analyze (optional)
  --export-json=<path>    Export analysis to JSON file (optional)
  --verbose, -v           Verbose output
  --help, -h              Show this help message

Examples:
  npm run analyze-hierarchy -- --config=configs/nyc_admin.json
  npm run analyze-hierarchy -- --config=configs/nyc_admin.json --xml-file=data/staging/nyc-admin/sample.xml
  npm run analyze-hierarchy -- --config=configs/nyc_admin.json --export-json=hierarchy-analysis.json
`);
}

/**
 * Find XML files in staging directory
 */
async function findXmlFiles(stagingDir, maxFiles = 5) {
  const files = [];
  
  try {
    const items = await fs.readdir(stagingDir);
    
    for (const item of items) {
      if (item.endsWith('.xml') && files.length < maxFiles) {
        files.push(path.join(stagingDir, item));
      }
    }
  } catch (error) {
    console.warn(`Could not read staging directory ${stagingDir}: ${error.message}`);
  }
  
  return files;
}

/**
 * Analyze XML hierarchy with trie-based unique element tracking
 */
class HierarchyTrie {
  constructor() {
    this.root = new Map();
    this.uniquePaths = new Set();
    this.pathCounts = new Map();
  }

  /**
   * Add a hierarchy path to the trie
   */
  addPath(pathElements) {
    let current = this.root;
    const fullPath = pathElements.join('.');
    
    // Track unique paths
    this.uniquePaths.add(fullPath);
    this.pathCounts.set(fullPath, (this.pathCounts.get(fullPath) || 0) + 1);
    
    // Build trie structure
    for (const element of pathElements) {
      if (!current.has(element)) {
        current.set(element, {
          children: new Map(),
          count: 0,
          samples: []
        });
      }
      current.get(element).count++;
      current = current.get(element).children;
    }
  }

  /**
   * Get all unique paths with their counts
   */
  getUniquePathsWithCounts() {
    return Array.from(this.pathCounts.entries())
      .map(([path, count]) => ({ path, count }))
      .sort((a, b) => b.count - a.count);
  }

  /**
   * Print trie structure
   */
  printTrie(node = this.root, depth = 0, prefix = '') {
    const indent = '  '.repeat(depth);
    
    for (const [key, value] of node.entries()) {
      console.log(`${indent}${key} (${value.count} occurrences)`);
      if (value.children.size > 0) {
        this.printTrie(value.children, depth + 1, prefix + key + '.');
      }
    }
  }
}

/**
 * Main analysis function
 */
async function main() {
  try {
    const options = parseCliArgs();

    if (options.help) {
      showHelp();
      return;
    }

    if (!options.config) {
      console.error('❌ Error: --config=<path> is required');
      showHelp();
      process.exit(1);
    }

    console.log(`📋 Loading configuration from: ${options.config}`);
    const config = await loadConfig(options.config);

    if (!config.sources || config.sources.length === 0) {
      console.error('❌ Error: No sources found in configuration');
      process.exit(1);
    }

    const source = config.sources[0]; // Use first source
    console.log(`🔍 Analyzing source: ${source.label} (${source.id})`);

    // Determine XML file(s) to analyze
    let xmlFiles = [];
    if (options.xmlFile) {
      xmlFiles = [options.xmlFile];
    } else {
      const stagingDir = source.staging_dir || `./data/staging/${source.id}`;
      xmlFiles = await findXmlFiles(stagingDir, 3); // Analyze up to 3 files
    }

    if (xmlFiles.length === 0) {
      console.error('❌ Error: No XML files found to analyze');
      console.log('💡 Try running the extract step first: npm run extract');
      process.exit(1);
    }

    console.log(`📄 Found ${xmlFiles.length} XML file(s) to analyze`);

    // Initialize trie for unique element tracking
    const hierarchyTrie = new HierarchyTrie();
    const allAnalyses = [];

    // Analyze each XML file
    for (const xmlFile of xmlFiles) {
      console.log(`\n🔬 Analyzing: ${path.basename(xmlFile)}`);
      
      try {
        const analysis = await analyzeXmlStructure(xmlFile);
        if (analysis) {
          allAnalyses.push(analysis);
          
          // Add paths to trie
          Object.keys(analysis.structure).forEach(structPath => {
            if (structPath) {
              const pathElements = structPath.split('.');
              hierarchyTrie.addPath(pathElements);
            }
          });
          
          if (options.verbose) {
            printAnalysis(analysis);
          }
        }
      } catch (error) {
        console.error(`❌ Error analyzing ${xmlFile}: ${error.message}`);
      }
    }

    // Print consolidated results
    console.log('\n=== CONSOLIDATED HIERARCHY ANALYSIS ===');
    
    if (allAnalyses.length > 0) {
      const firstAnalysis = allAnalyses[0];
      console.log(`Root Element: ${firstAnalysis.rootElement}`);
      
      if (firstAnalysis.hierarchyLevels.length > 0) {
        console.log('\n📊 Detected Hierarchy Levels:');
        firstAnalysis.hierarchyLevels.forEach((level, index) => {
          console.log(`  ${index + 1}. ${level.level}`);
          console.log(`     Depth Filter: ${level.level_filter}`);
          console.log(`     Style Filter: ${level.style_filter || 'N/A'}`);
          console.log(`     Fields: ${level.fields.join(', ')}`);
        });
      }
    }

    // Print unique paths with trie structure
    console.log('\n🌳 XML Structure Trie (Unique Element Paths):');
    hierarchyTrie.printTrie();

    console.log('\n📈 Most Common Element Paths:');
    const uniquePaths = hierarchyTrie.getUniquePathsWithCounts();
    uniquePaths.slice(0, 10).forEach(({ path, count }) => {
      console.log(`  ${path} (${count} occurrences)`);
    });

    // Export to JSON if requested
    if (options.exportJson && allAnalyses.length > 0) {
      const exportData = {
        timestamp: new Date().toISOString(),
        source: source.id,
        xmlFiles: xmlFiles.map(f => path.basename(f)),
        analysis: allAnalyses[0],
        uniquePaths: uniquePaths,
        trieStructure: Object.fromEntries(hierarchyTrie.root)
      };
      
      const jsonOutput = JSON.stringify(exportData, null, 2);
      await fs.writeFile(options.exportJson, jsonOutput);
      console.log(`\n💾 Analysis exported to: ${options.exportJson}`);
    }

    console.log('\n✅ Hierarchy analysis completed successfully!');

  } catch (error) {
    console.error('\n❌ Analysis failed:', error.message);
    
    if (process.env.NODE_ENV === 'development') {
      console.error('\nStack trace:');
      console.error(error.stack);
    }

    process.exit(1);
  }
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
