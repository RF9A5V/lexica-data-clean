/**
 * Dynamic XML Structure Analyzer
 * Automatically detects XML hierarchy from legislative documents
 */

import fs from 'fs/promises';
import { XMLParser } from 'fast-xml-parser';

/**
 * Analyze XML structure and auto-detect hierarchy
 */
export async function analyzeXmlStructure(xmlFilePath) {
  try {
    const xmlContent = await fs.readFile(xmlFilePath, 'utf-8');

    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '@_',
      allowBooleanAttributes: true,
      parseAttributeValue: true,
      trimValues: true
    });

    const parsedXml = parser.parse(xmlContent);
    return detectHierarchy(parsedXml);
  } catch (error) {
    console.error(`Error analyzing XML structure: ${error.message}`);
    return null;
  }
}

/**
 * Detect hierarchy from parsed XML
 */
function detectHierarchy(parsedXml) {
  const analysis = {
    rootElement: null,
    hierarchyLevels: [],
    structure: {},
    sampleData: {}
  };

  // Find root element (skip XML declarations and processing instructions)
  const rootKeys = Object.keys(parsedXml).filter(key =>
    !key.startsWith('?') && key !== 'DOCUMENT' || key === 'DOCUMENT'
  );

  if (rootKeys.includes('DOCUMENT')) {
    analysis.rootElement = 'DOCUMENT';
    const rootElement = parsedXml[analysis.rootElement];

    // Analyze the structure
    analyzeElement(rootElement, analysis, '', 0);

    // Extract hierarchy based on level-depth and style-name
    extractHierarchyLevels(analysis, parsedXml);
  } else if (rootKeys.length === 1) {
    analysis.rootElement = rootKeys[0];
    const rootElement = parsedXml[analysis.rootElement];

    // Analyze the structure
    analyzeElement(rootElement, analysis, '', 0);

    // Extract hierarchy based on level-depth and style-name
    extractHierarchyLevels(analysis, parsedXml);
  }

  return analysis;
}

/**
 * Recursively analyze XML element structure
 */
function analyzeElement(element, analysis, path, depth) {
  if (!element || typeof element !== 'object' || depth > 10) return;

  // Track structure
  if (!analysis.structure[path]) {
    analysis.structure[path] = {
      count: 0,
      attributes: new Set(),
      children: new Set(),
      samples: []
    };
  }

  analysis.structure[path].count++;

  // Collect attributes
  Object.keys(element).forEach(key => {
    if (key.startsWith('@_')) {
      analysis.structure[path].attributes.add(key);
    } else if (typeof element[key] === 'object') {
      analysis.structure[path].children.add(key);
    }
  });

  // Sample data collection (limited to avoid memory issues)
  if (analysis.structure[path].samples.length < 3) {
    const sample = {};
    Object.keys(element).forEach(key => {
      if (key.startsWith('@_')) {
        sample[key] = element[key];
      } else if (typeof element[key] === 'string' && element[key].length < 200) {
        sample[key] = element[key];
      }
    });
    analysis.structure[path].samples.push(sample);
  }

  // Recursively analyze children
  Object.keys(element).forEach(key => {
    if (!key.startsWith('@_') && typeof element[key] === 'object') {
      const childPath = path ? `${path}.${key}` : key;
      if (Array.isArray(element[key])) {
        element[key].forEach((item, index) => {
          if (index < 5) { // Limit array processing
            analyzeElement(item, analysis, childPath, depth + 1);
          }
        });
      } else {
        analyzeElement(element[key], analysis, childPath, depth + 1);
      }
    }
  });
}

/**
 * Extract hierarchy levels from analyzed structure
 */
function extractHierarchyLevels(analysis, parsedXml) {
  const levels = new Map();

  // Look for LEVEL elements with level-depth and style-name attributes
  const levelElements = [];

  // Function to find all LEVEL elements recursively
  function findLevelElements(obj, path = '') {
    if (!obj || typeof obj !== 'object') return;

    // Process LEVEL elements at current level
    if (obj.LEVEL) {
      const levels = Array.isArray(obj.LEVEL) ? obj.LEVEL : [obj.LEVEL];
      for (const level of levels) {
        if (level && typeof level === 'object' && level['@_level-depth'] && level['@_style-name']) {
          levelElements.push({
            depth: parseInt(level['@_level-depth']),
            styleName: level['@_style-name'],
            path: path ? `${path}.LEVEL` : 'LEVEL',
            element: level
          });
        }
        // Always recurse into LEVEL elements to find nested ones
        if (level && typeof level === 'object') {
          findLevelElements(level, path ? `${path}.LEVEL` : 'LEVEL');
        }
      }
    }

    // Recursively check all other object properties
    for (const [key, value] of Object.entries(obj)) {
      if (key !== 'LEVEL' && typeof value === 'object') {
        if (Array.isArray(value)) {
          value.forEach((item, index) => {
            if (typeof item === 'object') {
              findLevelElements(item, path ? `${path}.${key}[${index}]` : `${key}[${index}]`);
            }
          });
        } else {
          findLevelElements(value, path ? `${path}.${key}` : key);
        }
      }
    }
  }

  findLevelElements(parsedXml);

  // Group by depth and create hierarchy levels
  const levelsByDepth = {};
  for (const level of levelElements) {
    if (!levelsByDepth[level.depth]) {
      levelsByDepth[level.depth] = [];
    }
    levelsByDepth[level.depth].push(level);
  }

  // Create hierarchy from detected levels - include ALL depth levels
  const sortedDepths = Object.keys(levelsByDepth).map(d => parseInt(d)).sort((a, b) => a - b);
  for (const depth of sortedDepths) {
    const depthLevels = levelsByDepth[depth];
    if (depthLevels.length > 0) {
      // Group by style name to handle multiple styles at same depth
      const styleGroups = {};
      for (const level of depthLevels) {
        if (!styleGroups[level.styleName]) {
          styleGroups[level.styleName] = [];
        }
        styleGroups[level.styleName].push(level);
      }
      
      // Create hierarchy entry for each style at this depth
      for (const [styleName, styleGroup] of Object.entries(styleGroups)) {
        const firstLevel = styleGroup[0];
        const levelKey = `${styleName.toLowerCase().replace(/\s+/g, '_')}_${depth}`;

        // Detect available fields by examining sample elements
        const fields = detectFieldsFromSample(firstLevel.element);

        levels.set(levelKey, {
          depth: depth,
          styleName: styleName,
          path: firstLevel.path,
          fields: fields
        });
      }
    }
  }

  // Sort levels by depth and convert to array
  analysis.hierarchyLevels = Array.from(levels.values())
    .sort((a, b) => a.depth - b.depth)
    .map(level => ({
      level: level.styleName.toLowerCase().replace(/\s+/g, '_'),
      xpath: 'LEVEL',
      fields: level.fields,
      level_filter: `@_level-depth=${level.depth}`,
      style_filter: `@_style-name="${level.styleName}"`
    }));
}

/**
 * Detect available fields from a sample element
 */
function detectFieldsFromSample(element) {
  const fields = [];
  
  // Check for common field patterns
  if (element.RECORD) {
    if (element.RECORD.HEADING) fields.push('RECORD.HEADING');
    if (element.RECORD.PARA) fields.push('RECORD.PARA');
    if (element.RECORD.TEXT) fields.push('RECORD.TEXT');
  }
  
  // Check direct properties
  if (element.HEADING) fields.push('HEADING');
  if (element.TEXT) fields.push('TEXT');
  if (element.PARA) fields.push('PARA');
  
  // Check attributes
  if (element['@_title']) fields.push('@_title');
  if (element['@_number']) fields.push('@_number');
  if (element['@_id']) fields.push('@_id');
  
  // For elements without RECORD, still try to extract basic content
  if (fields.length === 0) {
    // Look for any text content or numbered elements
    if (element['@_style-name']) {
      fields.push('@_style-name'); // Use style name as identifier
    }
    fields.push('HEADING', 'PARA'); // Always try these as fallback
  }
  
  return fields.length > 0 ? fields : ['HEADING', 'PARA']; // fallback
}

/**
 * Debug function to print analysis results
 */
export function printAnalysis(analysis) {
  console.log('\n=== XML Structure Analysis ===');
  console.log(`Root Element: ${analysis.rootElement}`);
  console.log(`\nDetected Hierarchy Levels:`);

  analysis.hierarchyLevels.forEach((level, index) => {
    console.log(`  ${index + 1}. ${level.level} (depth: ${level.level_filter})`);
    console.log(`     XPath: ${level.xpath}`);
    console.log(`     Fields: ${level.fields.join(', ')}`);
    if (level.style_filter) {
      console.log(`     Style: ${level.style_filter}`);
    }
  });

  console.log(`\nStructure Overview:`);
  Object.keys(analysis.structure).forEach(path => {
    const struct = analysis.structure[path];
    console.log(`  ${path} (${struct.count} occurrences)`);
    if (struct.attributes.size > 0) {
      console.log(`    Attributes: ${Array.from(struct.attributes).join(', ')}`);
    }
    if (struct.children.size > 0) {
      console.log(`    Children: ${Array.from(struct.children).join(', ')}`);
    }
    
    // Show sample data for debugging
    if (struct.samples.length > 0) {
      console.log(`    Sample:`, JSON.stringify(struct.samples[0], null, 2).substring(0, 200) + '...');
    }
  });
}

/**
 * Export structure analysis to JSON for debugging
 */
export function exportAnalysisToJson(analysis, outputPath) {
  const exportData = {
    rootElement: analysis.rootElement,
    hierarchyLevels: analysis.hierarchyLevels,
    structure: {}
  };
  
  // Convert Sets to Arrays for JSON serialization
  Object.keys(analysis.structure).forEach(path => {
    const struct = analysis.structure[path];
    exportData.structure[path] = {
      count: struct.count,
      attributes: Array.from(struct.attributes),
      children: Array.from(struct.children),
      samples: struct.samples
    };
  });
  
  return JSON.stringify(exportData, null, 2);
}

/**
 * Generate parsing configuration from analysis with enhanced field detection
 */
export function generateParsingConfig(analysis, sourceId) {
  if (!analysis.rootElement || analysis.hierarchyLevels.length === 0) {
    return null;
  }

  // Detect parsing strategy based on structure patterns
  const parsingStrategy = detectParsingStrategy(analysis);

  // Enhanced hierarchy with better field mapping based on trie analysis
  const enhancedHierarchy = analysis.hierarchyLevels.map(level => {
    const config = {
      level: level.level,
      xpath: level.xpath,
      level_filter: level.level_filter,
      style_filter: level.style_filter,
      fields: []
    };

    // Map detected fields to extraction paths
    level.fields.forEach(field => {
      if (field === 'RECORD.HEADING') {
        config.fields.push({ path: 'RECORD.HEADING', target: 'heading' });
        config.fields.push({ path: 'RECORD.@_number', target: 'number' });
      } else if (field === 'RECORD.PARA') {
        config.fields.push({ path: 'RECORD.PARA', target: 'text', extract_text: true });
      } else if (field.startsWith('@_')) {
        config.fields.push({ path: field, target: field.replace('@_', '') });
      } else {
        config.fields.push({ path: field, target: field.toLowerCase() });
      }
    });

    return config;
  });

  return {
    xml_structure: {
      root_element: analysis.rootElement,
      hierarchy: enhancedHierarchy
    },
    parsing_strategy: parsingStrategy,
    parsing_hints: {
      text_extraction_paths: ['RECORD.PARA', 'PARA', '#text'],
      heading_paths: ['RECORD.HEADING', 'HEADING'],
      number_paths: ['RECORD.@_number', '@_number', 'number'],
      citation_patterns: extractCitationPatterns(analysis)
    }
  };
}

/**
 * Detect parsing strategy based on XML structure patterns
 */
function detectParsingStrategy(analysis) {
  const strategy = {
    type: 'generic',
    container_element: null,
    content_element: null,
    type_attribute: null,
    depth_attribute: null,
    traversal: 'breadth_first'
  };

  // Check for ALP pattern (LEVEL with style-name and RECORD)
  const hasLevelRecord = Object.keys(analysis.structure).some(path => 
    path.includes('LEVEL') && path.includes('RECORD')
  );
  
  if (hasLevelRecord) {
    strategy.type = 'level_record';
    strategy.container_element = 'LEVEL';
    strategy.content_element = 'RECORD';
    strategy.type_attribute = '@_style-name';
    strategy.depth_attribute = '@_level-depth';
    strategy.traversal = 'depth_first';
    return strategy;
  }

  // Check for nested section pattern (common in USC, CFR)
  const hasNestedSections = Object.keys(analysis.structure).some(path => 
    path.toLowerCase().includes('section') || path.toLowerCase().includes('part')
  );
  
  if (hasNestedSections) {
    strategy.type = 'nested_sections';
    strategy.traversal = 'depth_first';
    return strategy;
  }

  // Check for flat structure with numbered elements
  const hasNumberedElements = Object.keys(analysis.structure).some(path => {
    const struct = analysis.structure[path];
    return Array.from(struct.attributes).some(attr => 
      attr.includes('number') || attr.includes('id')
    );
  });

  if (hasNumberedElements) {
    strategy.type = 'numbered_flat';
    strategy.traversal = 'breadth_first';
    return strategy;
  }

  return strategy;
}

/**
 * Extract citation patterns from structure analysis
 */
function extractCitationPatterns(analysis) {
  const patterns = [];
  
  // Look for DESTINATION elements which indicate cross-references
  Object.keys(analysis.structure).forEach(path => {
    if (path.includes('DESTINATION')) {
      patterns.push({
        type: 'internal_reference',
        xpath: path,
        id_attribute: '@_id',
        name_attribute: '@_name'
      });
    }
    
    if (path.includes('LINK')) {
      patterns.push({
        type: 'external_link',
        xpath: path,
        destination_attribute: '@_destination-name'
      });
    }
  });
  
  return patterns;
}

/**
 * Generate configuration from analysis (legacy compatibility)
 */
export function generateConfigFromAnalysis(analysis, sourceId) {
  const config = generateParsingConfig(analysis, sourceId);
  return config ? { xml_structure: config.xml_structure } : null;
}
