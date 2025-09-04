/**
 * XML statute parser
 * Parses XML legislative documents and converts to NDJSON format with hierarchical relationships
 */

import fs from 'fs/promises';
import path from 'path';
import { XMLParser } from 'fast-xml-parser';
import { EXTRACTION_CONFIG } from './config.js';
import { NdjsonWriter } from './ndjson_writer.js';
import { extractCitationsFromText } from './citation_extractor.js';
import { analyzeXmlStructure, generateParsingConfig } from './xml_structure_analyzer.js';
import { parseAlpXml } from './alp_parser.js';
import { parseAdaptiveXml } from './adaptive_parser.js';

/**
 * Parse XML files to NDJSON format
 */
export async function parseXmlToNdjson(source, stagingDir, ndjsonOutput, options = {}) {
  const { verbose = false, dryRun = false } = options;
  const results = { processedFiles: 0, ndjsonLines: 0, citationsFound: 0 };

  if (dryRun) {
    console.log(`  Would parse XML files from ${stagingDir} to ${ndjsonOutput}`);
    return results;
  }

  // Get all XML files from staging directory
  const xmlFiles = await getXmlFiles(stagingDir);
  if (verbose) {
    console.log(`  Found ${xmlFiles.length} XML files to process`);
  }

  // Auto-detect XML structure if not provided
  let config;
  if (!source.xml_structure) {
    if (verbose) {
      console.log(`  Auto-detecting XML structure from ${xmlFiles[0]}...`);
    }
    const analysis = await analyzeXmlStructure(xmlFiles[0]);
    config = generateParsingConfig(analysis, source.id);
    
    if (config && verbose) {
      console.log(`  Detected ${config.xml_structure.hierarchy.length} hierarchy levels`);
      console.log(`  Found ${config.parsing_hints.citation_patterns.length} citation patterns`);
    }
  } else {
    config = {
      xml_structure: source.xml_structure,
      parsing_hints: source.parsing_hints || {}
    };
  }

  if (!config) {
    console.warn(`  Failed to auto-detect XML structure, using fallback`);
    config = {
      xml_structure: {
        root_element: 'DOCUMENT',
        hierarchy: [{
          level: 'section',
          xpath: 'LEVEL',
          fields: [{ path: 'HEADING', target: 'heading' }],
          level_filter: '@_level-depth>=1'
        }]
      },
      parsing_hints: {},
      parsing_strategy: { type: 'generic', traversal: 'breadth_first' }
    };
  }

  // Initialize XML parser with enhanced options
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    allowBooleanAttributes: true,
    parseAttributeValue: true,
    trimValues: true,
    parseTagValue: true,
    ...EXTRACTION_CONFIG.xmlParserOptions
  });

  // Create NDJSON writer
  const writer = new NdjsonWriter(ndjsonOutput);
  await writer.open();

  try {
    for (const xmlFile of xmlFiles) {
      if (verbose) {
        console.log(`  Processing: ${path.basename(xmlFile)}`);
      }

      // Parse XML file
      const xmlContent = await fs.readFile(xmlFile, 'utf-8');
      const parsedXml = parser.parse(xmlContent);

      // Use adaptive parser with auto-detected configuration
      if (verbose) {
        console.log(`    Using adaptive parser (${config.parsing_strategy.type}) for ${path.basename(xmlFile)}`);
      }
      const extractionResults = await parseAdaptiveXml(parsedXml, source, writer, config, options);
      
      results.ndjsonLines += extractionResults.unitsExtracted;
      results.citationsFound += extractionResults.citationsFound;
      results.processedFiles++;
    }
  } finally {
    await writer.close();
  }

  return results;
}

/**
 * Extract hierarchical units from parsed XML
 */
async function extractHierarchicalUnits(parsedXml, source, writer, xmlStructure, parsingHints = {}, options = {}) {
  const { verbose = false } = options;
  const results = { unitsExtracted: 0, citationsFound: 0 };

  // Get root element and hierarchy from provided structure
  const rootElement = xmlStructure?.root_element || 'DOCUMENT';
  const hierarchy = xmlStructure?.hierarchy || [];

  // Track parent-child relationships
  const unitStack = [];
  const processedIds = new Set();

  // Recursive function to traverse and extract units
  async function processElement(element, currentPath = [], parentId = null, levelIndex = 0) {
    if (!element || typeof element !== 'object') return;

    // Try to match this element against all hierarchy levels
    for (let i = 0; i < hierarchy.length; i++) {
      const hierarchyLevel = hierarchy[i];
      const levelName = hierarchyLevel.level;
      const xpath = hierarchyLevel.xpath;
      const fields = hierarchyLevel.fields || [];
      const levelFilter = hierarchyLevel.level_filter;

      // Check if element matches xpath and level filter
      if (element[xpath]) {
        const candidates = Array.isArray(element[xpath]) ? element[xpath] : [element[xpath]];
        
        for (const unitData of candidates) {
          // Apply level filter if specified
          if (levelFilter && !matchesFilter(unitData, levelFilter)) {
            continue;
          }

          const unit = await extractUnitData(unitData, levelName, source, parentId, fields, parsingHints);

          if (unit && !processedIds.has(unit.id)) {
            // Write unit to NDJSON
            await writer.write(unit);
            processedIds.add(unit.id);
            results.unitsExtracted++;
            unitStack.push(unit);

            // Extract citations from unit text if present
            if (unit.text && unit.text.trim()) {
              const citations = extractCitationsFromText(unit.text, source.id, unit.id);
              for (const citation of citations) {
                const citationRecord = {
                  type: 'citation',
                  source_unit_id: unit.id,
                  raw_citation: citation.rawText,
                  target_kind: citation.targetKind,
                  external_curie: citation.curie,
                  context_snippet: citation.context,
                  source_id: source.id,
                  created_at: new Date().toISOString()
                };
                await writer.write(citationRecord);
                results.citationsFound++;
              }
            }

            // Extract internal references using parsing hints
            if (parsingHints.citation_patterns) {
              const internalRefs = extractInternalReferences(unitData, parsingHints.citation_patterns);
              for (const ref of internalRefs) {
                const refRecord = {
                  type: 'internal_reference',
                  source_unit_id: unit.id,
                  target_id: ref.targetId,
                  target_name: ref.targetName,
                  reference_type: ref.type,
                  source_id: source.id,
                  created_at: new Date().toISOString()
                };
                await writer.write(refRecord);
              }
            }

            // Process child elements recursively
            await processChildElements(unitData, currentPath, unit.id);
            unitStack.pop();
          }
        }
      }
    }

    // Also recursively process all child elements
    await processChildElements(element, currentPath, parentId);
  }

  // Helper function to process child elements
  async function processChildElements(element, currentPath, parentId) {
    if (!element || typeof element !== 'object') return;
    
    for (const [key, value] of Object.entries(element)) {
      if (!key.startsWith('@_') && typeof value === 'object') {
        if (Array.isArray(value)) {
          for (const item of value) {
            await processElement(item, [...currentPath, key], parentId, 0);
          }
        } else {
          await processElement(value, [...currentPath, key], parentId, 0);
        }
      }
    }
  }

  // Helper function to check if element matches filter
  function matchesFilter(element, filter) {
    if (!filter) return true;
    
    // Parse simple filters like "@_level-depth=1" or "@_level-depth>=1"
    const match = filter.match(/^(@_[\w-]+)\s*([>=<]+)\s*(\d+)$/);
    if (match) {
      const [, attr, op, value] = match;
      const elementValue = element[attr];
      const targetValue = parseInt(value);
      
      if (elementValue === undefined) return false;
      
      switch (op) {
        case '=':
        case '==':
          return parseInt(elementValue) === targetValue;
        case '>=':
          return parseInt(elementValue) >= targetValue;
        case '<=':
          return parseInt(elementValue) <= targetValue;
        case '>':
          return parseInt(elementValue) > targetValue;
        case '<':
          return parseInt(elementValue) < targetValue;
        default:
          return false;
      }
    }
    
    return true;
  }

  // Start extraction from root
  await processElement(parsedXml);
  return results;
}

/**
 * Extract unit data from XML element with enhanced field mapping
 */
async function extractUnitData(unitData, levelName, source, parentId, fields, parsingHints = {}) {
  // Generate unique unit ID
  const unitId = await generateUnitId(source.id, levelName, unitData);
  if (!unitId) return null;

  const unit = {
    id: unitId,
    type: levelName,
    parent_id: parentId,
    source_id: source.id,
    citation: generateCitation(source, unitData, levelName),
    sort_key: generateSortKey(unitData, levelName),
    created_at: new Date().toISOString()
  };

  // Handle enhanced field configuration from analyzer
  if (fields && fields.length > 0 && typeof fields[0] === 'object' && fields[0].path) {
    // New enhanced field format from analyzer
    for (const fieldConfig of fields) {
      const value = getNestedValue(unitData, fieldConfig.path);
      if (value !== undefined && value !== null) {
        if (fieldConfig.extract_text) {
          unit[fieldConfig.target] = extractTextContent(value);
        } else {
          unit[fieldConfig.target] = value;
        }
      }
    }
  } else {
    // Legacy field format - maintain backward compatibility
    for (const field of fields || []) {
      const value = getNestedValue(unitData, field);
      if (value !== undefined && value !== null) {
        const fieldName = field.includes('.') ? field.split('.').pop().toLowerCase() : field.replace('@_', '');
        unit[fieldName] = value;
      }
    }
  }

  // Use parsing hints for intelligent text extraction
  if (!unit.text && parsingHints.text_extraction_paths) {
    for (const textPath of parsingHints.text_extraction_paths) {
      const textValue = getNestedValue(unitData, textPath);
      if (textValue) {
        unit.text = extractTextContent(textValue);
        break;
      }
    }
  }

  // Use parsing hints for heading extraction if not already set
  if (!unit.heading && parsingHints.heading_paths) {
    for (const headingPath of parsingHints.heading_paths) {
      const headingValue = getNestedValue(unitData, headingPath);
      if (headingValue) {
        unit.heading = typeof headingValue === 'string' ? headingValue : extractTextContent(headingValue);
        break;
      }
    }
  }

  // Use parsing hints for number extraction if not already set
  if (!unit.number && parsingHints.number_paths) {
    for (const numberPath of parsingHints.number_paths) {
      const numberValue = getNestedValue(unitData, numberPath);
      if (numberValue) {
        unit.number = numberValue;
        break;
      }
    }
  }

  return unit;
}

/**
 * Get value from nested object path (e.g., 'RECORD.HEADING')
 */
function getNestedValue(obj, path) {
  if (!obj || !path) return undefined;
  
  // Handle attribute paths
  if (path.startsWith('@_')) {
    return obj[path];
  }
  
  // Handle nested paths
  if (path.includes('.')) {
    const parts = path.split('.');
    let current = obj;
    for (const part of parts) {
      if (current && typeof current === 'object' && current[part] !== undefined) {
        current = current[part];
      } else {
        return undefined;
      }
    }
    return current;
  }
  
  // Handle direct paths
  return obj[path];
}

/**
 * Extract all text content from an element recursively
 */
function extractTextContent(element) {
  if (!element) return '';
  
  const textParts = [];
  
  // Helper function to recursively collect text
  function collectText(obj) {
    if (typeof obj === 'string') {
      textParts.push(obj.trim());
    } else if (typeof obj === 'object' && obj !== null) {
      // Skip attributes (keys starting with @_)
      for (const [key, value] of Object.entries(obj)) {
        if (!key.startsWith('@_')) {
          if (Array.isArray(value)) {
            value.forEach(collectText);
          } else {
            collectText(value);
          }
        }
      }
    }
  }
  
  collectText(element);
  return textParts.filter(text => text.length > 0).join(' ');
}

/**
 * Extract internal references using parsing hints
 */
function extractInternalReferences(unitData, citationPatterns) {
  const references = [];
  
  function searchForReferences(obj, path = '') {
    if (!obj || typeof obj !== 'object') return;
    
    for (const pattern of citationPatterns) {
      if (path.endsWith(pattern.xpath.split('.').pop())) {
        if (pattern.type === 'internal_reference') {
          const targetId = obj[pattern.id_attribute];
          const targetName = obj[pattern.name_attribute];
          if (targetId || targetName) {
            references.push({
              type: pattern.type,
              targetId: targetId,
              targetName: targetName
            });
          }
        }
      }
    }
    
    // Recursively search children
    for (const [key, value] of Object.entries(obj)) {
      if (!key.startsWith('@_') && typeof value === 'object') {
        const newPath = path ? `${path}.${key}` : key;
        if (Array.isArray(value)) {
          value.forEach(item => searchForReferences(item, newPath));
        } else {
          searchForReferences(value, newPath);
        }
      }
    }
  }
  
  searchForReferences(unitData);
  return references;
}

/**
 * Generate unique unit ID
 */
async function generateUnitId(sourceId, levelName, unitData) {
  const parts = [];

  // Add source identifier
  parts.push(sourceId);

  // Add level type
  parts.push(levelName);

  // Extract meaningful identifiers
  if (unitData.number || unitData['@_number']) {
    parts.push(unitData.number || unitData['@_number']);
  } else if (unitData.title || unitData['@_title']) {
    // Create slug from title if no number
    const title = (unitData.title || unitData['@_title']).toString();
    const slug = title.toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .substring(0, 20);
    if (slug) parts.push(slug);
  } else if (unitData.id || unitData['@_id']) {
    parts.push(unitData.id || unitData['@_id']);
  } else {
    // Fallback to hash of content
    const crypto = await import('crypto');
    const content = JSON.stringify(unitData);
    const hash = crypto.createHash('md5').update(content).digest('hex').substring(0, 8);
    parts.push(hash);
  }

  return parts.join('-');
}

/**
 * Generate human-readable citation
 */
function generateCitation(source, unitData, levelName) {
  const number = unitData.number || unitData['@_number'];
  const title = unitData.title || unitData['@_title'];

  // Format based on source type and level
  switch (source.instrument_kind) {
    case 'regulatory_code':
      if (source.code_key === 'rcny') {
        return `${number} RCNY ${title || ''}`.trim();
      } else if (source.code_key === 'nycrr') {
        return `${number} NYCRR ${title || ''}`.trim();
      }
      break;
    case 'statute_code':
      return `${source.code_key.toUpperCase()} ${number} ${title || ''}`.trim();
  }

  return `${number} ${title || ''}`.trim();
}

/**
 * Generate sort key for proper ordering
 */
function generateSortKey(unitData, levelName) {
  const number = unitData.number || unitData['@_number'] || '0';

  // Extract numeric parts and pad for consistent sorting
  const numericMatch = number.toString().match(/(\d+)/);
  if (numericMatch) {
    const num = parseInt(numericMatch[1], 10);
    return num.toString().padStart(6, '0');
  }

  return number.toString().padStart(6, '0');
}

/**
 * Get all XML files from directory recursively
 */
async function getXmlFiles(dir) {
  const files = [];

  async function scan(currentDir) {
    const items = await fs.readdir(currentDir);

    for (const item of items) {
      const itemPath = path.join(currentDir, item);
      const stat = await fs.stat(itemPath);

      if (stat.isDirectory()) {
        await scan(itemPath);
      } else if (item.endsWith('.xml')) {
        files.push(itemPath);
      }
    }
  }

  await scan(dir);
  return files;
}

/**
 * Detect if XML is in American Legal Publishing (ALP) format
 */
function detectAlpFormat(parsedXml) {
  // Check for ALP-specific structure indicators
  if (parsedXml.DOCUMENT) {
    const doc = parsedXml.DOCUMENT;
    
    // Look for ALP-specific attributes
    if (doc['@_source-infobase-name'] || doc['@_content-collection-id']) {
      return true;
    }
    
    // Check for LEVEL elements with style-name attributes (ALP pattern)
    if (doc.LEVEL) {
      const levels = Array.isArray(doc.LEVEL) ? doc.LEVEL : [doc.LEVEL];
      for (const level of levels) {
        if (level['@_style-name'] && level['@_level-depth']) {
          return true;
        }
      }
    }
  }
  
  return false;
}

/**
 * Create NDJSON writer stream
 */
function createNdjsonWriter(outputPath) {
  const stream = {
    write: async (data) => {
      const jsonLine = JSON.stringify(data) + '\n';
      await fs.appendFile(outputPath, jsonLine);
    },
    close: async () => {
      // Ensure file exists even if empty
      try {
        await fs.access(outputPath);
      } catch {
        await fs.writeFile(outputPath, '');
      }
    }
  };

  return stream;
}
