/**
 * Adaptive XML Parser - Generalizes parsing logic based on structure analysis
 * Replaces format-specific parsers with configurable strategy-based parsing
 */

import { extractCitationsFromText } from './citation_extractor.js';

/**
 * Parse XML using adaptive strategy based on structure analysis
 */
export async function parseAdaptiveXml(parsedXml, source, writer, config, options = {}) {
  const { verbose = false, errorOnly = false } = options;
  const results = { unitsExtracted: 0, citationsFound: 0 };

  if (!config.parsing_strategy) {
    throw new Error('No parsing strategy found in config');
  }

  const strategy = config.parsing_strategy;
  
  if (verbose && !errorOnly) {
    console.log(`Using ${strategy.type} parsing strategy with ${strategy.traversal} traversal`);
  }

  // Route to appropriate parsing strategy
  switch (strategy.type) {
    case 'level_record':
      return await parseLevelRecordStrategy(parsedXml, source, writer, config, options);
    case 'nested_sections':
      return await parseNestedSectionsStrategy(parsedXml, source, writer, config, options);
    case 'numbered_flat':
      return await parseNumberedFlatStrategy(parsedXml, source, writer, config, options);
    default:
      return await parseGenericStrategy(parsedXml, source, writer, config, options);
  }
}

/**
 * Parse using LEVEL/RECORD strategy (ALP format)
 */
async function parseLevelRecordStrategy(parsedXml, source, writer, config, options) {
  const { verbose = false } = options;
  const results = { unitsExtracted: 0, citationsFound: 0 };
  const strategy = config.parsing_strategy;

  // Find root document
  const rootElement = parsedXml[config.xml_structure.root_element];
  if (!rootElement) {
    return results;
  }

  // Start traversal from top-level container elements
  const containerElements = findElements(rootElement, strategy.container_element);
  
  for (const container of containerElements) {
    const traversalResults = await traverseLevelRecord(
      container, source, writer, config, null, 0, options
    );
    results.unitsExtracted += traversalResults.unitsExtracted;
    results.citationsFound += traversalResults.citationsFound;
  }

  return results;
}

/**
 * Recursive traversal for LEVEL/RECORD structure
 */
async function traverseLevelRecord(element, source, writer, config, parentId, depth, options) {
  const { verbose = false, errorOnly = false } = options;
  const results = { unitsExtracted: 0, citationsFound: 0 };
  const strategy = config.parsing_strategy;

  // Extract type from container element
  const type = normalizeType(element[strategy.type_attribute] || 'unknown');
  
  // Only show processing logs for depth 0-2 to reduce noise (unless error-only mode)
  if (verbose && !errorOnly && depth <= 2) {
    console.log(`${'  '.repeat(depth)}Processing ${type} (depth ${depth})`);
  }

  // Process content elements at this level first
  const contentElements = findElements(element, strategy.content_element);
  
  for (const content of contentElements) {
    try {
      const unit = await extractUnitFromContent(content, type, source, parentId, config);
      if (unit) {
        await writer.write(unit);
        results.unitsExtracted++;

        // Extract citations with enhanced logging
        if (unit.text) {
          const citations = extractCitationsFromText(unit.text, source.id, unit.id);
          for (const citation of citations) {
            // All citations from extractCitationsFromText should be valid now
            await writer.write(citation);
            results.citationsFound++;
          }
        }
      }
    } catch (error) {
      console.log(`❌ Error processing ${type} content:`, {
        error: error.message,
        contentKeys: Object.keys(content || {}),
        elementType: type
      });
    }
  }

  // Then recursively process nested container elements
  const nestedContainers = findElements(element, strategy.container_element);
  for (const nested of nestedContainers) {
    const nestedResults = await traverseLevelRecord(
      nested, source, writer, config, parentId, depth + 1, options
    );
    results.unitsExtracted += nestedResults.unitsExtracted;
    results.citationsFound += nestedResults.citationsFound;
  }

  return results;
}

/**
 * Parse using nested sections strategy (USC, CFR format)
 */
async function parseNestedSectionsStrategy(parsedXml, source, writer, config, options) {
  const results = { unitsExtracted: 0, citationsFound: 0 };
  
  // Use hierarchy levels from config
  for (const level of config.xml_structure.hierarchy) {
    const elements = findElementsByXPath(parsedXml, level.xpath);
    
    for (const element of elements) {
      const unit = await extractUnitFromHierarchy(element, level, source, null, config);
      if (unit) {
        await writer.write(unit);
        results.unitsExtracted++;

        // Extract citations
        if (unit.text) {
          const citations = extractCitationsFromText(unit.text, source.id, unit.id);
          for (const citation of citations) {
            await writer.write(citation);
            results.citationsFound++;
          }
        }
      }
    }
  }

  return results;
}

/**
 * Parse using numbered flat strategy
 */
async function parseNumberedFlatStrategy(parsedXml, source, writer, config, options) {
  const results = { unitsExtracted: 0, citationsFound: 0 };
  
  // Process each hierarchy level sequentially
  for (const level of config.xml_structure.hierarchy) {
    const elements = findElementsByXPath(parsedXml, level.xpath);
    
    for (const element of elements) {
      const unit = await extractUnitFromHierarchy(element, level, source, null, config);
      if (unit) {
        await writer.write(unit);
        results.unitsExtracted++;
      }
    }
  }

  return results;
}

/**
 * Generic parsing strategy fallback
 */
async function parseGenericStrategy(parsedXml, source, writer, config, options) {
  const results = { unitsExtracted: 0, citationsFound: 0 };
  
  // Use existing hierarchical extraction logic
  for (const level of config.xml_structure.hierarchy) {
    const elements = findElementsByXPath(parsedXml, level.xpath);
    
    for (const element of elements) {
      const unit = await extractUnitFromHierarchy(element, level, source, null, config);
      if (unit) {
        await writer.write(unit);
        results.unitsExtracted++;
      }
    }
  }

  return results;
}

/**
 * Extract unit data from content element (RECORD, etc.)
 */
async function extractUnitFromContent(content, type, source, parentId, config) {
  const unit = {
    id: generateId(source.id),
    source_id: source.id,
    type: type,
    parent_id: parentId,
    version: source.version || '1.0'
  };

  // Extract fields based on parsing hints
  const hints = config.parsing_hints;
  
  // Extract heading
  unit.heading = extractByPaths(content, hints.heading_paths);
  
  // Extract text content
  const textContent = extractByPaths(content, hints.text_extraction_paths);
  if (textContent) {
    unit.text = consolidateTextContent(textContent);
  }
  
  // Extract number/identifier
  unit.number = extractByPaths(content, hints.number_paths);
  
  // Generate citation and sort keys
  unit.citation = generateCitation(unit, source);
  unit.sort_key = generateSortKey(unit);

  return unit.heading || unit.text ? unit : null;
}

/**
 * Extract unit data from hierarchy element
 */
async function extractUnitFromHierarchy(element, levelConfig, source, parentId, config) {
  const unit = {
    id: generateId(source.id),
    source_id: source.id,
    type: levelConfig.level,
    parent_id: parentId,
    version: source.version || '1.0'
  };

  // Extract fields based on level configuration
  for (const fieldConfig of levelConfig.fields) {
    const value = getNestedValue(element, fieldConfig.path);
    if (value !== undefined && value !== null) {
      if (fieldConfig.extract_text) {
        unit[fieldConfig.target] = extractTextContent(value);
      } else {
        unit[fieldConfig.target] = value;
      }
    }
  }

  // Generate citation and sort keys
  unit.citation = generateCitation(unit, source);
  unit.sort_key = generateSortKey(unit);

  return unit.heading || unit.text ? unit : null;
}

/**
 * Helper functions
 */

function findElements(parent, elementName) {
  if (!parent || !elementName) return [];
  
  const element = parent[elementName];
  if (!element) return [];
  
  return Array.isArray(element) ? element : [element];
}

function findElementsByXPath(root, xpath) {
  // Simplified XPath evaluation - would need full implementation for complex paths
  const parts = xpath.split('/').filter(part => part && part !== '.');
  let current = [root];
  
  for (const part of parts) {
    const next = [];
    for (const node of current) {
      if (node && node[part]) {
        const found = Array.isArray(node[part]) ? node[part] : [node[part]];
        next.push(...found);
      }
    }
    current = next;
  }
  
  return current;
}

function extractByPaths(element, paths) {
  for (const path of paths) {
    const value = getNestedValue(element, path);
    if (value) return value;
  }
  return null;
}

function getNestedValue(obj, path) {
  return path.split('.').reduce((current, key) => {
    return current && current[key] !== undefined ? current[key] : null;
  }, obj);
}

function consolidateTextContent(content) {
  if (typeof content === 'string') return content.trim();
  if (Array.isArray(content)) {
    return content.map(item => extractTextContent(item)).filter(Boolean).join(' ');
  }
  return extractTextContent(content);
}

function extractTextContent(element) {
  if (!element) return '';
  
  if (typeof element === 'string') {
    return element.trim();
  }
  
  if (typeof element === 'object') {
    // Detect mixed content: has both #text and child elements
    const hasText = element['#text'] !== undefined;
    const hasChildElements = Object.keys(element).some(key => !key.startsWith('@_') && key !== '#text');
    
    if (hasText && hasChildElements) {
      return extractMixedContentPreservingOrder(element);
    }
    
    const textParts = [];
    
    // Handle direct text content
    if (element['#text']) {
      const textContent = typeof element['#text'] === 'string' ? element['#text'] : String(element['#text']);
      textParts.push(textContent.trim());
    }
    
    // Recursively extract from child elements, including LINK tags
    for (const [key, value] of Object.entries(element)) {
      if (!key.startsWith('@_') && key !== '#text') {
        // Handle line break elements as spaces (including self-closing elements)
        if (key === 'LINEBRK' || key === 'BR' || key === 'br') {
          // Handle both self-closing and empty elements
          if (Array.isArray(value)) {
            // Multiple line breaks
            for (let i = 0; i < value.length; i++) {
              textParts.push(' ');
            }
          } else {
            // Single line break (self-closing or empty)
            textParts.push(' ');
          }
        } else if (Array.isArray(value)) {
          value.forEach(item => {
            const childText = extractTextContent(item);
            if (childText) textParts.push(childText);
          });
        } else {
          const childText = extractTextContent(value);
          if (childText) textParts.push(childText);
        }
      }
    }
    
    return textParts.join(' ').trim();
  }
  
  return '';
}

/**
 * Extract mixed content preserving document order by using regex replacement
 */
function extractMixedContentPreservingOrder(element) {
  // Get the original text content
  const textContent = String(element['#text'] || '');
  
  // For mixed content, we need to replace inline elements with their text content
  // This approach uses pattern matching to handle common citation formats
  
  let result = textContent;
  
  // Handle LINEBRK elements first - they need special handling for mixed content
  if (element.LINEBRK) {
    // For mixed content with LINEBRK, we need to insert spaces where line breaks occur
    // Since XML parsers don't preserve exact positioning, we'll use a heuristic approach
    const linebreakCount = Array.isArray(element.LINEBRK) ? element.LINEBRK.length : 1;
    
    // Common pattern: "NEW YORK CITY<LINEBRK/>ADMINISTRATIVE CODE"
    // Look for specific patterns where words are concatenated without spaces
    result = result.replace(/CITYADMINISTRATIVE/g, 'CITY ADMINISTRATIVE');
    result = result.replace(/([A-Z][a-z]+)([A-Z][A-Z]+)/g, '$1 $2'); // e.g., "CityADMINISTRATIVE" -> "City ADMINISTRATIVE"
    result = result.replace(/([A-Z]+)([A-Z][a-z]+)/g, '$1 $2'); // e.g., "CITYADMINISTRATIVE" -> "CITY Administrative"
  }
  
  // Handle LINK elements - replace placeholders or reconstruct based on patterns
  if (element.LINK) {
    const links = [];
    
    if (Array.isArray(element.LINK)) {
      element.LINK.forEach(link => {
        const linkText = extractTextContent(link);
        if (linkText) links.push(linkText);
      });
    } else {
      const linkText = extractTextContent(element.LINK);
      if (linkText) links.push(linkText);
    }
    
    // Pattern-based reconstruction for common citation formats
    if (textContent.includes('RCNY §') && links.length > 0) {
      // Handle "40 RCNY § and §" pattern with multiple links
      if (links.length === 2 && textContent.includes('and §')) {
        // Replace "§ and §" with "§ {first} and § {second}"
        result = textContent.replace(/§\s*and\s*§/, `§ ${links[0]} and § ${links[1]}`);
      } else if (links.length === 1) {
        // Replace first "§" with "§ {link}"
        result = textContent.replace(/§/, `§ ${links[0]}`);
      }
    } else {
      // For other patterns, append links at the end
      result = textContent + ' ' + links.join(' ');
    }
  }
  
  // Handle other inline elements similarly
  for (const [key, value] of Object.entries(element)) {
    if (!key.startsWith('@_') && key !== '#text' && key !== 'LINK' && key !== 'LINEBRK') {
      const childText = extractTextContent(value);
      if (childText) {
        result += ' ' + childText;
      }
    }
  }
  
  return result.replace(/\s+/g, ' ').trim();
}

function normalizeType(type) {
  if (!type) return 'unknown';
  return type.toLowerCase().replace(/[^a-z0-9]/g, '_');
}

function generateId(sourceId) {
  return `${sourceId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function generateCitation(unit, source) {
  const parts = [];
  if (unit.number) parts.push(unit.number);
  if (unit.heading) parts.push(unit.heading);
  return parts.join(' - ') || `${source.name} ${unit.type}`;
}

function generateSortKey(unit) {
  const parts = [];
  if (unit.number && typeof unit.number === 'string') {
    // Extract numeric parts for proper sorting
    const numMatch = unit.number.match(/(\d+)/);
    if (numMatch) {
      parts.push(numMatch[1].padStart(6, '0'));
    }
  }
  if (unit.heading && typeof unit.heading === 'string') {
    parts.push(unit.heading.toLowerCase());
  }
  return parts.join('_') || unit.id;
}
