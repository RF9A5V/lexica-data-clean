/**
 * American Legal Publishing (ALP) specific XML parser
 * Handles the LEVEL → RECORD → HEADING/PARA structure
 */

import { NdjsonWriter } from './ndjson_writer.js';
import { extractCitationsFromText } from './citation_extractor.js';

/**
 * Parse ALP XML structure with depth-first traversal
 */
export async function parseAlpXml(parsedXml, source, writer, options = {}) {
  const { verbose = false } = options;
  const results = { unitsExtracted: 0, citationsFound: 0 };
  
  // Track parent-child relationships
  const processedIds = new Set();
  
  /**
   * Process a LEVEL element recursively
   */
  async function processLevel(levelElement, parentId = null, depth = 0) {
    if (!levelElement || typeof levelElement !== 'object') return;
    
    // Extract level metadata
    const styleName = levelElement['@_style-name'];
    const levelDepth = levelElement['@_level-depth'];
    const levelId = levelElement['@_style-id'];
    
    if (!styleName) return; // Skip levels without style names
    
    // Normalize style name to use as type
    const normalizedType = styleName.toLowerCase().replace(/\s+/g, '_');
    
    if (verbose) {
      console.log(`${'  '.repeat(depth)}Processing ${styleName} (depth ${levelDepth})`);
    }
    
    // Process all RECORD elements at this level first
    if (levelElement.RECORD) {
      const records = Array.isArray(levelElement.RECORD) ? levelElement.RECORD : [levelElement.RECORD];
      
      for (const record of records) {
        const unit = await processRecord(record, normalizedType, source, parentId, levelId);
        if (unit && !processedIds.has(unit.id)) {
          await writer.write(unit);
          processedIds.add(unit.id);
          results.unitsExtracted++;
          
          // Extract citations from text content
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
        }
      }
    }
    
    // Then process nested LEVEL elements (depth-first)
    if (levelElement.LEVEL) {
      const nestedLevels = Array.isArray(levelElement.LEVEL) ? levelElement.LEVEL : [levelElement.LEVEL];
      
      for (const nestedLevel of nestedLevels) {
        await processLevel(nestedLevel, parentId, depth + 1);
      }
    }
  }
  
  /**
   * Process a RECORD element to extract HEADING and PARA content
   */
  async function processRecord(record, levelType, source, parentId, levelId) {
    if (!record || typeof record !== 'object') return null;
    
    // Generate unit ID
    const recordId = record['@_id'] || record['@_number'] || levelId;
    const unitId = `${source.id}-${levelType}-${recordId}`;
    
    const unit = {
      id: unitId,
      type: levelType,
      parent_id: parentId,
      source_id: source.id,
      created_at: new Date().toISOString()
    };
    
    // Extract HEADING as title
    if (record.HEADING) {
      unit.heading = extractTextContent(record.HEADING);
    }
    
    // Consolidate all PARA elements into text field
    if (record.PARA) {
      const paras = Array.isArray(record.PARA) ? record.PARA : [record.PARA];
      const textParts = [];
      
      for (const para of paras) {
        const paraText = extractTextContent(para);
        if (paraText && paraText.trim()) {
          textParts.push(paraText.trim());
        }
      }
      
      unit.text = textParts.join(' ');
    }
    
    // Extract other metadata
    if (record['@_number']) {
      unit.number = record['@_number'];
    }
    
    if (record['@_version']) {
      unit.version = record['@_version'];
    }
    
    // Generate citation and sort key
    unit.citation = generateCitation(source, unit, levelType);
    unit.sort_key = generateSortKey(unit);
    
    return unit;
  }
  
  /**
   * Extract text content recursively from any element
   */
  function extractTextContent(element) {
    if (!element) return '';
    
    if (typeof element === 'string') {
      return element.trim();
    }
    
    if (typeof element === 'object') {
      const textParts = [];
      
      // Handle direct text content
      if (element['#text']) {
        const textContent = typeof element['#text'] === 'string' ? element['#text'] : String(element['#text']);
        textParts.push(textContent.trim());
      }
      
      // Recursively extract from child elements
      for (const [key, value] of Object.entries(element)) {
        if (!key.startsWith('@_') && key !== '#text') {
          if (Array.isArray(value)) {
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
      
      return textParts.filter(text => text.length > 0).join(' ');
    }
    
    return '';
  }
  
  /**
   * Generate human-readable citation
   */
  function generateCitation(source, unit, levelType) {
    const parts = [];
    
    if (unit.number) {
      parts.push(unit.number);
    }
    
    if (source.code_key) {
      parts.push(source.code_key.toUpperCase());
    }
    
    if (unit.heading) {
      parts.push(unit.heading);
    }
    
    return parts.join(' ').trim() || `${levelType} ${unit.id}`;
  }
  
  /**
   * Generate sort key for proper ordering
   */
  function generateSortKey(unit) {
    if (unit.number) {
      const numMatch = unit.number.toString().match(/(\d+)/);
      if (numMatch) {
        return parseInt(numMatch[1]).toString().padStart(6, '0');
      }
    }
    
    return '000000';
  }
  
  // Start processing from the root DOCUMENT
  if (parsedXml.DOCUMENT && parsedXml.DOCUMENT.LEVEL) {
    const rootLevels = Array.isArray(parsedXml.DOCUMENT.LEVEL) ? parsedXml.DOCUMENT.LEVEL : [parsedXml.DOCUMENT.LEVEL];
    
    for (const rootLevel of rootLevels) {
      await processLevel(rootLevel, null, 0);
    }
  }
  
  return results;
}
