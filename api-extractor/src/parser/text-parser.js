/**
 * NYSenate Legislative Text Parser
 * Parses hierarchical legislative text and generates interpolation tokens
 */

class TextParser {
  constructor() {
    // Hierarchical marker patterns - simple line-based matching
    this.patterns = [
      // NYSenate format patterns (with 2-space indentation)
      { name: 'subsection', test: (line) => /^  (\d+(?:-[a-z]+)*)\.\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^  (\d+(?:-[a-z]+)*)\.\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'SUBSECTION' },
      { name: 'paragraph', test: (line) => /^  \(([a-z]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^  \(([a-z]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'PARAGRAPH' },
      { name: 'subparagraph', test: (line) => /^  \(([ivx]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^  \(([ivx]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'SUBPARAGRAPH' },
      { name: 'clause', test: (line) => /^  \(([A-Z]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^  \(([A-Z]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'CLAUSE' },
      { name: 'item', test: (line) => /^  \((\d+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^  \((\d+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'ITEM' },
      
      // Original patterns (flexible spacing) as fallback
      { name: 'subsection', test: (line) => /^\s*(\d+(?:-[a-z]+)*)\.\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^\s*(\d+(?:-[a-z]+)*)\.\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'SUBSECTION' },
      { name: 'paragraph', test: (line) => /^\s*\(([a-z]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^\s*\(([a-z]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'PARAGRAPH' },
      { name: 'subparagraph', test: (line) => /^\s*\(([ivx]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^\s*\(([ivx]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'SUBPARAGRAPH' },
      { name: 'clause', test: (line) => /^\s*\(([A-Z]+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^\s*\(([A-Z]+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'CLAUSE' },
      { name: 'item', test: (line) => /^\s*\((\d+)\)\s+(.+)$/.test(line), extract: (line) => { const m = line.match(/^\s*\((\d+)\)\s+(.+)$/); return m ? { number: m[1], content: line.trim() } : null; }, tokenPrefix: 'ITEM' }
    ];
  }

  /**
   * Main tokenization function - converts text to tokens and extracts children
   */
  tokenizeText(text, parentId) {
    if (!text || !text.trim()) {
      return { tokenizedText: text, childElements: [] };
    }

    // Normalize text
    const normalizedText = this.normalizeText(text);
    
    // Process line by line
    const lines = normalizedText.split('\n');
    const childElements = [];
    const tokenizedLines = [];
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const hierarchicalElement = this.identifyHierarchicalElement(line);
      
      if (hierarchicalElement) {
        // Generate token for this hierarchical element
        const token = this.generateToken(parentId, hierarchicalElement.tokenPrefix, hierarchicalElement.number);
        
        childElements.push({
          type: hierarchicalElement.name,
          number: hierarchicalElement.number,
          text: hierarchicalElement.content,
          token: token,
          start: 0, // Will be calculated later if needed
          end: 0    // Will be calculated later if needed
        });
        
        tokenizedLines.push(token);
      } else {
        tokenizedLines.push(line);
      }
    }

    return {
      tokenizedText: tokenizedLines.join('\n'),
      childElements
    };
  }

  /**
   * Identify if a line contains a hierarchical element
   */
  identifyHierarchicalElement(line) {
    for (const pattern of this.patterns) {
      if (pattern.test(line)) {
        const extracted = pattern.extract(line);
        if (extracted) {
          return {
            name: pattern.name,
            tokenPrefix: pattern.tokenPrefix,
            number: extracted.number,
            content: extracted.content
          };
        }
      }
    }
    return null;
  }

  /**
   * Identify all hierarchical markers in text (for testing)
   */
  identifyHierarchicalMarkers(text) {
    const markers = [];
    const lines = text.split('\n');
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      const element = this.identifyHierarchicalElement(line);
      if (element) {
        const marker = {
          type: element.name,
          number: element.number,
          position: i
        };
        markers.push(marker);
      }
    }
    
    return markers;
  }

  /**
   * Extract hierarchical child elements from text (legacy method)
   */
  extractChildElements(text, parentId) {
    const elements = [];
    const processedRanges = new Set();

    // Process each pattern type in hierarchical order
    for (const pattern of this.patterns) {
      const matches = this.findMatches(text, pattern);
      
      for (const match of matches) {
        // Skip if this range was already processed by a higher-priority pattern
        if (this.isRangeProcessed(match.start, match.end, processedRanges)) {
          continue;
        }

        const element = {
          type: pattern.name,
          number: match.number,
          text: match.content.trim(),
          token: this.generateToken(parentId, pattern.tokenPrefix, match.number),
          start: match.start,
          end: match.end,
          children: []
        };

        // Recursively process child content for nested elements
        if (element.text.length > 0) {
          const nestedChildren = this.extractChildElements(element.text, element.token);
          if (nestedChildren.length > 0) {
            element.children = nestedChildren;
            // Replace nested content with tokens in parent text
            element.text = this.tokenizeNestedContent(element.text, nestedChildren);
          }
        }

        elements.push(element);
        this.markRangeProcessed(match.start, match.end, processedRanges);
      }
    }

    // Sort by position in original text
    return elements.sort((a, b) => a.start - b.start);
  }

  /**
   * Find all matches for a given pattern in text (legacy method)
   */
  findMatches(text, pattern) {
    const matches = [];
    
    // This method is no longer used with the new line-based approach
    if (!pattern.regex) {
      return matches;
    }
    
    let match;
    
    // Reset regex state
    pattern.regex.lastIndex = 0;
    
    while ((match = pattern.regex.exec(text)) !== null) {
      const number = match[1]; // Number/identifier
      const content = match[2]; // Content after marker
      const fullContent = match[0]; // Full line including marker
      
      matches.push({
        start: match.index,
        end: match.index + match[0].length,
        number: number,
        content: fullContent, // Full line including marker
        fullMatch: match[0]
      });
    }

    return matches;
  }

  /**
   * Check if a text range has already been processed
   */
  isRangeProcessed(start, end, processedRanges) {
    for (const range of processedRanges) {
      if (this.rangesOverlap(start, end, range.start, range.end)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Mark a text range as processed
   */
  markRangeProcessed(start, end, processedRanges) {
    processedRanges.add({ start, end });
  }

  /**
   * Check if two ranges overlap
   */
  rangesOverlap(start1, end1, start2, end2) {
    return start1 < end2 && start2 < end1;
  }

  /**
   * Generate consistent token identifier
   */
  generateToken(parentId, tokenPrefix, number) {
    // Extract section identifier from parent ID
    const sectionMatch = parentId.match(/section:([^:]+)$/);
    const sectionId = sectionMatch ? sectionMatch[1] : 'unknown';
    
    return `{{${tokenPrefix}_${sectionId}_${number}}}`;
  }

  /**
   * Replace nested content with tokens
   */
  tokenizeNestedContent(text, nestedChildren) {
    let tokenizedText = text;
    
    // Sort children by position (reverse order to maintain positions)
    const sortedChildren = [...nestedChildren].sort((a, b) => b.start - a.start);
    
    for (const child of sortedChildren) {
      const beforeToken = tokenizedText.substring(0, child.start);
      const afterToken = tokenizedText.substring(child.end);
      tokenizedText = beforeToken + child.token + afterToken;
    }
    
    return tokenizedText;
  }

  /**
   * Split text into header and hierarchical body
   */
  splitHeaderAndBody(text) {
    // Find first hierarchical marker
    let firstMarkerPos = text.length;
    
    for (const pattern of this.patterns) {
      pattern.regex.lastIndex = 0;
      const match = pattern.regex.exec(text);
      if (match && match.index < firstMarkerPos) {
        firstMarkerPos = match.index;
      }
    }

    if (firstMarkerPos === text.length) {
      return { header: text, body: '' };
    }

    return {
      header: text.substring(0, firstMarkerPos).trim(),
      body: text.substring(firstMarkerPos).trim()
    };
  }

  /**
   * Identify all hierarchical markers in text
   */
  identifyHierarchicalMarkers(text) {
    const markers = [];
    
    for (const pattern of this.patterns) {
      const matches = this.findMatches(text, pattern);
      for (const match of matches) {
        markers.push({
          type: pattern.name,
          number: match.number,
          position: match.start
        });
      }
    }

    return markers.sort((a, b) => a.position - b.position);
  }

  /**
   * Normalize text for consistent processing
   */
  normalizeText(text) {
    return text
      .replace(/\r\n/g, '\n')           // Normalize line endings
      .replace(/[ \t]+/g, ' ')         // Collapse multiple spaces/tabs to single space
      .replace(/\n[ \t]+/g, '\n')      // Remove leading whitespace on lines
      .replace(/[ \t]+\n/g, '\n')      // Remove trailing whitespace on lines
      .replace(/\n{3,}/g, '\n\n')      // Collapse multiple newlines
      .trim();                         // Remove leading/trailing whitespace
  }

  /**
   * Validate token format
   */
  isValidToken(token) {
    return /^{{[A-Z_]+_[^_}]+_[^_}]+}}$/.test(token);
  }

  /**
   * Extract token components
   */
  parseToken(token) {
    const match = token.match(/^{{([A-Z_]+)_([^_}]+)_([^_}]+)}}$/);
    if (!match) return null;
    
    return {
      type: match[1],
      sectionId: match[2],
      number: match[3]
    };
  }
}

export { TextParser };
