import fs from 'fs';
import path from 'path';
import { readNDJSON } from '../lib/utils.js';
import { parseSubsections } from '../transform/subsection_parser.js';

/**
 * Test utility to reconstitute sections from NDJSON data and verify against cache
 */
class ReconstitutionTester {
  constructor() {
    this.entries = new Map();
    this.errors = [];
    this.warnings = [];
  }

  /**
   * Load NDJSON data into memory for processing
   */
  loadNDJSON(filePath) {
    const data = readNDJSON(filePath);
    this.entries.clear();
    
    for (const entry of data) {
      this.entries.set(entry.id, entry);
    }
    
    console.log(`📚 Loaded ${this.entries.size} entries from ${path.basename(filePath)}`);
  }

  /**
   * Load cached section data for comparison
   */
  loadCacheData(cacheDir, lawCode) {
    const cacheFile = path.join(cacheDir, `${lawCode.toLowerCase()}-sections.json`);
    if (!fs.existsSync(cacheFile)) {
      throw new Error(`Cache file not found: ${cacheFile}`);
    }
    
    const cacheFile_content = JSON.parse(fs.readFileSync(cacheFile, 'utf8'));
    const cacheData = cacheFile_content.sections || [];
    console.log(`📦 Loaded ${cacheData.length} cached sections for ${lawCode}`);
    return cacheData;
  }

  /**
   * Interpolate tokens in text with actual content from entries
   */
  interpolateTokens(text, depth = 0) {
    if (depth > 10) {
      this.warnings.push(`Maximum interpolation depth reached for text: ${text.substring(0, 100)}...`);
      return text;
    }

    // Find all tokens in the format {{TYPE_PARENT_CHILD}}
    const tokenRegex = /\{\{([A-Z_]+)_([^}]+)\}\}/g;
    let interpolated = text;
    let hasTokens = false;

    interpolated = interpolated.replace(tokenRegex, (match, tokenType, tokenPath) => {
      hasTokens = true;
      
      // Find the entry that matches this token
      const entryId = this.findEntryByToken(tokenType, tokenPath);
      if (!entryId) {
        this.errors.push(`Token not found: ${match}`);
        return `[MISSING: ${match}]`;
      }

      const entry = this.entries.get(entryId);
      if (!entry || !entry.text) {
        this.errors.push(`Entry has no text: ${entryId}`);
        return `[NO TEXT: ${match}]`;
      }

      // Recursively interpolate tokens in the found text
      return this.interpolateTokens(entry.text, depth + 1);
    });

    return interpolated;
  }

  /**
   * Find entry ID that matches a token pattern
   */
  findEntryByToken(tokenType, tokenPath) {
    const expectedType = tokenType.toLowerCase();
    
    // For tokens like "PARAGRAPH_17.2.a", construct the expected entry ID
    // Token format: PARAGRAPH_17.2.a where PARAGRAPH is type and 17.2.a is the hierarchical path
    // Entry ID format: nysenate:abc:section:17_paragraph_2.a (section number, then element path without section prefix)
    const pathParts = tokenPath.split('.');
    if (pathParts.length >= 2) {
      const parentSection = pathParts[0];
      // The child identifier is everything after the section number
      const childIdentifier = pathParts.slice(1).join('.');
      
      // Construct the expected entry ID pattern (case insensitive)
      const expectedIdPattern = `section:${parentSection.toLowerCase()}_${expectedType}_${childIdentifier.toLowerCase()}`;
      
      // Search for entries that match this pattern (case insensitive)
      for (const [id, entry] of this.entries) {
        if (id.toLowerCase().includes(expectedIdPattern)) {
          return id;
        }
      }
    }
    
    return null;
  }

  /**
   * Test reconstitution of a single section
   */
  testSection(sectionId, originalText, logErrors = true) {
    const section = this.entries.get(sectionId);
    if (!section) {
      if (logErrors) {
        this.errors.push(`Section not found: ${sectionId}`);
      }
      return false;
    }

    console.log(`🔍 Testing section: ${sectionId}`);
    
    // Determine if this section should be compared as caption-only or full content
    // If cached text is much longer than NDJSON text, this section contains subsections
    // that should be interpolated for comparison
    const shouldInterpolate = originalText.length > section.text.length * 2;
    
    let reconstituted;
    if (shouldInterpolate && section.text.includes('{{')) {
      // Full content section - interpolate tokens
      reconstituted = this.interpolateTokens(section.text);
      // Apply same normalization to reconstituted text as we do to original
      reconstituted = this.normalizeFullText(reconstituted, section.number);
    } else {
      // Caption-only section - extract just the caption
      reconstituted = this.extractSectionCaption(section.text);
    }
    
    // Normalize the original text using the same parser transformations
    const normalizedOriginal = shouldInterpolate ? 
      this.normalizeFullText(originalText, section.number) :
      this.normalizeExpectedText(originalText, section.number);
    const cleanOriginal = this.normalizeText(normalizedOriginal);
    const cleanReconstituted = this.normalizeText(reconstituted);
    
    // Compare texts
    if (cleanOriginal === cleanReconstituted) {
      console.log(`✅ Section ${section.number} reconstitution successful`);
      return true;
    } else {
      this.errors.push(`Section ${section.number} reconstitution mismatch`);
      console.log(`❌ Section ${section.number} reconstitution failed`);
      console.log(`Expected length: ${cleanOriginal.length}`);
      console.log(`Actual length: ${cleanReconstituted.length}`);
      
      // Show first difference
      const diffIndex = this.findFirstDifference(cleanOriginal, cleanReconstituted);
      if (diffIndex >= 0) {
        const context = 50;
        const start = Math.max(0, diffIndex - context);
        const end = Math.min(cleanOriginal.length, diffIndex + context);
        
        console.log(`First difference at position ${diffIndex}:`);
        console.log(`Original: "${cleanOriginal.substring(start, end)}"`);
        console.log(`Reconstituted: "${cleanReconstituted.substring(start, end)}"`);
      }
      
      return false;
    }
  }

  /**
   * Extract only the caption (non-token text) from a section entry
   * Section entries may contain full text or "Caption\n{{TOKEN1}}\n{{TOKEN2}}" - we want just the caption
   */
  extractSectionCaption(sectionText) {
    // Check if this contains tokens - if so, extract non-token lines
    if (sectionText.includes('{{')) {
      const lines = sectionText.split('\n');
      const captionLines = lines.filter(line => !line.trim().match(/^\{\{[^}]+\}\}$/));
      const caption = captionLines.join('\n').trim();
      
      // Apply same normalization as normalizeExpectedText to extract just the caption
      return this.extractCaptionFromFullText(caption);
    } else {
      // This is full section text, extract caption using same logic as normalizeExpectedText
      return this.extractCaptionFromFullText(sectionText);
    }
  }

  /**
   * Extract caption from full section text (same logic as normalizeExpectedText)
   */
  extractCaptionFromFullText(text) {
    // Normalize line breaks and escape sequences
    let normalized = text.replace(/\\n/g, '\n').replace(/\r\n?/g, '\n');
    
    // Try to match leading "§ <num>. <caption>. <rest>"
    const sectionMatch = normalized.match(/^\s*§\s*\d+[a-z-]*\.\s*(.+?)\.\s*([\s\S]*)$/s);
    if (sectionMatch) {
      const caption = (sectionMatch[1] || '').trim();
      return caption;
    } else {
      // Fallback: strip just the section marker
      normalized = normalized.replace(/^\s*§\s*\d+[a-z-]*\.\s*/m, '');
      const subdivisionMatch = /(\d+(?:-[a-z]+)*)\.\s/.exec(normalized);
      if (subdivisionMatch) {
        normalized = normalized.slice(subdivisionMatch.index);
      }
      return normalized.trim();
    }
  }

  /**
   * Normalize full section text (for sections with subsections)
   * This strips the section prefix but keeps the full content
   */
  normalizeFullText(text, sectionNumber) {
    // Normalize line breaks and escape sequences
    let normalized = text.replace(/\\n/g, '\n').replace(/\r\n?/g, '\n');
    
    // Strip the section prefix "§ X." but keep all content
    normalized = normalized.replace(/^\s*\*?\s*§\s*\d+[a-z-]*\.\s*/m, '');
    
    return normalized.trim();
  }

  /**
   * Normalize cached text using the same transformations as the parser
   * This applies the same text processing that the parser does to create the expected format
   */
  normalizeExpectedText(text, sectionNumber) {
    // Apply the same transformations that the parser applies:
    
    // 1. Normalize line breaks and escape sequences
    let normalized = text.replace(/\\n/g, '\n').replace(/\r\n?/g, '\n');
    
    // 2. Strip section prefix and extract caption + body (same as splitCaptionAndBody logic)
    // Try to match leading "§ <num>. <caption>. <rest>"
    const sectionMatch = normalized.match(/^\s*§\s*\d+[a-z-]*\.\s*(.+?)\.\s*([\s\S]*)$/s);
    if (sectionMatch) {
      const caption = (sectionMatch[1] || '').trim();
      const body = (sectionMatch[2] || '').trim();
      
      // The parser creates section text as: caption + subsection tokens
      // For reconstitution, we want just the caption since subsections are tokenized
      normalized = caption;
    } else {
      // Fallback: strip just the section marker and use stripCaption logic
      normalized = normalized.replace(/^\s*§\s*\d+[a-z-]*\.\s*/m, '');
      const subdivisionMatch = /(\d+(?:-[a-z]+)*)\.\s/.exec(normalized);
      if (subdivisionMatch) {
        normalized = normalized.slice(subdivisionMatch.index);
      }
    }
    
    return normalized;
  }

  /**
   * Normalize text for comparison
   */
  normalizeText(text) {
    return text
      .replace(/\s+/g, ' ')  // Normalize whitespace
      .replace(/\n+/g, '\n') // Normalize line breaks
      .trim();
  }

  /**
   * Find first character difference between two strings
   */
  findFirstDifference(str1, str2) {
    const minLength = Math.min(str1.length, str2.length);
    for (let i = 0; i < minLength; i++) {
      if (str1[i] !== str2[i]) {
        return i;
      }
    }
    return str1.length !== str2.length ? minLength : -1;
  }

  /**
   * Run full test suite
   */
  async runTests(ndjsonPath, cacheDir, lawCode, sectionNumber = null) {
    console.log(`🧪 Starting reconstitution tests for ${lawCode}${sectionNumber ? ` section ${sectionNumber}` : ''}`);
    
    // Load data
    this.loadNDJSON(ndjsonPath);
    const cacheData = this.loadCacheData(cacheDir, lawCode);
    
    // Filter sections if specific section requested
    const sectionsToTest = sectionNumber 
      ? cacheData.filter(s => s.docType === 'SECTION' && s.docId === sectionNumber)
      : cacheData.filter(s => s.docType === 'SECTION' && s.text);
    
    if (sectionNumber && sectionsToTest.length === 0) {
      console.error(`❌ Section ${sectionNumber} not found in cached data`);
      return { passed: 0, total: 0, success: false, errors: [`Section ${sectionNumber} not found`], warnings: [] };
    }
    
    // Test each cached section
    let passCount = 0;
    let totalCount = 0;
    
    for (const cachedSection of sectionsToTest) {
      if (cachedSection.text) {
        totalCount++;
        
        // Find corresponding NDJSON entry
        // Try both uppercase and lowercase versions to handle case sensitivity
        const sectionIdUpper = `nysenate:${lawCode.toLowerCase()}:section:${cachedSection.docId}`;
        const sectionIdLower = `nysenate:${lawCode.toLowerCase()}:section:${cachedSection.docId.toLowerCase()}`;
        
        // Try uppercase first (don't log errors), then lowercase, then log error if neither works
        if (this.testSection(sectionIdUpper, cachedSection.text, false)) {
          passCount++;
        } else if (this.testSection(sectionIdLower, cachedSection.text, false)) {
          passCount++;
        } else {
          // Neither format worked, log the error
          this.errors.push(`Section not found: ${cachedSection.docId} (tried both ${sectionIdUpper} and ${sectionIdLower})`);
        }
      }
    }
    
    // Report results
    console.log(`\n📊 Test Results:`);
    console.log(`✅ Passed: ${passCount}/${totalCount} sections`);
    console.log(`❌ Failed: ${totalCount - passCount}/${totalCount} sections`);
    
    if (this.warnings.length > 0) {
      console.log(`⚠️  Warnings: ${this.warnings.length}`);
      this.warnings.forEach(warning => console.log(`   ${warning}`));
    }
    
    if (this.errors.length > 0) {
      console.log(`🚨 Errors: ${this.errors.length}`);
      this.errors.slice(0, 10).forEach(error => console.log(`   ${error}`));
      if (this.errors.length > 10) {
        console.log(`   ... and ${this.errors.length - 10} more errors`);
      }
    }
    
    return {
      passed: passCount,
      total: totalCount,
      success: passCount === totalCount,
      errors: this.errors,
      warnings: this.warnings
    };
  }
}

export { ReconstitutionTester };
