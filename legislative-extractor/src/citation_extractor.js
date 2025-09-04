/**
 * Citation extractor for legislative texts
 * Extracts statute and regulation citations from legislative content
 */

import fs from 'fs/promises';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';

// Citation patterns for different legislative sources
const CITATION_PATTERNS = [
  // New York statutes (DRL, FCA, CPLR, RPL, ECL, etc.)
  {
    pattern: /\b(DRL|FCA|CPLR|RPL|ECL|GOL|ABC|CORP|BUSC|INS|LAB|NAV|NPCL|PAR|PBH|PUB|RAC|RPAPL|TAX|V&T|CAN|UCC|EPTL|EDL)\s*§\s*([\w\.\-]+)(?:\s*\([^)]+\))?\b/g,
    sourceType: 'statute_code',
    format: 'nys_statute'
  },
  // RCNY (Rules of the City of New York)
  {
    pattern: /\b(\d+)\s*RCNY\s*§\s*([\d\.\-]+(?:[a-z])?)\b/g,
    sourceType: 'regulatory_code',
    format: 'rcny'
  },
  // RCNY continuation pattern (for "and § X-XX" after initial RCNY citation)
  {
    pattern: /(?:\b(\d+)\s*RCNY\s*§\s*[\d\.\-]+(?:[a-z])?\b.*?)\band\s*§\s*([\d\.\-]+(?:[a-z])?)\b/g,
    sourceType: 'regulatory_code',
    format: 'rcny_continuation'
  },
  // NYCRR (New York Codes, Rules and Regulations)
  {
    pattern: /\b(\d+)\s*NYCRR\s*(?:Part|§)\s*([\w\.\-]+)\b/g,
    sourceType: 'regulatory_code',
    format: 'nycrr'
  },
  // Federal statutes
  {
    pattern: /\b(\d+)\s*U\.?S\.?C\.?\s*(?:§|sec\.?)\s*([\w\.\-]+)\b/g,
    sourceType: 'statute_code',
    format: 'usc'
  },
  // CFR (Code of Federal Regulations)
  {
    pattern: /\b(\d+)\s*C\.?F\.?R\.?\s*(?:§|sec\.?)\s*([\w\.\-]+)\b/g,
    sourceType: 'regulatory_code',
    format: 'cfr'
  },
  // State statutes (generic pattern for other states)
  {
    pattern: /\b([A-Z]{2})\s*(?:Code|Stat\.?|Ann\.?)\s*(?:§|sec\.?)\s*([\w\.\-]+)\b/g,
    sourceType: 'statute_code',
    format: 'state_statute'
  }
];

/**
 * Extract citations from legislative text content
 */
export async function extractCitationsFromLegislativeTexts(source, options = {}) {
  const { verbose = false, dryRun = false } = options;
  const results = { processedUnits: 0, citationsFound: 0 };

  if (dryRun) {
    console.log(`  Would extract citations from legislative texts for source ${source.id}`);
    return results;
  }

  // Get all units with text content from database
  const unitsWithText = await getUnitsWithText(source);

  if (verbose) {
    console.log(`  Found ${unitsWithText.length} units with text content`);
  }

  const citations = [];

  for (const unit of unitsWithText) {
    const extractedCitations = extractCitationsFromText(unit.text_plain || unit.text_html || '');

    for (const citation of extractedCitations) {
      citations.push({
        source_unit_id: unit.id,
        raw_citation: citation.rawText,
        target_kind: citation.targetKind,
        external_curie: citation.curie,
        context_snippet: citation.context,
        citation_format: citation.format,
        source_id: source.id,
        created_at: new Date().toISOString()
      });
    }

    results.processedUnits++;

    if (verbose && extractedCitations.length > 0) {
      console.log(`  Unit ${unit.id}: ${extractedCitations.length} citations`);
    }
  }

  results.citationsFound = citations.length;

  // Store citations in database
  if (citations.length > 0) {
    await storeCitations(source, citations, { verbose });
  }

  return results;
}

/**
 * Extract citations from a single text string
 */
export function extractCitationsFromText(text, sourceId, sourceUnitId = null) {
  const citations = [];
  
  if (!text || typeof text !== 'string') {
    return citations;
  }

  // Process each citation pattern
  for (const { pattern, sourceType, format } of CITATION_PATTERNS) {
    let match;
    while ((match = pattern.exec(text)) !== null) {
      const rawText = match[0];
      const citation = parseCitationMatch(match, format);

      if (citation) {
        // Extract context around citation
        const context = extractContext(text, match.index, rawText.length);

        const citationRecord = {
          id: `citation_${sourceId}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
          type: 'citation',
          rawText,
          targetKind: sourceType,
          ...citation,
          format,
          context,
          source_id: sourceId,
          source_unit_id: sourceUnitId
        };

        // Validate citation structure before adding
        if (!citationRecord.id || !citationRecord.type || !citationRecord.curie) {
          console.log(`⚠️  Malformed citation detected:`, {
            rawText,
            citation,
            format,
            sourceType,
            missing: {
              id: !citationRecord.id,
              type: !citationRecord.type,
              curie: !citationRecord.curie
            },
            textSnippet: context ? context.substring(0, 100) + '...' : 'no context'
          });
          continue; // Skip malformed citations
        }

        citations.push(citationRecord);
      }
    }
  }

  return citations;
}

/**
 * Parse a citation match into structured data
 */
function parseCitationMatch(match, format) {
  const groups = match.slice(1); // Remove full match

  switch (format) {
    case 'nys_statute':
      // DRL § 240, FCA § 1234-a, etc.
      const statuteCode = groups[0].toLowerCase();
      const section = groups[1];
      return {
        curie: `${statuteCode}:${section}`,
        statuteCode,
        section
      };

    case 'rcny':
      // 34 RCNY § 4-08
      const rcnyTitle = groups[0];
      const rcnySection = groups[1];
      return {
        curie: `rcny:${rcnyTitle}-${rcnySection}`,
        title: rcnyTitle,
        section: rcnySection
      };

    case 'rcny_continuation':
      // Handles "40 RCNY § 1-03 and § 1-04" - extracts the continuation section
      const contTitle = groups[0]; // Title from the initial RCNY citation
      const contSection = groups[1]; // Section from "and § X-XX"
      return {
        curie: `rcny:${contTitle}-${contSection}`,
        title: contTitle,
        section: contSection
      };

    case 'nycrr':
      // 19 NYCRR § 1200.1
      const nycrrTitle = groups[0];
      const nycrrSection = groups[1];
      return {
        curie: `nycrr:${nycrrTitle}-${nycrrSection}`,
        title: nycrrTitle,
        section: nycrrSection
      };

    case 'usc':
      // 42 U.S.C. § 1983
      const uscTitle = groups[0];
      const uscSection = groups[1];
      return {
        curie: `usc:${uscTitle}-${uscSection}`,
        title: uscTitle,
        section: uscSection
      };

    case 'cfr':
      // 29 C.F.R. § 1910.95
      const cfrTitle = groups[0];
      const cfrSection = groups[1];
      return {
        curie: `cfr:${cfrTitle}-${cfrSection}`,
        title: cfrTitle,
        section: cfrSection
      };

    case 'state_statute':
      // CA Code § 1234
      const stateAbbr = groups[0].toLowerCase();
      const stateSection = groups[1];
      return {
        curie: `${stateAbbr}:${stateSection}`,
        state: stateAbbr,
        section: stateSection
      };

    default:
      return null;
  }
}

/**
 * Extract context snippet around citation
 */
function extractContext(text, citationIndex, citationLength) {
  const CONTEXT_WINDOW = 100;
  const start = Math.max(0, citationIndex - CONTEXT_WINDOW);
  const end = Math.min(text.length, citationIndex + citationLength + CONTEXT_WINDOW);

  let context = text.slice(start, end);

  // Add ellipsis if truncated
  if (start > 0) context = '...' + context;
  if (end < text.length) context = context + '...';

  return context.trim();
}

/**
 * Get units with text content from database
 */
async function getUnitsWithText(source) {
  // This would connect to the legislative database and query units with text
  // Placeholder for actual database query
  return [];
}

/**
 * Store extracted citations in database
 */
async function storeCitations(source, citations, options = {}) {
  const { verbose = false } = options;

  // This would insert citations into the legislative database
  // Placeholder for actual database insertion

  if (verbose) {
    console.log(`  Stored ${citations.length} citations for source ${source.id}`);
  }
}

/**
 * Generate citation statistics
 */
export function generateCitationStats(citations) {
  const stats = {
    total: citations.length,
    byTargetKind: {},
    byFormat: {},
    bySource: {},
    curies: new Set()
  };

  for (const citation of citations) {
    // Count by target kind
    stats.byTargetKind[citation.targetKind] = (stats.byTargetKind[citation.targetKind] || 0) + 1;

    // Count by format
    stats.byFormat[citation.format] = (stats.byFormat[citation.format] || 0) + 1;

    // Track sources
    if (citation.curie) {
      stats.curies.add(citation.curie);
      const source = citation.curie.split(':')[0];
      stats.bySource[source] = (stats.bySource[source] || 0) + 1;
    }
  }

  stats.uniqueCuries = stats.curies.size;
  return stats;
}

/**
 * Validate CURIE format
 */
export function validateCurie(curie) {
  if (!curie || typeof curie !== 'string') return false;

  // Basic CURIE validation: namespace:identifier
  const parts = curie.split(':');
  if (parts.length !== 2) return false;

  const [namespace, identifier] = parts;
  if (!namespace || !identifier) return false;

  // Namespace should be alphanumeric with possible hyphens/underscores
  if (!/^[a-zA-Z][a-zA-Z0-9_-]*$/.test(namespace)) return false;

  return true;
}

/**
 * Normalize citation text for matching
 */
export function normalizeCitationText(text) {
  return text
    .replace(/\s+/g, ' ')
    .replace(/§/g, '§')
    .replace(/sec\.?/gi, '§')
    .trim();
}
