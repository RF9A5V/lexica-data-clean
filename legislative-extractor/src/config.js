/**
 * Configuration constants and utilities for legislative extractor
 */

// Default extraction configuration
export const EXTRACTION_CONFIG = {
  maxConcurrency: 5,
  batchSize: 1000,
  retryAttempts: 3,
  downloadDelayMs: 1000,
  xmlParserOptions: {
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    allowBooleanAttributes: true,
    parseAttributeValue: true,
    trimValues: true
  }
};

// Default ZIP processing configuration
export const ZIP_CONFIG = {
  maxConcurrency: 3,
  retryAttempts: 3,
  downloadDelayMs: 500,
  validateZipFiles: true
};

// Default database configuration
export const DATABASE_CONFIG = {
  host: 'localhost',
  port: 5432,
  user: 'dev',
  password: 'dev',
  database: 'legislative_data',
  ssl: false,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000
};

// Unit type definitions for legislative hierarchy
export const UNIT_TYPES = [
  'title',
  'subtitle',
  'chapter',
  'subchapter',
  'article',
  'subarticle',
  'part',
  'subpart',
  'division',
  'subdivision',
  'section',
  'subsection',
  'paragraph',
  'subparagraph',
  'clause',
  'subclause',
  'appendix',
  'other'
];

// Citation target kinds for cross-references
export const CITATION_TARGET_KINDS = [
  'statute_section',
  'reg_section',
  'case',
  'unknown'
];

// Change action types for legislative amendments
export const CHANGE_ACTIONS = [
  'add',
  'amend',
  'repeal',
  'renumber',
  'reserved'
];

/**
 * Validate configuration object
 */
export function validateConfig(config) {
  const errors = [];

  // Check required top-level fields
  if (!config.database) {
    errors.push('Missing required field: database');
  }

  if (!config.sources || !Array.isArray(config.sources)) {
    errors.push('Missing or invalid field: sources (must be array)');
  } else if (config.sources.length === 0) {
    errors.push('Sources array cannot be empty');
  }

  // Validate database configuration
  if (config.database) {
    const requiredDbFields = ['host', 'port', 'database', 'user', 'password'];
    for (const field of requiredDbFields) {
      if (!config.database[field]) {
        errors.push(`Missing database field: ${field}`);
      }
    }
  }

  // Validate sources
  if (config.sources) {
    config.sources.forEach((source, index) => {
      if (!source.id) {
        errors.push(`Source ${index}: missing required field 'id'`);
      }
      if (!source.label) {
        errors.push(`Source ${index}: missing required field 'label'`);
      }
      if (!source.instrument_kind) {
        errors.push(`Source ${index}: missing required field 'instrument_kind'`);
      } else if (!['statute_code', 'regulatory_code', 'charter'].includes(source.instrument_kind)) {
        errors.push(`Source ${index}: invalid instrument_kind '${source.instrument_kind}'`);
      }
      if (!source.code_key) {
        errors.push(`Source ${index}: missing required field 'code_key'`);
      }
      if (!source.jurisdiction_scope) {
        errors.push(`Source ${index}: missing required field 'jurisdiction_scope'`);
      } else if (!['city', 'state', 'federal', 'national'].includes(source.jurisdiction_scope)) {
        errors.push(`Source ${index}: invalid jurisdiction_scope '${source.jurisdiction_scope}'`);
      }
    });
  }

  // Validate XML structure if present
  if (config.xml_structure) {
    if (!config.xml_structure.root_element) {
      errors.push('Missing xml_structure.root_element');
    }
    if (!config.xml_structure.hierarchy || !Array.isArray(config.xml_structure.hierarchy)) {
      errors.push('Missing or invalid xml_structure.hierarchy');
    }
  }

  return {
    isValid: errors.length === 0,
    errors
  };
}

/**
 * Merge configuration with defaults
 */
export function mergeConfigDefaults(config) {
  const merged = { ...config };

  // Merge database defaults
  merged.database = {
    ...DATABASE_CONFIG,
    ...config.database
  };

  // Add extraction config defaults
  merged.extraction = {
    ...EXTRACTION_CONFIG,
    ...config.extraction
  };

  // Add ZIP config defaults
  merged.zip = {
    ...ZIP_CONFIG,
    ...config.zip
  };

  // Ensure sources have default values
  if (merged.sources) {
    merged.sources = merged.sources.map(source => ({
      ...source,
      staging_dir: source.staging_dir || `./data/staging/${source.id}`,
      ndjson_output: source.ndjson_output || `./data/processed/${source.id}-statutes.ndjson`,
      zip_dir: source.zip_dir || `./data/zips/${source.id}`
    }));
  }

  return merged;
}

/**
 * Get configuration summary for logging
 */
export function getConfigSummary(config) {
  return {
    database: `${config.database.host}:${config.database.port}/${config.database.database}`,
    sources: config.sources.map(s => ({
      id: s.id,
      label: s.label,
      instrument_kind: s.instrument_kind,
      jurisdiction_scope: s.jurisdiction_scope,
      zip_urls_count: s.zip_urls?.length || 0
    })),
    xml_structure: config.xml_structure ? {
      root_element: config.xml_structure.root_element,
      hierarchy_levels: config.xml_structure.hierarchy?.length || 0
    } : null
  };
}
