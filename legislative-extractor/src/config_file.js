/**
 * Configuration file loading and parsing utilities
 */

import fs from 'fs/promises';
import path from 'path';
import { validateConfig, mergeConfigDefaults } from './config.js';

/**
 * Load and parse a JSON configuration file
 */
export async function loadConfig(configPath) {
  try {
    // Resolve path relative to current working directory
    const resolvedPath = path.resolve(process.cwd(), configPath);

    // Read and parse JSON file
    const configContent = await fs.readFile(resolvedPath, 'utf-8');
    const config = JSON.parse(configContent);

    // Validate configuration
    const validation = validateConfig(config);
    if (!validation.isValid) {
      throw new Error(`Configuration validation failed:\n${validation.errors.map(e => `  • ${e}`).join('\n')}`);
    }

    // Merge with defaults
    const mergedConfig = mergeConfigDefaults(config);

    return mergedConfig;
  } catch (error) {
    if (error.code === 'ENOENT') {
      throw new Error(`Configuration file not found: ${configPath}`);
    } else if (error instanceof SyntaxError) {
      throw new Error(`Invalid JSON in configuration file: ${error.message}`);
    } else {
      throw error;
    }
  }
}

/**
 * Load configuration from a string (useful for testing)
 */
export function loadConfigFromString(configString) {
  try {
    const config = JSON.parse(configString);

    const validation = validateConfig(config);
    if (!validation.isValid) {
      throw new Error(`Configuration validation failed:\n${validation.errors.map(e => `  • ${e}`).join('\n')}`);
    }

    return mergeConfigDefaults(config);
  } catch (error) {
    if (error instanceof SyntaxError) {
      throw new Error(`Invalid JSON: ${error.message}`);
    } else {
      throw error;
    }
  }
}

/**
 * Save configuration to a file
 */
export async function saveConfig(configPath, config) {
  try {
    const resolvedPath = path.resolve(process.cwd(), configPath);
    const configString = JSON.stringify(config, null, 2);
    await fs.writeFile(resolvedPath, configString, 'utf-8');
    return resolvedPath;
  } catch (error) {
    throw new Error(`Failed to save configuration: ${error.message}`);
  }
}

/**
 * Create default configuration for a legislative source
 */
export function createDefaultConfig(sourceId, sourceLabel, instrumentKind, jurisdictionScope) {
  return {
    database: {
      host: 'localhost',
      port: 5432,
      database: `${sourceId}_legislative`,
      user: 'dev',
      password: 'dev'
    },
    metadata: {
      jurisdiction: jurisdictionScope === 'city' ? 'New York City' :
                   jurisdictionScope === 'state' ? 'New York State' :
                   jurisdictionScope === 'federal' ? 'United States' : 'Unknown',
      publisher: 'Unknown',
      current_edition_date: new Date().toISOString().split('T')[0]
    },
    sources: [
      {
        id: sourceId,
        label: sourceLabel,
        instrument_kind: instrumentKind,
        code_key: sourceId,
        jurisdiction_scope: jurisdictionScope,
        zip_urls: [],
        staging_dir: `./data/staging/${sourceId}`,
        ndjson_output: `./data/processed/${sourceId}-statutes.ndjson`,
        zip_dir: `./data/zips/${sourceId}`
      }
    ],
    xml_structure: {
      root_element: 'Statute',
      hierarchy: [
        { level: 'title', xpath: 'Title', fields: ['number', 'title'] },
        { level: 'chapter', xpath: 'Chapter', fields: ['number', 'title'] },
        { level: 'section', xpath: 'Section', fields: ['number', 'title', 'text'] }
      ]
    }
  };
}

/**
 * Validate and normalize file paths in configuration
 */
export function normalizeConfigPaths(config, baseDir = process.cwd()) {
  const normalized = { ...config };

  if (normalized.sources) {
    normalized.sources = normalized.sources.map(source => ({
      ...source,
      staging_dir: source.staging_dir ? path.resolve(baseDir, source.staging_dir) : source.staging_dir,
      ndjson_output: source.ndjson_output ? path.resolve(baseDir, source.ndjson_output) : source.ndjson_output,
      zip_dir: source.zip_dir ? path.resolve(baseDir, source.zip_dir) : source.zip_dir
    }));
  }

  return normalized;
}

/**
 * Get list of all configuration files in configs directory
 */
export async function listConfigFiles(configsDir = './configs') {
  try {
    const resolvedDir = path.resolve(process.cwd(), configsDir);
    const files = await fs.readdir(resolvedDir);
    return files.filter(file => file.endsWith('.json'));
  } catch (error) {
    if (error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
}

/**
 * Load all configurations from configs directory
 */
export async function loadAllConfigs(configsDir = './configs') {
  const configFiles = await listConfigFiles(configsDir);
  const configs = [];

  for (const file of configFiles) {
    try {
      const configPath = path.join(configsDir, file);
      const config = await loadConfig(configPath);
      configs.push({
        file,
        path: configPath,
        config
      });
    } catch (error) {
      console.warn(`Failed to load config ${file}: ${error.message}`);
    }
  }

  return configs;
}

/**
 * Validate configuration compatibility across sources
 */
export function validateConfigCompatibility(configs) {
  const errors = [];
  const codeKeys = new Set();
  const sourceIds = new Set();

  for (const { file, config } of configs) {
    // Check for duplicate code keys
    for (const source of config.sources) {
      if (codeKeys.has(source.code_key)) {
        errors.push(`Duplicate code_key '${source.code_key}' found in ${file}`);
      }
      codeKeys.add(source.code_key);

      if (sourceIds.has(source.id)) {
        errors.push(`Duplicate source id '${source.id}' found in ${file}`);
      }
      sourceIds.add(source.id);
    }
  }

  return {
    isValid: errors.length === 0,
    errors
  };
}
