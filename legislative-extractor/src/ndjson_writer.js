/**
 * NDJSON writer utility for legislative data extraction
 * Handles writing structured data to NDJSON format with proper formatting
 */

import fs from 'fs/promises';
import path from 'path';

/**
 * NDJSON Writer class for streaming data to NDJSON files
 */
export class NdjsonWriter {
  constructor(outputPath) {
    this.outputPath = outputPath;
    this.stream = null;
    this.lineCount = 0;
    this.isOpen = false;
  }

  /**
   * Open the NDJSON file for writing
   */
  async open() {
    if (this.isOpen) return;

    // Ensure output directory exists
    await fs.mkdir(path.dirname(this.outputPath), { recursive: true });

    // Create or truncate the output file
    await fs.writeFile(this.outputPath, '');
    this.isOpen = true;
  }

  /**
   * Write a single record to NDJSON
   */
  async write(record) {
    if (!this.isOpen) {
      throw new Error('NDJSON writer is not open. Call open() first.');
    }

    // Validate record structure
    if (!this.isValidRecord(record)) {
      console.warn('Invalid record structure, skipping:', record);
      return;
    }

    // Convert to JSON string with pretty formatting for readability
    const jsonLine = JSON.stringify(record, null, 0) + '\n';

    // Append to file
    await fs.appendFile(this.outputPath, jsonLine);
    this.lineCount++;
  }

  /**
   * Write multiple records to NDJSON
   */
  async writeBatch(records) {
    if (!Array.isArray(records)) {
      throw new Error('Records must be an array');
    }

    const jsonLines = records
      .filter(record => this.isValidRecord(record))
      .map(record => JSON.stringify(record, null, 0))
      .join('\n') + '\n';

    await fs.appendFile(this.outputPath, jsonLines);
    this.lineCount += records.length;
  }

  /**
   * Close the NDJSON writer
   */
  async close() {
    if (!this.isOpen) return;

    this.isOpen = false;
    this.stream = null;
  }

  /**
   * Get current line count
   */
  getLineCount() {
    return this.lineCount;
  }

  /**
   * Get output file path
   */
  getOutputPath() {
    return this.outputPath;
  }

  /**
   * Validate record structure
   */
  isValidRecord(record) {
    if (!record || typeof record !== 'object') {
      return false;
    }

    // Check for required fields based on record type
    if (record.type === 'unit') {
      return record.id && record.type && record.source_id;
    }

    if (record.type === 'citation') {
      return record.id && record.rawText && record.curie;
    }

    // Generic validation for other record types
    return record.id || record.type || record.source_id;
  }

  /**
   * Get file statistics
   */
  async getStats() {
    try {
      const stats = await fs.stat(this.outputPath);
      return {
        fileSize: stats.size,
        lineCount: this.lineCount,
        outputPath: this.outputPath,
        lastModified: stats.mtime
      };
    } catch (error) {
      return {
        fileSize: 0,
        lineCount: this.lineCount,
        outputPath: this.outputPath,
        error: error.message
      };
    }
  }
}

/**
 * Create a simple NDJSON writer function
 */
export async function createNdjsonWriter(outputPath) {
  const writer = new NdjsonWriter(outputPath);
  await writer.open();
  return writer;
}

/**
 * Write records to NDJSON file (convenience function)
 */
export async function writeToNdjson(records, outputPath) {
  const writer = new NdjsonWriter(outputPath);
  await writer.open();

  try {
    if (Array.isArray(records)) {
      await writer.writeBatch(records);
    } else {
      await writer.write(records);
    }

    return await writer.getStats();
  } finally {
    await writer.close();
  }
}

/**
 * Read NDJSON file and parse records
 */
export async function readFromNdjson(inputPath) {
  try {
    const content = await fs.readFile(inputPath, 'utf-8');
    const lines = content.trim().split('\n').filter(line => line.trim());

    const records = [];
    for (const line of lines) {
      try {
        const record = JSON.parse(line);
        records.push(record);
      } catch (error) {
        console.warn(`Error parsing NDJSON line: ${error.message}`);
      }
    }

    return records;
  } catch (error) {
    throw new Error(`Failed to read NDJSON file: ${error.message}`);
  }
}

/**
 * Validate NDJSON file structure
 */
export async function validateNdjsonFile(filePath) {
  try {
    const records = await readFromNdjson(filePath);
    const errors = [];

    // Validate each record
    records.forEach((record, index) => {
      if (!record || typeof record !== 'object') {
        errors.push(`Record ${index}: Invalid object`);
        return;
      }

      // Check for common required fields
      if (!record.id && !record.type) {
        errors.push(`Record ${index}: Missing id or type field`);
      }
    });

    return {
      isValid: errors.length === 0,
      recordCount: records.length,
      errors
    };
  } catch (error) {
    return {
      isValid: false,
      recordCount: 0,
      errors: [error.message]
    };
  }
}

/**
 * Generate summary statistics for NDJSON file
 */
export async function getNdjsonSummary(filePath) {
  try {
    const records = await readFromNdjson(filePath);
    const summary = {
      totalRecords: records.length,
      recordTypes: {},
      sources: new Set(),
      dateRange: { min: null, max: null }
    };

    for (const record of records) {
      // Count record types
      const type = record.type || 'unknown';
      summary.recordTypes[type] = (summary.recordTypes[type] || 0) + 1;

      // Track sources
      if (record.source_id) {
        summary.sources.add(record.source_id);
      }

      // Track date ranges
      if (record.created_at) {
        const date = new Date(record.created_at);
        if (!summary.dateRange.min || date < summary.dateRange.min) {
          summary.dateRange.min = date;
        }
        if (!summary.dateRange.max || date > summary.dateRange.max) {
          summary.dateRange.max = date;
        }
      }
    }

    summary.sources = Array.from(summary.sources);
    return summary;
  } catch (error) {
    throw new Error(`Failed to generate summary: ${error.message}`);
  }
}
