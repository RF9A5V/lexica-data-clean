#!/usr/bin/env node

/**
 * Debug script to examine XML file structure
 */

import fs from 'fs/promises';
import { XMLParser } from 'fast-xml-parser';

async function debugXmlFile(filePath) {
  try {
    console.log(`\n=== Examining: ${filePath} ===`);

    // Read file
    const xmlContent = await fs.readFile(filePath, 'utf-8');
    console.log(`File size: ${xmlContent.length} characters`);

    // Show first 500 characters
    console.log(`\nFirst 500 characters:`);
    console.log(xmlContent.substring(0, 500));
    console.log('...');

    // Parse XML
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '@_',
      allowBooleanAttributes: true,
      parseAttributeValue: true,
      trimValues: true
    });

    const parsedXml = parser.parse(xmlContent);
    console.log(`\nParsed XML keys at root level:`);
    console.log(Object.keys(parsedXml));

    // Show structure recursively
    function showStructure(obj, depth = 0, maxDepth = 3) {
      if (depth > maxDepth) return;

      const indent = '  '.repeat(depth);
      for (const [key, value] of Object.entries(obj)) {
        if (typeof value === 'object' && value !== null) {
          console.log(`${indent}${key}:`);
          if (Array.isArray(value)) {
            console.log(`${indent}  [Array with ${value.length} items]`);
            if (value.length > 0 && depth < maxDepth) {
              showStructure(value[0], depth + 1, maxDepth);
            }
          } else {
            showStructure(value, depth + 1, maxDepth);
          }
        } else {
          console.log(`${indent}${key}: ${typeof value} = ${JSON.stringify(value)}`);
        }
      }
    }

    console.log(`\nXML Structure (max depth 3):`);
    showStructure(parsedXml);

  } catch (error) {
    console.error(`Error examining ${filePath}:`, error.message);
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    console.log('Usage: node debug-xml.js <xml-file>');
    process.exit(1);
  }

  for (const filePath of args) {
    await debugXmlFile(filePath);
  }
}

main().catch(console.error);
