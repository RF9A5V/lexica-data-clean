#!/usr/bin/env node
import 'dotenv/config';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { appendLine, ensureDir, getIn } from './lib/utils.js';
import { NysenateClient } from './client/nysenate.js';
import { mapNodeToUnit, unitIdFor } from './transform/nysenate.js';
import { parseSubsections, splitCaptionAndBody, classifyLevel } from './transform/subsection_parser.js';
import { ReconstitutionTester } from './test/reconstitution-test.js';
import { loadNdjsonToDatabase } from './db/loader.js';
import { SectionCache } from './cache/section-cache.js';
import { TextParser } from './parser/text-parser.js';

const args = parseArgs(process.argv.slice(2));

async function main() {
  const cmd = args._[0];
  if (!cmd || cmd === 'help' || cmd === '--help' || cmd === '-h') return usage();

  switch (cmd) {
    case 'cache':
      return cacheAllSections();
    case 'fetch':
      return fetchAll();
    case 'validate':
      return validateFile(args.file || args.f || 'data/nysenate.ndjson');
    case 'load':
      return loadToDatabase();
    case 'test-parser':
      return testParser();
    case 'cache-stats':
      return showCacheStats();
    case 'test-reconstitution':
      return testReconstitution();
    default:
      console.error(`Unknown command: ${cmd}`);
      return usage(1);
  }
}

async function cacheAllSections() {
  const cacheDir = args['cache-dir'] || 'data/cache';
  const dryRun = !!args['dry-run'];
  const listAll = !!args['all-laws'];
  const lawArgs = [].concat(args.law || []).flat().filter(Boolean);
  const lawTypeArgs = [].concat(args['law-type'] || []).flat().filter(Boolean);

  const config = await loadConfig();
  const client = new NysenateClient(config);
  const cache = new SectionCache(cacheDir);

  // Determine laws to process
  let laws = await determineLawsToProcess(client, config, lawArgs, lawTypeArgs, listAll);

  console.log(`Caching sections for ${laws.length} law(s)...`);

  for (const lawId of laws) {
    console.log(`\nCaching sections for law ${lawId}...`);
    
    if (await cache.hasCachedSections(lawId)) {
      console.log(`  ⚠️  Sections already cached for ${lawId}. Use --force to overwrite.`);
      if (!args.force) continue;
    }

    try {
      // Fetch full law tree with text
      const json = await client.getLawTree(lawId, { full: true });
      const root = getIn(json, ['result', 'documents'], null);
      
      if (!root) {
        console.warn(`  ⚠️  No law tree documents found for ${lawId}`);
        continue;
      }

      // Flatten sections with text
      const sections = [];
      function collectSections(node, path = []) {
        if (!node || typeof node !== 'object') return;
        
        // Add current node if it has text content
        const text = getIn(node, ['text'], '');
        if (text && text.trim()) {
          sections.push({
            docType: node.docType || node.type,
            docId: node.locationId || node.docId || node.id || node.number,
            text: text.trim(),
            path: path.map(p => ({
              docType: p.docType || p.type,
              docId: p.locationId || p.docId || p.id || p.number
            }))
          });
        }
        
        // Recurse into children
        const kids = getIn(node, ['documents', 'items'], []);
        if (Array.isArray(kids)) {
          for (const kid of kids) {
            collectSections(kid, [...path, node]);
          }
        }
      }

      collectSections(root);

      if (!dryRun) {
        await cache.saveSectionsToCache(lawId, sections, {
          fetchedAt: new Date().toISOString(),
          apiResponse: { hasFullText: true }
        });
      } else {
        console.log(`  📊 Would cache ${sections.length} sections for ${lawId}`);
      }
    } catch (error) {
      console.error(`  ❌ Error caching sections for ${lawId}: ${error.message}`);
    }
  }

  console.log('\n✅ Section caching complete');
}

async function fetchAll() {
  const baseOutDir = args['out-dir'] || 'data/nysenate';
  const cacheDir = args['cache-dir'] || 'data/cache';
  const dryRun = !!args['dry-run'];
  const listAll = !!args['all-laws'];
  const lawArgs = [].concat(args.law || []).flat().filter(Boolean);
  const lawTypeArgs = [].concat(args['law-type'] || []).flat().filter(Boolean);
  const splitSubsections = !!args['split-subsections'];

  await ensureDir(baseOutDir);

  const config = await loadConfig();
  const client = new NysenateClient(config);
  const cache = new SectionCache(cacheDir);
  const sourceId = config.source_id || 'nysenate';

  // Determine laws to process - now use cache instead of checkpoints
  let laws = await determineLawsToProcess(client, config, lawArgs, lawTypeArgs, listAll);

  const completed = new Set();
  const todo = laws.filter((law) => !completed.has(law));
  console.log(`Processing ${todo.length} law(s)...`);

  for (const lawId of todo) {
    console.log(`\nLaw ${lawId} ...`);
    const lawDir = path.join(baseOutDir, String(lawId).toLowerCase());
    await ensureDir(lawDir);
    const outFile = path.join(lawDir, `nysenate.${lawId}.ndjson`);

    // Check if we have cached sections for this law
    if (await cache.hasCachedSections(lawId)) {
      console.log(`  📂 Using cached sections for ${lawId}`);
      await processLawFromCache({ cache, lawId, outFile, sourceId, mapping: config.mapping || {}, dryRun, splitSubsections });
    } else {
      console.log(`  ⚠️  No cached sections found for ${lawId}. Run 'cache' command first.`);
      console.log(`  📡 Falling back to live API fetch...`);
      await processLawViaLawTreeFull({ client, lawId, outFile, sourceId, mapping: config.mapping || {}, dryRun, splitSubsections });
    }

    completed.add(lawId);
  }

  console.log(`\nDone.`);
}

async function validateFile(file) {
  try {
    const fh = await fs.open(file, 'r');
    let count = 0;
    for await (const line of fh.readLines()) {
      if (!line.trim()) continue;
      JSON.parse(line);
      count++;
    }
    console.log(`Validated ${count} NDJSON lines in ${file}`);
  } catch (e) {
    console.error(`Validation failed: ${e.message}`);
    process.exitCode = 1;
  }
}

async function loadCheckpoint(file, defaults) {
  try {
    const text = await fs.readFile(file, 'utf8');
    const json = JSON.parse(text);
    return { outFile: defaults.outFile, offsets: {}, lawsCompleted: [], ...json };
  } catch {
    return { outFile: defaults.outFile, offsets: {}, lawsCompleted: [], currentLaw: null, lastRun: new Date().toISOString() };
  }
}

async function saveCheckpoint(file, ck) {
  ck.lastRun = new Date().toISOString();
  await fs.writeFile(file, JSON.stringify(ck, null, 2) + '\n', 'utf8');
}

function usage(code = 0) {
  console.log(`Usage:
  node src/index.js cache [--all-laws | --law-type TYPE | --law LAWID ...] [--cache-dir DIR] [--dry-run]
  node src/index.js fetch [--all-laws | --law-type TYPE | --law LAWID ...] [--out-dir DIR] [--cache-dir DIR] [--split-subsections] [--dry-run]
  node src/index.js validate --file <ndjson-file>
  node src/index.js load [--law LAWID ...] [--out-dir DIR] [--dry-run]
  node src/index.js test-parser [--law LAWID] [--cache-dir DIR]
  node src/index.js test-reconstitution [--law LAWID] [--section SECTION] [--data-dir DIR] [--cache-dir DIR]
  node src/index.js cache-stats [--cache-dir DIR]
  `);
  process.exitCode = code;
}

async function processLawViaSections({ client, lawId, outFile, checkpointFile, ck, sourceId, mapping, dryRun, since, size }) {
  ck.offsets[lawId] ||= { page: 1 };
  let page = ck.offsets[lawId].page || 1;
  size = Math.max(10, Math.min(200, parseInt(size || 100)));
  let totalPages = undefined;

  while (true) {
    console.log(`  Page ${page}${totalPages ? '/' + totalPages : ''}`);
    const { items, totalPages: tp } = await client.listSections(lawId, { page, size, modifiedSince: since });
    if (totalPages === undefined && typeof tp === 'number') totalPages = tp;
    if (!items || items.length === 0) break;

    for (const it of items) {
      const recs = mapSectionToRecords({ sourceId, lawId, section: it, mapping });
      for (const r of recs) {
        if (dryRun) continue;
        await appendLine(outFile, JSON.stringify(r));
      }
    }

    page += 1;
    ck.offsets[lawId].page = page;
    await saveCheckpoint(checkpointFile, ck);
    if (totalPages && page > totalPages) break;
  }
}

async function processLawViaLawTreeFull({ client, lawId, outFile, checkpointFile, ck, sourceId, mapping, dryRun, splitSubsections }) {
  // Fetch full law tree with text
  const json = await client.getLawTree(lawId, { full: true });
  const root = getIn(json, ['result', 'documents'], null);
  if (!root) {
    console.warn('  No law tree documents in response.');
    return;
  }

  // Flatten with parent path
  const flat = [];
  function visit(node, path) {
    if (!node || typeof node !== 'object') return;
    flat.push({ node, path });
    const kids = getIn(node, ['documents', 'items'], []);
    if (Array.isArray(kids)) {
      for (const k of kids) visit(k, [...path, node]);
    }
  }

  // Start from the chapter root
  visit(root, []);

  // Emit top-level law unit once
  const lawUnit = {
    id: `${sourceId}:${String(lawId).toLowerCase()}`,
    type: 'title',
    number: lawId,
    label: lawId,
    parent_id: null,
    sort_key: lawId,
    citation: lawId,
    canonical_id: `${sourceId}:${lawId}`,
    source_id: sourceId
  };
  if (!dryRun) await appendLine(outFile, JSON.stringify(lawUnit));

  // Resume support via index
  ck.treeFull = ck.treeFull || {};
  ck.treeFull[lawId] = ck.treeFull[lawId] || { index: 0, total: flat.length };
  const state = ck.treeFull[lawId];

  for (let i = state.index; i < flat.length; i++) {
    const { node, path } = flat[i];
    const docType = String(node.docType || node.type || '').toUpperCase();
    if (!docType) { state.index = i + 1; await saveCheckpoint(checkpointFile, ck); continue; }

    const parentNode = path.length === 0 ? null : path[path.length - 1];
    const parentId = parentNode
      ? unitIdFor({ sourceId, lawId, docType: String(parentNode.docType || parentNode.type), docId: String(parentNode.locationId || parentNode.docId || parentNode.id || parentNode.number) })
      : `${sourceId}:${String(lawId).toLowerCase()}`;

    const unit = mapNodeToUnit({ sourceId, lawId, node, parentId, mapping });

    if (splitSubsections && unit.type === 'section' && unit.text) {
      // Separate caption from body before parsing subsections
      const { caption, body } = splitCaptionAndBody(unit.text);
      const subs = parseSubsections(body);
      if (subs.length > 0) {
        const { text, ...unitNoText } = unit; // remove original text from section
        if (caption && caption.length > 0) unitNoText.text = caption; // preserve caption only
        if (!dryRun) await appendLine(outFile, JSON.stringify(unitNoText));
        for (let subIndex = 0; subIndex < subs.length; subIndex++) {
          const s = subs[subIndex];
          const ut = classifyLevel(s.level);
          if (!ut || !s.text) continue;
          
          const markerPath = String(s.marker || '').split('.').filter(Boolean);
          
          // Generate fallback identifier for subdivisions without proper markers
          let docId, number, label, sortKey, citation, canonicalId;
          
          if (markerPath.length === 0) {
            // Use fallback identifiers instead of dropping the subdivision
            const fallbackId = `${s.level}-${subIndex + 1}`;
            docId = fallbackId;
            number = `${subIndex + 1}`;
            label = `${ut} ${subIndex + 1}`;
            sortKey = `${unit.sort_key}.${String(subIndex + 1).padStart(3, '0')}`;
            citation = unit.citation ? `${unit.citation}(${ut}-${subIndex + 1})` : null;
            canonicalId = unit.canonical_id ? `${unit.canonical_id}(${ut}-${subIndex + 1})` : null;
          } else {
            // Use parsed markers - join with dashes for docId to create proper hierarchical IDs
            docId = markerPath.join('-');
            number = markerPath[markerPath.length - 1];
            label = markerPath[markerPath.length - 1];
            sortKey = `${unit.sort_key}.${markerPath.map(p => p.padStart(3, '0')).join('.')}`;
            citation = unit.citation ? `${unit.citation}(${markerPath.join('.')})` : null;
            canonicalId = unit.canonical_id ? `${unit.canonical_id}(${markerPath.join('.')})` : null;
          }
          
          const childId = unitIdFor({ sourceId, lawId, docType: ut.toUpperCase(), docId });
          const child = {
            id: childId,
            type: ut,
            number,
            label,
            parent_id: unit.id,
            sort_key: sortKey,
            citation,
            canonical_id: canonicalId,
            source_id: sourceId,
            effective_start: '1900-01-01',
            effective_end: null,
            text: s.text
          };
          if (!dryRun) await appendLine(outFile, JSON.stringify(child));
        }
      } else {
        if (!dryRun) await appendLine(outFile, JSON.stringify(unit));
      }
    } else {
      if (!dryRun) await appendLine(outFile, JSON.stringify(unit));
    }

    state.index = i + 1;
    await saveCheckpoint(checkpointFile, ck);
  }
}

async function loadConfig() {
  const here = path.dirname(fileURLToPath(import.meta.url));
  const cfgPath = path.resolve(here, '../configs/nysenate.json');
  const text = await fs.readFile(cfgPath, 'utf8');
  return JSON.parse(text);
}

function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const [k, v] = a.includes('=') ? a.slice(2).split('=') : [a.slice(2), argv[i + 1]?.startsWith('-') ? undefined : argv[++i]];
      if (k === 'law' || k === 'law-type') { out[k] ||= []; out[k].push(v); } else { out[k] = v ?? true; }
    } else if (a.startsWith('-')) {
      const k = a.slice(1);
      const v = argv[i + 1]?.startsWith('-') ? undefined : argv[++i];
      out[k] = v ?? true;
    } else {
      out._.push(a);
    }
  }
  return out;
}

async function loadToDatabase() {
  const config = await loadConfig();
  const dryRun = !!args['dry-run'];
  const verbose = !!args.verbose || !!args.v;
  const lawArgs = [].concat(args.law || []).flat().filter(Boolean);
  const cacheDir = args['cache-dir'] || 'data/cache';
  
  if (!config.database) {
    console.error('No database configuration found in config file');
    process.exit(1);
  }

  // Determine which NDJSON files to load
  const baseOutDir = args['out-dir'] || 'data/nysenate';
  const files = [];
  
  if (lawArgs.length > 0) {
    // Load specific laws
    for (const law of lawArgs) {
      const ndjsonFile = path.join(baseOutDir, law.toLowerCase(), `nysenate.${law.toUpperCase()}.ndjson`);
      try {
        await fs.access(ndjsonFile);
        files.push(ndjsonFile);
      } catch (error) {
        console.warn(`NDJSON file not found: ${ndjsonFile}`);
      }
    }
  } else {
    // Load all NDJSON files
    try {
      const dirs = await fs.readdir(baseOutDir);
      for (const dir of dirs) {
        const dirPath = path.join(baseOutDir, dir);
        const stat = await fs.stat(dirPath);
        if (stat.isDirectory()) {
          const ndjsonFile = path.join(dirPath, `nysenate.${dir.toUpperCase()}.ndjson`);
          try {
            await fs.access(ndjsonFile);
            files.push(ndjsonFile);
          } catch (error) {
            // Skip if file doesn't exist
          }
        }
      }
    } catch (error) {
      console.error(`Error reading output directory: ${error.message}`);
      process.exit(1);
    }
  }

  if (files.length === 0) {
    console.log('No NDJSON files found to load');
    return;
  }

  console.log(`Loading ${files.length} NDJSON file(s) to database...`);
  
  let totalResults = { insertedUnits: 0, insertedVersions: 0, insertedCitations: 0 };
  let schemaCreated = false;
  
  for (const file of files) {
    console.log(`\nLoading ${path.basename(file)}...`);
    
    try {
      const results = await loadNdjsonToDatabase(file, config.database, { 
        verbose, 
        dryRun, 
        skipSchemaCreation: schemaCreated,
        cacheDir 
      });
      
      // Mark schema as created after first successful load
      if (!schemaCreated && !dryRun) {
        schemaCreated = true;
      }
      
      totalResults.insertedUnits += results.insertedUnits;
      totalResults.insertedVersions += results.insertedVersions;
      totalResults.insertedCitations += results.insertedCitations;
      totalResults.reconstitutionFallbacks = (totalResults.reconstitutionFallbacks || 0) + (results.reconstitutionFallbacks || 0);
      
      console.log(`  ✅ Loaded ${results.insertedUnits} units, ${results.insertedVersions} text versions, ${results.insertedCitations} citations`);
    } catch (error) {
      console.error(`  ❌ Error loading ${file}: ${error.message}`);
      if (verbose) {
        console.error(error.stack);
      }
    }
  }
  
  console.log(`\nTotal: ${totalResults.insertedUnits} units, ${totalResults.insertedVersions} text versions, ${totalResults.insertedCitations} citations loaded successfully`);
  if (totalResults.reconstitutionFallbacks > 0) {
    console.log(`Used cached text fallback for ${totalResults.reconstitutionFallbacks} sections with reconstitution mismatches`);
  }
}

// Helper function to determine laws to process
async function determineLawsToProcess(client, config, lawArgs, lawTypeArgs, listAll) {
  let laws = lawArgs;
  if (listAll || laws.length === 0 || lawTypeArgs.length > 0) {
    console.log('Listing laws...');
    const items = await client.listLaws();
    if (lawTypeArgs.length > 0) {
      const typePaths = (config.mapping?.lawTypeField) || [];
      const typesAvailable = new Set(
        items.map((it) => pickAny(it, typePaths)).filter(Boolean).map((t) => String(t).toUpperCase())
      );
      if (typesAvailable.size === 0) {
        throw new Error('API did not provide law types; cannot filter by --law-type');
      }
      const bad = lawTypeArgs.filter((t) => !typesAvailable.has(String(t).toUpperCase()));
      if (bad.length > 0) {
        throw new Error(`Unknown --law-type: ${bad.join(', ')}. Available types: ${Array.from(typesAvailable).join(', ')}`);
      }
      const allowed = new Set(lawTypeArgs.map((t) => String(t).toUpperCase()));
      const filtered = items.filter((it) => allowed.has(String(pickAny(it, typePaths)).toUpperCase()));
      laws = filtered.map((it) => it.lawId || it.id || it.code || it.name).filter(Boolean);
    } else {
      laws = items.map((it) => it.lawId || it.id || it.code || it.name).filter(Boolean);
    }
  }
  return laws;
}

// Process law from cached sections
async function processLawFromCache({ cache, lawId, outFile, sourceId, mapping, dryRun, splitSubsections }) {
  try {
    const cachedData = await cache.loadCachedSections(lawId);
    const sections = cachedData.sections;

    // Emit top-level law unit
    const lawUnit = {
      id: `${sourceId}:${String(lawId).toLowerCase()}`,
      type: 'title',
      number: lawId,
      label: lawId,
      parent_id: null,
      sort_key: lawId,
      citation: lawId,
      canonical_id: `${sourceId}:${lawId}`,
      source_id: sourceId
    };
    if (!dryRun) await appendLine(outFile, JSON.stringify(lawUnit));

    console.log(`  📊 Processing ${sections.length} cached sections`);

    for (const section of sections) {
      // Convert cached section to unit format
      const parentId = section.path.length > 0 
        ? unitIdFor({ 
            sourceId, 
            lawId, 
            docType: String(section.path[section.path.length - 1].docType || '').toUpperCase(), 
            docId: String(section.path[section.path.length - 1].docId || '') 
          })
        : `${sourceId}:${String(lawId).toLowerCase()}`;

      const unit = {
        id: unitIdFor({ sourceId, lawId, docType: String(section.docType || '').toUpperCase(), docId: String(section.docId || '') }),
        type: mapDocTypeToUnitType(section.docType),
        number: section.docId,
        label: section.docId,
        parent_id: parentId,
        sort_key: generateSortKey(lawId, section),
        citation: generateCitation(lawId, section),
        canonical_id: generateCanonicalId(sourceId, lawId, section),
        source_id: sourceId,
        effective_start: '1900-01-01',
        effective_end: null,
        text: section.text
      };

      if (splitSubsections && unit.type === 'section' && unit.text) {
        // Use new parser for subsection splitting
        const parser = new TextParser();
        const { caption, body } = splitCaptionAndBody(unit.text || '');
        
        try {
          const tokens = parseSubsections(body, unit.number);
          
          if (tokens.length > 0) {
            // Section with parsed subsections
            const { text, ...unitNoText } = unit;
            
            // Create interpolatable tokens for child subdivisions
            const childTokens = tokens.map(token => {
              const tokenType = (token.type || 'unknown').toUpperCase();
              return `{{${tokenType}_${unit.number}.${token.marker}}}`;
            });
            
            // Combine caption with child tokens
            let sectionText = caption && caption.length > 0 ? caption : '';
            if (childTokens.length > 0) {
              if (sectionText) sectionText += '\n';
              sectionText += childTokens.join('\n');
            }
            
            unitNoText.text = sectionText;
            if (!dryRun) await appendLine(outFile, JSON.stringify(unitNoText));

            // Process each token as a subsection
            for (const token of tokens) {
              // Skip tokens with undefined type or marker
              if (!token.type || !token.marker) {
                console.warn(`  ⚠️  Skipping malformed token in section ${unit.number}: type=${token.type}, marker=${token.marker}`);
                continue;
              }
              
              const childUnit = {
                id: `${unit.id}_${token.type}_${token.marker}`,
                type: token.type,
                number: token.marker,
                label: token.marker,
                parent_id: unit.id,
                sort_key: `${unit.sort_key}.${token.marker}`,
                citation: unit.citation ? `${unit.citation}(${token.marker})` : null,
                canonical_id: unit.canonical_id ? `${unit.canonical_id}(${token.marker})` : null,
                source_id: sourceId,
                effective_start: '1900-01-01',
                effective_end: null,
                text: token.text
              };
              if (!dryRun) await appendLine(outFile, JSON.stringify(childUnit));
            }
          } else {
            if (!dryRun) await appendLine(outFile, JSON.stringify(unit));
          }
        } catch (error) {
          console.warn(`  ⚠️  Parser error for section ${section.docId}: ${error.message}`);
          if (!dryRun) await appendLine(outFile, JSON.stringify(unit));
        }
      } else {
        if (!dryRun) await appendLine(outFile, JSON.stringify(unit));
      }
    }
  } catch (error) {
    throw new Error(`Failed to process cached sections for ${lawId}: ${error.message}`);
  }
}

// Test parser functionality
async function testParser() {
  const cacheDir = args['cache-dir'] || 'data/cache';
  const lawId = args.law || 'PEN'; // Default to Penal Law
  
  const cache = new SectionCache(cacheDir);
  const parser = new TextParser();
  const interpolator = new TokenInterpolator();

  console.log(`🧪 Testing parser with law ${lawId}...`);

  if (!(await cache.hasCachedSections(lawId))) {
    console.error(`❌ No cached sections found for ${lawId}. Run 'cache' command first.`);
    return;
  }

  const cachedData = await cache.loadCachedSections(lawId);
  const sections = cachedData.sections.slice(0, 5); // Test first 5 sections

  let totalSections = 0;
  let parsedSections = 0;
  let roundTripSuccesses = 0;

  for (const section of sections) {
    totalSections++;
    console.log(`\n📄 Section ${section.docId} (${section.docType})`);
    console.log(`   Text length: ${section.text.length} chars`);

    try {
      // Test tokenization
      const tokens = parser.tokenizeText(section.text);
      if (tokens.length > 0) {
        parsedSections++;
        console.log(`   ✅ Parsed ${tokens.length} hierarchical elements`);

        // Test round-trip
        const reinterpolated = interpolator.reinterpolateTokens(section.text, tokens);
        if (reinterpolated === section.text) {
          roundTripSuccesses++;
          console.log(`   ✅ Round-trip successful`);
        } else {
          console.log(`   ⚠️  Round-trip mismatch (${reinterpolated.length} vs ${section.text.length} chars)`);
        }
      } else {
        console.log(`   ℹ️  No hierarchical elements found`);
      }
    } catch (error) {
      console.log(`   ❌ Parser error: ${error.message}`);
    }
  }

  console.log(`\n📊 Test Results:`);
  console.log(`   Total sections tested: ${totalSections}`);
  console.log(`   Sections with hierarchical elements: ${parsedSections}`);
  console.log(`   Round-trip successes: ${roundTripSuccesses}`);
  console.log(`   Success rate: ${totalSections > 0 ? Math.round((roundTripSuccesses / totalSections) * 100) : 0}%`);
}

// Show cache statistics
async function showCacheStats() {
  const cacheDir = args['cache-dir'] || 'data/cache';
  const cache = new SectionCache(cacheDir);

  console.log('📊 Cache Statistics\n');
  const stats = await cache.getCacheStats();

  if (stats.error) {
    console.error(`❌ Error reading cache: ${stats.error}`);
    return;
  }

  console.log(`Total cached laws: ${stats.totalCachedLaws}`);
  
  if (stats.laws.length > 0) {
    console.log('\nCached laws:');
    for (const law of stats.laws) {
      console.log(`  ${law.lawId}: ${law.sectionsCount} sections (cached ${law.cachedAt})`);
    }
  } else {
    console.log('\nNo laws cached yet. Run the "cache" command to populate the cache.');
  }
}

// Helper functions for unit mapping
function mapDocTypeToUnitType(docType) {
  const type = String(docType || '').toLowerCase();
  const mapping = {
    'chapter': 'chapter',
    'title': 'title', 
    'article': 'article',
    'part': 'part',
    'section': 'section',
    'subsection': 'subsection',
    'paragraph': 'paragraph',
    'subparagraph': 'subparagraph',
    'clause': 'clause',
    'item': 'item'
  };
  return mapping[type] || 'section';
}

function generateSortKey(lawId, section) {
  return `${lawId}.${section.docId || '0'}`;
}

function generateCitation(lawId, section) {
  return `${lawId} § ${section.docId}`;
}

function generateCanonicalId(sourceId, lawId, section) {
  return `${sourceId}:${lawId}:${section.docId}`;
}

async function testReconstitution() {
  const lawCode = String(Array.isArray(args.law) ? args.law[0] : args.law || 'ABC');
  const sectionNumber = args.section ? String(args.section) : null;
  const dataDir = args['data-dir'] || 'data';
  const cacheDir = args['cache-dir'] || 'data/cache';
  
  const ndjsonPath = path.join(dataDir, 'nysenate', lawCode.toLowerCase(), `nysenate.${lawCode}.ndjson`);
  
  if (!await fs.access(ndjsonPath).then(() => true).catch(() => false)) {
    console.error(`❌ NDJSON file not found: ${ndjsonPath}`);
    console.error(`Run 'fetch --law ${lawCode} --split-subsections' first`);
    process.exitCode = 1;
    return;
  }
  
  const tester = new ReconstitutionTester();
  const results = await tester.runTests(ndjsonPath, cacheDir, lawCode, sectionNumber);
  
  if (!results.success) {
    console.error(`\n❌ Reconstitution test failed for ${lawCode}${sectionNumber ? ` section ${sectionNumber}` : ''}`);
    process.exitCode = 1;
  } else {
    console.log(`\n✅ All reconstitution tests passed for ${lawCode}${sectionNumber ? ` section ${sectionNumber}` : ''}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});

function pickAny(obj, paths) {
  for (const p of paths) {
    if (Array.isArray(p)) {
      const v = getIn(obj, p, undefined);
      if (v !== undefined && v !== null) return v;
    } else if (p in obj) {
      return obj[p];
    }
  }
  return undefined;
}
