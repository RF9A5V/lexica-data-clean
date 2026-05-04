/**
 * Output validator — scans ./output/<volume>/cases.json files for data-quality
 * issues that would either block ingestion (hard) or surface as bad data once
 * loaded (soft).
 *
 * Designed to be re-run after parser changes to catch regressions and to
 * survey the full extracted corpus without re-doing the analysis from scratch.
 *
 * Hard checks (would block ingestion or break invariants):
 *   - missing case_curie           (dedup key is required)
 *   - duplicate case_curie         (within a single volume)
 *   - missing name                 (`cases.name` is NOT NULL in collection DB)
 *   - missing first_page           (cite-page is required)
 *   - empty opinions[]             (every case must have at least one body)
 *
 * Soft checks (will load but indicates parser quality issues):
 *   - missing decision_date
 *   - missing source_url
 *   - caption text contains running-header signature ("<page> <vol> APPELLATE
 *     DIVISION REPORTS, 3d SERIES" or N.Y.3d / Misc 3d variants)
 *   - decision_date not YYYY-MM-DD
 *   - parser_version on volume doesn't match the current parser
 *
 * Usage (as module):
 *   import { validateAllOutputs, validateVolume } from './src/validate.js';
 *   const report = await validateAllOutputs(outputDir, { parserVersion });
 *
 * Usage (as CLI via main.js):
 *   node main.js validate                 # all volumes, summary
 *   node main.js validate --json          # JSON report on stdout
 *   node main.js validate --volume=158AD3d
 *   node main.js validate --strict        # exit 1 if any hard issues
 */

import { readdir, readFile, stat } from 'fs/promises';
import path from 'path';

// Running-header signature that should never appear inside a caption. AD3d:
// "<page> <volume> APPELLATE DIVISION REPORTS, 3d SERIES". NY3d and Misc 3d
// variants follow the same structure with their own reporter text.
const RUNNING_HEAD_RE = /\b\d+\s+\d+\s+(?:APPELLATE\s+DIVISION\s+REPORTS,?\s+3d\s+SERIES|N\.?\s*Y\.?\s*REPORTS,?\s+3d\s+SERIES|MISCELLANEOUS\s+REPORTS,?\s+3d\s+SERIES)\b/i;

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function shortRef(c) {
  return {
    curie: c.case_curie || null,
    citation: c.citation || null,
    first_page: c.first_page ?? null,
    source_url: c.source_url || null,
  };
}

export function validateVolumeData(volumeName, data, opts = {}) {
  const parserVersion = opts.parserVersion || null;
  const cases = Array.isArray(data?.cases) ? data.cases : [];
  const issues = {
    hard: {
      noCurie: [],
      duplicateCurie: [],
      noName: [],
      noFirstPage: [],
      noOpinions: [],
    },
    soft: {
      noDate: [],
      noSourceUrl: [],
      runningHeadInCaption: [],
      badDateFormat: [],
      parserVersionMismatch: false,
    },
  };

  if (parserVersion && data?.parser_version && data.parser_version !== parserVersion) {
    issues.soft.parserVersionMismatch = {
      file_version: data.parser_version,
      current_version: parserVersion,
    };
  }

  const seenCuries = new Map(); // curie → first index
  for (let i = 0; i < cases.length; i++) {
    const c = cases[i];
    const ref = shortRef(c);

    // Hard
    if (!c.case_curie) issues.hard.noCurie.push(ref);
    else if (seenCuries.has(c.case_curie)) {
      issues.hard.duplicateCurie.push({
        ...ref,
        first_index: seenCuries.get(c.case_curie),
        duplicate_index: i,
      });
    } else {
      seenCuries.set(c.case_curie, i);
    }
    if (!c.name || String(c.name).trim() === '') issues.hard.noName.push(ref);
    if (c.first_page == null) issues.hard.noFirstPage.push(ref);
    if (!Array.isArray(c.opinions) || c.opinions.length === 0) issues.hard.noOpinions.push(ref);

    // Soft
    if (!c.decision_date) issues.soft.noDate.push(ref);
    else if (!DATE_RE.test(String(c.decision_date))) {
      issues.soft.badDateFormat.push({ ...ref, decision_date: c.decision_date });
    }
    if (!c.source_url) issues.soft.noSourceUrl.push(ref);
    const captionTexts = [c.caption_text, ...(c.captions || []).map(cap => cap?.name)];
    if (captionTexts.some(t => t && RUNNING_HEAD_RE.test(t))) {
      issues.soft.runningHeadInCaption.push(ref);
    }
  }

  const hardCount = Object.values(issues.hard).reduce((a, v) => a + (Array.isArray(v) ? v.length : 0), 0);
  const softCount = Object.entries(issues.soft).reduce((a, [k, v]) => a + (Array.isArray(v) ? v.length : (v ? 1 : 0)), 0);

  return {
    volume: volumeName,
    parser_version: data?.parser_version || null,
    case_count: cases.length,
    hard_count: hardCount,
    soft_count: softCount,
    issues,
  };
}

export async function validateAllOutputs(outputDir, opts = {}) {
  const dirents = await readdir(outputDir, { withFileTypes: true });
  const volumes = dirents.filter(d => d.isDirectory()).map(d => d.name).sort();
  const filterVolume = opts.volume || null;

  const reports = [];
  for (const v of volumes) {
    if (filterVolume && v !== filterVolume) continue;
    const fp = path.join(outputDir, v, 'cases.json');
    try {
      await stat(fp);
    } catch {
      reports.push({
        volume: v, parser_version: null, case_count: 0,
        hard_count: 1, soft_count: 0,
        issues: { hard: { missingCasesJson: true } },
      });
      continue;
    }
    let data;
    try {
      data = JSON.parse(await readFile(fp, 'utf8'));
    } catch (err) {
      reports.push({
        volume: v, parser_version: null, case_count: 0,
        hard_count: 1, soft_count: 0,
        issues: { hard: { parseError: err.message } },
      });
      continue;
    }
    reports.push(validateVolumeData(v, data, opts));
  }

  // Roll up
  const rollup = {
    volumes_scanned: reports.length,
    volumes_with_hard_issues: reports.filter(r => r.hard_count > 0).length,
    volumes_with_soft_issues: reports.filter(r => r.soft_count > 0).length,
    total_cases: reports.reduce((a, r) => a + r.case_count, 0),
    hard_totals: {
      noCurie: 0, duplicateCurie: 0, noName: 0, noFirstPage: 0, noOpinions: 0,
      missingCasesJson: 0, parseError: 0,
    },
    soft_totals: {
      noDate: 0, noSourceUrl: 0, runningHeadInCaption: 0, badDateFormat: 0,
      parserVersionMismatch: 0,
    },
  };
  for (const r of reports) {
    if (r.issues?.hard?.missingCasesJson) rollup.hard_totals.missingCasesJson++;
    if (r.issues?.hard?.parseError) rollup.hard_totals.parseError++;
    for (const k of Object.keys(rollup.hard_totals)) {
      const v = r.issues?.hard?.[k];
      if (Array.isArray(v)) rollup.hard_totals[k] += v.length;
    }
    for (const k of Object.keys(rollup.soft_totals)) {
      const v = r.issues?.soft?.[k];
      if (Array.isArray(v)) rollup.soft_totals[k] += v.length;
      else if (v && k === 'parserVersionMismatch') rollup.soft_totals.parserVersionMismatch++;
    }
  }

  return { reports, rollup };
}

function formatRefList(refs, max = 5) {
  if (!Array.isArray(refs) || !refs.length) return '';
  const shown = refs.slice(0, max).map(r => {
    const parts = [r.curie || '?'];
    if (r.first_page != null) parts.push(`page=${r.first_page}`);
    if (r.source_url) parts.push(r.source_url);
    return '      ' + parts.join('  ');
  }).join('\n');
  const more = refs.length > max ? `\n      ... +${refs.length - max} more` : '';
  return shown + more;
}

export function printValidationReport({ reports, rollup }, opts = {}) {
  const verbose = !!opts.verbose;
  const showSamples = opts.samples !== false;

  for (const r of reports) {
    if (r.hard_count === 0 && r.soft_count === 0 && !verbose) continue;
    const parts = [`${r.volume}`];
    parts.push(`cases=${r.case_count}`);
    parts.push(`hard=${r.hard_count}`);
    parts.push(`soft=${r.soft_count}`);
    if (r.parser_version) parts.push(`parser=${r.parser_version}`);
    console.log(parts.join('  '));
    if (!showSamples) continue;
    if (r.issues?.hard?.missingCasesJson) console.log('    HARD: cases.json missing');
    if (r.issues?.hard?.parseError) console.log(`    HARD: cases.json parse error: ${r.issues.hard.parseError}`);
    for (const [k, v] of Object.entries(r.issues?.hard || {})) {
      if (Array.isArray(v) && v.length) {
        console.log(`    HARD ${k}: ${v.length}`);
        const samples = formatRefList(v);
        if (samples) console.log(samples);
      }
    }
    for (const [k, v] of Object.entries(r.issues?.soft || {})) {
      if (Array.isArray(v) && v.length) {
        console.log(`    soft ${k}: ${v.length}`);
        if (verbose) {
          const samples = formatRefList(v, 3);
          if (samples) console.log(samples);
        }
      } else if (v && k === 'parserVersionMismatch') {
        console.log(`    soft parserVersionMismatch: file=${v.file_version} current=${v.current_version}`);
      }
    }
  }

  console.log('');
  console.log('== Summary ==');
  console.log(`Volumes scanned        : ${rollup.volumes_scanned}`);
  console.log(`Total cases            : ${rollup.total_cases}`);
  console.log(`Volumes w/ hard issues : ${rollup.volumes_with_hard_issues}`);
  console.log(`Volumes w/ soft issues : ${rollup.volumes_with_soft_issues}`);
  console.log('');
  console.log('Hard totals:');
  for (const [k, v] of Object.entries(rollup.hard_totals)) {
    if (v > 0) console.log(`  ${k.padEnd(22)} ${v}`);
  }
  console.log('Soft totals:');
  for (const [k, v] of Object.entries(rollup.soft_totals)) {
    if (v > 0) console.log(`  ${k.padEnd(22)} ${v}`);
  }
}
