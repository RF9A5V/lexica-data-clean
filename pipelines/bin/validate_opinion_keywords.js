#!/usr/bin/env node

/*
  NY Reporter - Opinion Keyword Validator
  - Fetches random or specific opinions and prints associated keywords for spot checks
  - Filters by opinion_type (defaults to majority,unanimous) and skips valueless opinions

  Usage examples:
    node bin/validate_opinion_keywords.js --db ny_reporter --limit 5
    node bin/validate_opinion_keywords.js --db $DATABASE_URL --case-id 12345
    node bin/validate_opinion_keywords.js --db ny_reporter --opinion-ids 101,202,303 --json
    node bin/validate_opinion_keywords.js --db ny_reporter --opinion-types majority,unanimous --limit 10
*/

const common = require('../src/common');

function parseArgs(argv) {
  const args = {
    limit: 5,
    opinionTypes: ['majority', 'unanimous'],
    json: false,
    random: true,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--db') { args.db = argv[++i]; }
    else if (a === '--limit') { args.limit = parseInt(argv[++i], 10) || 5; }
    else if (a === '--case-id') { args.caseId = parseInt(argv[++i], 10); }
    else if (a === '--opinion-ids') { args.opinionIds = argv[++i].split(',').map(s => parseInt(s.trim(), 10)).filter(n => Number.isFinite(n)); }
    else if (a === '--opinion-types') { args.opinionTypes = argv[++i].split(',').map(s => s.trim()).filter(Boolean); }
    else if (a === '--json') { args.json = true; }
    else if (a === '--no-random') { args.random = false; }
    else if (a === '--help' || a === '-h') { args.help = true; }
  }
  return args;
}

function usage() {
  console.log(`Usage: validate_opinion_keywords --db <DATABASE_URL|DB_NAME> [OPTIONS]\n\nOptions:\n  --limit N                 Number of opinions to sample (default: 5)\n  --case-id ID              Filter by case_id\n  --opinion-ids CSV         Explicit opinion IDs to fetch (comma-separated)\n  --opinion-types CSV       Filter opinion_type values (default: majority,unanimous)\n  --no-random               Do not randomize order (default: random order when not using --opinion-ids)\n  --json                    Output JSON instead of pretty text\n  --help, -h                Show this help\n\nExamples:\n  node bin/validate_opinion_keywords.js --db ny_reporter --limit 5\n  node bin/validate_opinion_keywords.js --db $DATABASE_URL --case-id 12345\n  node bin/validate_opinion_keywords.js --db ny_reporter --opinion-ids 101,202,303 --json\n  node bin/validate_opinion_keywords.js --db ny_reporter --opinion-types majority,unanimous --limit 10\n`);
}

async function fetchOpinions(pool, { limit, caseId, opinionIds, opinionTypes, random }) {
  const where = ['o.is_valueless = false'];
  const params = [];
  let idx = 1;

  if (opinionTypes && opinionTypes.length > 0) {
    where.push(`o.opinion_type = ANY($${idx}::text[])`);
    params.push(opinionTypes);
    idx++;
  }
  if (Number.isFinite(caseId)) {
    where.push(`o.case_id = $${idx}`);
    params.push(caseId);
    idx++;
  }
  if (opinionIds && opinionIds.length > 0) {
    where.push(`o.id = ANY($${idx}::int[])`);
    params.push(opinionIds);
    idx++;
  }

  // When selecting randomly (and not targeting explicit IDs), restrict to opinions that already have keywords
  if ((!opinionIds || opinionIds.length === 0) && random) {
    where.push('EXISTS (SELECT 1 FROM opinion_keywords ok WHERE ok.opinion_id = o.id)');
  }

  let order = '';
  if ((!opinionIds || opinionIds.length === 0) && random) {
    order = 'ORDER BY random()';
  } else {
    order = 'ORDER BY o.id';
  }

  const limitClause = (limit && limit > 0) ? `LIMIT $${idx}` : '';
  if (limitClause) params.push(limit);

  const sql = `
    SELECT 
      o.id AS opinion_id,
      o.case_id,
      o.opinion_type,
      o.author,
      o.text,
      c.name AS case_name,
      cit.cite AS citation
    FROM opinions o
    LEFT JOIN cases c ON c.id = o.case_id
    LEFT JOIN LATERAL (
      SELECT cite FROM citations ct WHERE ct.case_id = o.case_id ORDER BY ct.id ASC LIMIT 1
    ) cit ON TRUE
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ${order}
    ${limitClause}
  `;

  const res = await pool.query(sql, params);
  return res.rows;
}

async function fetchOpinionKeywords(pool, opinionIds) {
  if (!opinionIds || opinionIds.length === 0) return new Map();
  const sql = `
    SELECT 
      ok.opinion_id,
      k.keyword_text,
      k.tier,
      ok.relevance_score,
      ok.extraction_method,
      ok.category,
      ok.context
    FROM opinion_keywords ok
    JOIN keywords k ON k.id = ok.keyword_id
    WHERE ok.opinion_id = ANY($1::int[])
    ORDER BY k.tier, k.keyword_text
  `;
  const res = await pool.query(sql, [opinionIds]);
  const byOpinion = new Map();
  for (const row of res.rows) {
    if (!byOpinion.has(row.opinion_id)) byOpinion.set(row.opinion_id, []);
    byOpinion.get(row.opinion_id).push(row);
  }
  return byOpinion;
}

async function fetchOpinionHoldings(pool, opinionIds) {
  if (!opinionIds || opinionIds.length === 0) return new Map();
  const sql = `
    SELECT 
      oh.opinion_id,
      oh.issue,
      oh.holding,
      oh.rule,
      oh.reasoning,
      oh.precedential_value,
      oh.confidence
    FROM opinion_holdings oh
    WHERE oh.opinion_id = ANY($1::int[])
    ORDER BY oh.confidence DESC, oh.id
  `;
  const res = await pool.query(sql, [opinionIds]);
  const byOpinion = new Map();
  for (const row of res.rows) {
    if (!byOpinion.has(row.opinion_id)) byOpinion.set(row.opinion_id, []);
    byOpinion.get(row.opinion_id).push(row);
  }
  return byOpinion;
}

async function fetchOpinionOverruledCases(pool, opinionIds) {
  if (!opinionIds || opinionIds.length === 0) return new Map();
  const sql = `
    SELECT 
      ooc.opinion_id,
      ooc.case_name,
      ooc.citation,
      ooc.scope,
      ooc.overruling_language
    FROM opinion_overruled_cases ooc
    WHERE ooc.opinion_id = ANY($1::int[])
    ORDER BY ooc.case_name, ooc.id
  `;
  const res = await pool.query(sql, [opinionIds]);
  const byOpinion = new Map();
  for (const row of res.rows) {
    if (!byOpinion.has(row.opinion_id)) byOpinion.set(row.opinion_id, []);
    byOpinion.get(row.opinion_id).push(row);
  }
  return byOpinion;
}

function formatOpinionPretty(op, keywords, holdings, overruledCases) {
  const header = `Opinion ${op.opinion_id} (case_id=${op.case_id || 'n/a'}) [${op.opinion_type || 'n/a'}]`;
  const lines = [header];
  if (op.case_name) lines.push(`  Case: ${op.case_name}`);
  if (op.author) lines.push(`  Author: ${op.author}`);
  if (op.citation) lines.push(`  Citation: ${op.citation}`);
  if (op.text) {
    lines.push('');
    lines.push('Text:');
    lines.push(op.text);
  }

  if (!keywords || keywords.length === 0) {
    lines.push('  (no opinion_keywords found)');
  } else {

  // Group by tier
  const groups = keywords.reduce((acc, k) => {
    const tier = k.tier || k.category || 'unknown';
    acc[tier] = acc[tier] || [];
    acc[tier].push(k);
    return acc;
  }, {});

  const order = ['field_of_law', 'doctrine', 'distinguishing_factor', 'procedural_posture', 'case_outcome'];
  for (const tier of order.concat(Object.keys(groups).filter(t => !order.includes(t)))) {
    const arr = groups[tier];
    if (!arr || arr.length === 0) continue;
    lines.push(`  ${tier}:`);

    for (const k of arr) {
      const base = `    - ${k.keyword_text}`;
      const details = [];
      if (k.relevance_score != null) details.push(`score=${Number(k.relevance_score).toFixed(2)}`);
      if (k.extraction_method) details.push(k.extraction_method);
      if (k.category && k.category !== tier) details.push(`cat=${k.category}`);

      let line = base + (details.length ? ` (${details.join(', ')})` : '');

      // For distinguishing factors, show context details if available
      if (tier === 'distinguishing_factor' && k.context) {
        try {
          const ctx = typeof k.context === 'string' ? JSON.parse(k.context) : k.context;
          const axis = ctx.axis || ctx.axis_code;
          const spec = ctx.specific_reasoning;
          const gen = ctx.generalized_pattern;
          const extra = [axis ? `axis=${axis}` : null, spec ? `reasoning=${spec}` : null, gen ? `generalized=${gen}` : null].filter(Boolean);
          if (extra.length) line += `\n      · ${extra.join(' | ')}`;
        } catch (_) {}
      }

      lines.push(line);
    }
  }

  }

  // Add holdings section
  if (holdings && holdings.length > 0) {
    lines.push('  holdings:');
    for (const h of holdings) {
      lines.push(`    - Issue: ${h.issue}`);
      lines.push(`      Holding: ${h.holding}`);
      lines.push(`      Rule: ${h.rule}`);
      lines.push(`      Reasoning: ${h.reasoning}`);
      lines.push(`      Precedential Value: ${h.precedential_value} (confidence=${Number(h.confidence).toFixed(2)})`);
      lines.push('');
    }
  }

  // Add overruled cases section
  if (overruledCases && overruledCases.length > 0) {
    lines.push('  overruled_cases:');
    for (const oc of overruledCases) {
      lines.push(`    - Case: ${oc.case_name}`);
      if (oc.citation) lines.push(`      Citation: ${oc.citation}`);
      lines.push(`      Scope: ${oc.scope}`);
      lines.push(`      Overruling Language: ${oc.overruling_language}`);
      lines.push('');
    }
  }

  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help || !args.db) { usage(); process.exit(args.help ? 0 : 1); }

  const pool = common.makePgClient(args.db);
  try {
    const opinions = await fetchOpinions(pool, args);
    const opinionIds = opinions.map(o => o.opinion_id);
    const [kwMap, holdingsMap, overruledCasesMap] = await Promise.all([
      fetchOpinionKeywords(pool, opinionIds),
      fetchOpinionHoldings(pool, opinionIds),
      fetchOpinionOverruledCases(pool, opinionIds)
    ]);

    if (args.json) {
      const out = opinions.map(op => ({
        opinion: op,
        keywords: kwMap.get(op.opinion_id) || [],
        holdings: holdingsMap.get(op.opinion_id) || [],
        overruled_cases: overruledCasesMap.get(op.opinion_id) || []
      }));
      console.log(JSON.stringify(out, null, 2));
      return;
    }

    if (opinions.length === 0) {
      console.log('No opinions matched the filter.');
      return;
    }

    for (const op of opinions) {
      const kw = kwMap.get(op.opinion_id) || [];
      const holdings = holdingsMap.get(op.opinion_id) || [];
      const overruledCases = overruledCasesMap.get(op.opinion_id) || [];
      console.log(formatOpinionPretty(op, kw, holdings, overruledCases));
      console.log('');
    }
  } catch (err) {
    console.error(`[validator] Error: ${err.message}`);
    if (args.debug) console.error(err.stack);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
