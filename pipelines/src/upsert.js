async function getKeywordByText(client, keywordText) {
  const res = await client.query('SELECT id, tier FROM keywords WHERE keyword_text = $1', [keywordText]);
  return res.rows[0] || null;
}

async function insertKeyword(client, keywordText, tier) {
  const res = await client.query(
    'INSERT INTO keywords (keyword_text, tier, frequency) VALUES ($1, $2, 0) ON CONFLICT (keyword_text) DO NOTHING RETURNING id, tier',
    [keywordText, tier]
  );
  if (res.rowCount > 0) return res.rows[0];
  // conflict: fetch existing
  const existing = await getKeywordByText(client, keywordText);
  if (existing && existing.tier !== tier) {
    console.warn(`[upsert] keyword tier mismatch for "${keywordText}" existing=${existing.tier} incoming=${tier} (leaving as existing)`);
  }
  return existing;
}

async function upsertOpinionKeyword(client, opinionId, keywordId, { relevanceScore = null, method = null, category = null, context = {} } = {}) {
  await client.query(
    `WITH up AS (
       INSERT INTO opinion_keywords (opinion_id, keyword_id, relevance_score, extraction_method, category, context)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (opinion_id, keyword_id) DO UPDATE
         SET relevance_score = COALESCE(EXCLUDED.relevance_score, opinion_keywords.relevance_score),
             extraction_method = COALESCE(EXCLUDED.extraction_method, opinion_keywords.extraction_method),
             category = COALESCE(EXCLUDED.category, opinion_keywords.category),
             context = opinion_keywords.context || EXCLUDED.context
       RETURNING (xmax = 0) AS inserted
     )
     UPDATE keywords SET frequency = frequency + 1
     WHERE id = $2 AND EXISTS (SELECT 1 FROM up WHERE inserted)`,
    [opinionId, keywordId, relevanceScore, method, category, JSON.stringify(context)]
  );
}

function makeDistinguishingKey(axis, text) {
  // naive normalization for uniqueness in unified keywords table
  return `${axis.trim()}: ${text.trim()}`;
}

// Helper function to extract text from character indices
function extractEvidence(opinionText, start, end) {
  if (!opinionText || start == null || end == null) return null;
  if (start < 0 || end < 0 || start >= opinionText.length || end > opinionText.length || start >= end) {
    return null;
  }
  return opinionText.slice(start, end);
}

// Upsert a single holding row for an opinion (dedupe on opinion_id + issue + holding + rule)
async function upsertOpinionHolding(client, opinionId, holding) {
  const { issue, holding: holdingText, rule, reasoning, precedential_value, confidence } = holding || {};
  if (!issue || !holdingText || !rule || !reasoning || !precedential_value || confidence == null) return;

  await client.query(
    `INSERT INTO opinion_holdings (opinion_id, issue, holding, rule, reasoning, precedential_value, confidence)
     SELECT $1, $2, $3, $4, $5, $6, $7
     WHERE NOT EXISTS (
       SELECT 1 FROM opinion_holdings
       WHERE opinion_id = $1 AND issue = $2 AND holding = $3 AND rule = $4
     )`,
    [opinionId, issue, holdingText, rule, reasoning, precedential_value, confidence]
  );
}

async function upsertOpinionOverruledCase(client, opinionId, overruledCase) {
  const { case_name, citation, scope, overruling_language, overruling_type, overruling_court, overruling_case } = overruledCase || {};
  if (!case_name || !scope || !overruling_language) return;

  const TYPE_SET = new Set(['direct', 'reported']);
  const safe_type = overruling_type && TYPE_SET.has(overruling_type) ? overruling_type : null;

  await client.query(
    `INSERT INTO opinion_overruled_cases (opinion_id, case_name, citation, scope, overruling_language, overruling_type, overruling_court, overruling_case)
     SELECT $1, $2, $3, $4, $5, $6, $7, $8
     WHERE NOT EXISTS (
       SELECT 1 FROM opinion_overruled_cases
       WHERE opinion_id = $1 AND case_name = $2 AND scope = $3 AND overruling_language = $4 AND COALESCE(overruling_type, '') = COALESCE($6, '')
     )`,
    [opinionId, case_name, citation || null, scope, overruling_language, safe_type, overruling_court || null, overruling_case || null]
  );
}

// Upsert an extracted citation for an opinion (dedupe conservatively on several fields)
async function upsertOpinionCitation(client, opinionId, citation) {
  if (!citation || typeof citation !== 'object') return;
  const {
    cite_text,
    case_name,
    normalized_citation,
    authority_type,
    jurisdiction,
    court_level,
    year,
    pincite,
    citation_context,
    citation_signal,
    precedential_weight,
    discussion_level,
    legal_proposition,
    confidence
  } = citation;

  // Require at least one of cite_text or normalized_citation to avoid empty rows
  if (!cite_text && !normalized_citation) return;

  // Sanitize enum fields to avoid DB constraint violations
  const AUTH_TYPES = new Set(['case','statute','regulation','constitutional','secondary']);
  const COURT_LEVELS = new Set(['supreme','appellate','trial','federal_appellate','federal_district']);
  const PRECEDENTIAL_WEIGHTS = new Set(['binding','highly_persuasive','persuasive','non_binding']);

  const safe_authority_type = authority_type && AUTH_TYPES.has(authority_type) ? authority_type : null;
  const safe_court_level = court_level && COURT_LEVELS.has(court_level) ? court_level : null;
  const safe_precedential_weight = precedential_weight && PRECEDENTIAL_WEIGHTS.has(precedential_weight) ? precedential_weight : null;
  const safe_confidence = (typeof confidence === 'number' && confidence >= 0.5 && confidence <= 1) ? confidence : null;

  await client.query(
    `INSERT INTO opinion_citations (
        opinion_id, cite_text, case_name, normalized_citation, authority_type,
        jurisdiction, court_level, year, pincite, citation_context,
        citation_signal, precedential_weight, discussion_level, legal_proposition, confidence
     )
     SELECT $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15
     WHERE NOT EXISTS (
       SELECT 1 FROM opinion_citations oc
       WHERE oc.opinion_id = $1
         AND COALESCE(oc.normalized_citation, '') = COALESCE($4, '')
         AND COALESCE(oc.cite_text, '') = COALESCE($2, '')
         AND COALESCE(oc.pincite, '') = COALESCE($9, '')
     )`,
    [
      opinionId,
      cite_text || null,
      case_name || null,
      normalized_citation || null,
      safe_authority_type,
      jurisdiction || null,
      safe_court_level,
      year || null,
      pincite || null,
      citation_context || null,
      citation_signal || null,
      safe_precedential_weight,
      discussion_level || null,
      legal_proposition || null,
      safe_confidence
    ]
  );
}

async function _upsertCoreCategories(client, opinionId, payload, { method = 'pass1_llm', opinionText = null } = {}) {
  // field_of_law
  for (const item of payload.field_of_law || []) {
    const kw = await insertKeyword(client, item.label, 'field_of_law');
    if (!kw) continue;
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      relevanceScore: item.score ?? null,
      method,
      category: 'field_of_law'
    });
  }
  // procedural_posture
  for (const item of payload.procedural_posture || []) {
    const kw = await insertKeyword(client, item.canonical, 'procedural_posture');
    if (!kw) continue;
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      method,
      category: 'procedural_posture'
    });
  }
  // case_outcome
  for (const item of payload.case_outcome || []) {
    const kw = await insertKeyword(client, item.canonical, 'case_outcome');
    if (!kw) continue;
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      method,
      category: 'case_outcome'
    });
  }
  // distinguishing_factors
  for (const item of payload.distinguishing_factors || []) {
    // Use the generalized pattern as the keyword (if provided)
    // Fall back to old format if no generalized pattern
    const keywordText = item.generalized || makeDistinguishingKey(item.axis, item.reasoning);
    const kw = await insertKeyword(client, keywordText, 'distinguishing_factor');
    if (!kw) continue;
    
    // Store full context including axis, specific reasoning, and generalized pattern
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      method,
      category: 'distinguishing_factor',
      context: { 
        axis: item.axis,
        axis_code: item.axis, // Store short code too for searching
        specific_reasoning: item.reasoning,
        generalized_pattern: item.generalized || keywordText,
        importance: item.importance || null 
      }
    });
  }
}

async function upsertUnified(client, opinionId, payload, { method = 'unified_llm', opinionText = null } = {}) {
  // First, handle all Pass 1 categories
  await _upsertCoreCategories(client, opinionId, payload, { method, opinionText });

  // Holdings (unified-only)
  for (const h of payload.holdings || []) {
    await upsertOpinionHolding(client, opinionId, h);
  }

  // Overruled Cases (unified-only)
  for (const oc of payload.overruled_cases || []) {
    await upsertOpinionOverruledCase(client, opinionId, oc);
  }

  // Citations (unified-only)
  for (const ci of payload.citations || []) {
    await upsertOpinionCitation(client, opinionId, ci);
  }

  // Doctrines as normal keywords
  for (const item of payload.doctrines || []) {
    if (!item || !item.name) continue;
    const kw = await insertKeyword(client, item.name, 'doctrine');
    if (!kw) continue;
    const evidence_text = extractEvidence(opinionText, item.evidence_start, item.evidence_end);
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      method,
      category: 'doctrine',
      context: {
        evidence_start: item.evidence_start ?? null,
        evidence_end: item.evidence_end ?? null,
        ...(evidence_text ? { evidence_text } : {})
      }
    });
  }

  // Doctrinal tests as normal keywords
  for (const item of payload.doctrinal_tests || []) {
    if (!item || !item.name) continue;
    const kw = await insertKeyword(client, item.name, 'doctrinal_test');
    if (!kw) continue;
    const evidence_text = extractEvidence(opinionText, item.evidence_start, item.evidence_end);
    await upsertOpinionKeyword(client, opinionId, kw.id, {
      method,
      category: 'doctrinal_test',
      context: {
        evidence_start: item.evidence_start ?? null,
        evidence_end: item.evidence_end ?? null,
        ...(Array.isArray(item.doctrine_names) ? { doctrine_names: item.doctrine_names } : {}),
        ...(item.test_type ? { test_type: item.test_type } : {}),
        ...(item.primary_citation ? { primary_citation: item.primary_citation } : {}),
        ...(Array.isArray(item.aliases) ? { aliases: item.aliases } : {}),
        ...(evidence_text ? { evidence_text } : {})
      }
    });
  }
}

// Deprecated: keep for legacy callers until all references are removed
async function upsertPass1(client, opinionId, payload, opts = {}) {
  console.warn('[deprec] upsertPass1() is deprecated; forwarding to unified core categories');
  return _upsertCoreCategories(client, opinionId, payload, opts);
}

module.exports = {
  insertKeyword,
  upsertOpinionKeyword,
  upsertOpinionHolding,
  upsertOpinionOverruledCase,
  upsertOpinionCitation,
  upsertPass1,
  upsertUnified,
};
