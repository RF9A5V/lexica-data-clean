/**
 * Field mapping utilities for minimal/verbose LLM output conversion
 * Converts between minimal field names (for token efficiency) and verbose names (for validation/storage)
 */

// Pass 1 field mappings
const PASS1_MINIMAL_TO_VERBOSE = {
  // Top-level fields
  v: 'valueless',
  vr: 'valueless_reason',
  f: 'field_of_law',
  p: 'procedural_posture',
  o: 'case_outcome',
  d: 'distinguishing_factors',
  
  // Item-level fields
  l: 'label',
  sc: 'score',
  c: 'canonical',
  j: 'jurisdictional',
  a: 'axis',
  r: 'reasoning',
  g: 'generalized',
  i: 'importance'
};

// Axis mappings for distinguishing factors (updated to unified prompt spec)
const AXIS_MINIMAL_TO_VERBOSE = {
  idc: 'industry_domain_context',
  osc: 'organizational_structural_context',
  ppe: 'policy_practice_environment',
  ops: 'operational_setting_conditions',
  arc: 'actor_roles_capacities',
  aic: 'action_inaction_categories',
  rac: 'resource_asset_context',
  cif: 'communication_information_flow',
  tsf: 'temporal_sequence_factors',
  kas: 'knowledge_awareness_state',
  rse: 'risk_safeguard_environment',
  imp: 'impact_profile'
};

const AXIS_VERBOSE_TO_MINIMAL = Object.fromEntries(
  Object.entries(AXIS_MINIMAL_TO_VERBOSE).map(([k, v]) => [v, k])
);

const PASS1_VERBOSE_TO_MINIMAL = Object.fromEntries(
  Object.entries(PASS1_MINIMAL_TO_VERBOSE).map(([k, v]) => [v, k])
);

// Pass 2 field mappings
const PASS2_MINIMAL_TO_VERBOSE = {
  // Top-level fields
  d: 'doctrines',
  t: 'doctrinal_tests',
  
  // Item-level fields
  n: 'name',
  al: 'aliases',
  pc: 'primary_citation',
  tt: 'test_type',
  dn: 'doctrine_names'
};

const PASS2_VERBOSE_TO_MINIMAL = Object.fromEntries(
  Object.entries(PASS2_MINIMAL_TO_VERBOSE).map(([k, v]) => [v, k])
);

/**
 * Expand minimal Pass 1 response to verbose format
 */
function expandPass1Response(minimalPayload) {
  if (!minimalPayload || typeof minimalPayload !== 'object') {
    return minimalPayload;
  }

  const expanded = {};

  // Handle valueless case
  if (minimalPayload.v === true) {
    expanded.valueless = true;
    if (minimalPayload.vr) expanded.valueless_reason = minimalPayload.vr;
    return expanded;
  }
  
  // Set valueless to false if explicitly provided
  if (minimalPayload.v !== undefined) {
    expanded.valueless = minimalPayload.v;
  }

  // field_of_law
  if (minimalPayload.f) {
    expanded.field_of_law = minimalPayload.f.map(item => ({
      label: item.l,
      score: item.sc
    }));
  }

  // procedural_posture
  if (minimalPayload.p) {
    expanded.procedural_posture = minimalPayload.p.map(item => ({
      canonical: item.c
    }));
  }

  // case_outcome
  if (minimalPayload.o) {
    expanded.case_outcome = minimalPayload.o.map(item => ({
      canonical: item.c
    }));
  }

  // distinguishing_factors
  if (minimalPayload.d) {
    expanded.distinguishing_factors = minimalPayload.d.map(item => ({
      axis: AXIS_MINIMAL_TO_VERBOSE[item.a] || item.a,
      reasoning: item.r,
      generalized: item.g,
      importance: item.i
    }));
  }

  return expanded;
}

/**
 * Expand minimal Pass 2 response to verbose format
 */
function expandPass2Response(minimalPayload) {
  if (!minimalPayload || typeof minimalPayload !== 'object') {
    return minimalPayload;
  }

  const expanded = {};

  // doctrines
  if (minimalPayload.d) {
    expanded.doctrines = minimalPayload.d.map(item => ({
      name: item.n,
      evidence_start: item.s,
      evidence_end: item.e
    }));
  }

  // doctrinal_tests
  if (minimalPayload.t) {
    expanded.doctrinal_tests = minimalPayload.t.map(item => ({
      name: item.n,
      doctrine_names: item.dn,
      evidence_start: item.s,
      evidence_end: item.e,
      ...(item.al && { aliases: item.al }),
      ...(item.pc && { primary_citation: item.pc }),
      ...(item.tt && { test_type: item.tt })
    }));
  }

  return expanded;
}

/**
 * Contract verbose Pass 1 response to minimal format (for testing/samples)
 */
function contractPass1Response(verbosePayload) {
  if (!verbosePayload || typeof verbosePayload !== 'object') {
    return verbosePayload;
  }

  const minimal = {};

  // Handle valueless case
  if (verbosePayload.valueless !== undefined) {
    minimal.v = verbosePayload.valueless;
    if (verbosePayload.valueless_reason) minimal.vr = verbosePayload.valueless_reason;
    return minimal;
  }

  // field_of_law
  if (verbosePayload.field_of_law) {
    minimal.f = verbosePayload.field_of_law.map(item => ({
      l: item.label,
      sc: item.score
    }));
  }

  // procedural_posture
  if (verbosePayload.procedural_posture) {
    minimal.p = verbosePayload.procedural_posture.map(item => ({
      c: item.canonical,
      s: item.evidence_start,
      e: item.evidence_end,
      ...(item.jurisdictional && { j: item.jurisdictional })
    }));
  }

  // case_outcome
  if (verbosePayload.case_outcome) {
    minimal.o = verbosePayload.case_outcome.map(item => ({
      c: item.canonical
    }));
  }

  // distinguishing_factors
  if (verbosePayload.distinguishing_factors) {
    minimal.d = verbosePayload.distinguishing_factors.map(item => ({
      a: AXIS_VERBOSE_TO_MINIMAL[item.axis] || item.axis,
      r: item.reasoning,
      g: item.generalized,
      i: item.importance
    }));
  }

  return minimal;
}

/**
 * Contract verbose Pass 2 response to minimal format (for testing/samples)
 */
function contractPass2Response(verbosePayload) {
  if (!verbosePayload || typeof verbosePayload !== 'object') {
    return verbosePayload;
  }

  const minimal = {};

  // doctrines
  if (verbosePayload.doctrines) {
    minimal.d = verbosePayload.doctrines.map(item => ({
      n: item.name,
      s: item.evidence_start,
      e: item.evidence_end
    }));
  }

  // doctrinal_tests
  if (verbosePayload.doctrinal_tests) {
    minimal.t = verbosePayload.doctrinal_tests.map(item => ({
      n: item.name,
      dn: item.doctrine_names,
      s: item.evidence_start,
      e: item.evidence_end,
      ...(item.aliases && { al: item.aliases }),
      ...(item.primary_citation && { pc: item.primary_citation }),
      ...(item.test_type && { tt: item.test_type })
    }));
  }

  return minimal;
}

module.exports = {
  PASS1_MINIMAL_TO_VERBOSE,
  PASS1_VERBOSE_TO_MINIMAL,
  PASS2_MINIMAL_TO_VERBOSE,
  PASS2_VERBOSE_TO_MINIMAL,
  expandPass1Response,
  expandPass2Response,
  contractPass1Response,
  contractPass2Response
};

/**
 * Expand minimal Unified response to verbose format
 * Unified minimal keys:
 *  - v (valueless), vr (valueless_reason)
 *  - f (field_of_law), p (procedural_posture), o (case_outcome), df (distinguishing_factors)
 *  - dc (doctrines), dt (doctrinal_tests)
 */
function expandUnifiedResponse(minimalPayload) {
  if (!minimalPayload || typeof minimalPayload !== 'object') {
    return minimalPayload;
  }

  // Compose Pass 1 minimal object (note: df -> d)
  const p1Minimal = {};
  if (minimalPayload.v !== undefined) p1Minimal.v = minimalPayload.v;
  if (minimalPayload.vr !== undefined) p1Minimal.vr = minimalPayload.vr;
  if (minimalPayload.f) p1Minimal.f = minimalPayload.f;
  if (minimalPayload.p) p1Minimal.p = minimalPayload.p;
  if (minimalPayload.o) p1Minimal.o = minimalPayload.o;
  if (minimalPayload.df) p1Minimal.d = minimalPayload.df;

  const p1Expanded = expandPass1Response(p1Minimal) || {};

  // Compose Pass 2 minimal object (dc -> d, dt -> t)
  const p2Minimal = {};
  if (minimalPayload.dc) p2Minimal.d = minimalPayload.dc;
  if (minimalPayload.dt) p2Minimal.t = minimalPayload.dt;

  const p2Expanded = expandPass2Response(p2Minimal) || {};

  // Holdings (unified-only)
  let holdingsExpanded = {};
  if (Array.isArray(minimalPayload.h)) {
    holdingsExpanded = {
      holdings: minimalPayload.h.map(item => ({
        issue: item.is,
        holding: item.ho,
        rule: item.ru,
        reasoning: item.re,
        precedential_value: item.pv,
        confidence: item.cf
      }))
    };
  }

  // Overruled Cases (unified-only)
  let overruledCasesExpanded = {};
  if (Array.isArray(minimalPayload.oc)) {
    overruledCasesExpanded = {
      overruled_cases: minimalPayload.oc.map(item => ({
        case_name: item.cn,
        citation: item.ct,
        scope: item.s,
        overruling_language: item.ol,
        ...(item.ot && { overruling_type: item.ot }),
        ...(item.ocourt && { overruling_court: item.ocourt }),
        ...(item.ocase && { overruling_case: item.ocase })
      }))
    };
  }

  // Citations (unified-only)
  let citationsExpanded = {};
  if (Array.isArray(minimalPayload.ci)) {
    citationsExpanded = {
      citations: minimalPayload.ci.map(item => ({
        cite_text: item.ct,
        case_name: item.cn,
        normalized_citation: item.cn_norm,
        authority_type: item.at,
        jurisdiction: item.j,
        court_level: item.cl,
        year: item.y,
        pincite: item.pc,
        citation_context: item.cc,
        citation_signal: item.cs,
        precedential_weight: item.pw,
        discussion_level: item.dl,
        legal_proposition: item.lp,
        confidence: item.cf
      }))
    };
  }

  return { ...p1Expanded, ...p2Expanded, ...holdingsExpanded, ...overruledCasesExpanded, ...citationsExpanded };
}

module.exports.expandUnifiedResponse = expandUnifiedResponse;
