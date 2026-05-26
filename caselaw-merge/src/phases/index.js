// Phase registry. Each phase exposes { id, name, run({...}) }.
// Phase 1 is fully implemented (schema bootstrap); 2–16 are stubs that
// throw "not implemented" with a spec-quoting comment inline.

import { phase01 } from './phase01_schema.js';
import { phase02 } from './phase02_keywords.js';
import { phase03 } from './phase03_cases.js';
import { phase04 } from './phase04_opinions.js';
import { phase05 } from './phase05_citations.js';
import { phase06 } from './phase06_analysis_runs.js';
import { phase07 } from './phase07_opinion_children.js';
import { phase08 } from './phase08_case_captions.js';
import { phase09 } from './phase09_doctrine_anchors.js';
import { phase10 } from './phase10_doctrine_case_classifications.js';
import { phase11 } from './phase11_keyword_relations.js';
import { phase12 } from './phase12_appellate_history_case_status.js';
import { phase13 } from './phase13_appellate_history_connections.js';
import { phase14 } from './phase14_appellate_history_resolution_queue.js';
import { phase15 } from './phase15_batch_and_notes.js';
import { phase16 } from './phase16_view_and_validation.js';

export const PHASES = [
  phase01, phase02, phase03, phase04,
  phase05, phase06, phase07, phase08,
  phase09, phase10, phase11, phase12,
  phase13, phase14, phase15, phase16,
];

export function phaseById(id) {
  const p = PHASES.find((x) => x.id === id);
  if (!p) throw new Error(`Unknown phase id: ${id}`);
  return p;
}
