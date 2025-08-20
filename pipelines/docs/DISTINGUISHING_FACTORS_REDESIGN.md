# Distinguishing Factors Generalization Design

## Current Problem
Distinguishing factors are stored as case-specific facts rather than reusable patterns:
- `"temporal_sequence_timing: Payments were made to defendant while corporation continued..."`
- Not searchable across similar cases
- Not useful as keywords

## Proposed Solution

### 1. Two-Level Storage Architecture

#### Keywords Table (Generalized Patterns)
Store only the generalized pattern as the keyword:
```sql
-- Example keywords for the same case
"Payment During Ongoing Operations"  -- from temporal_sequence_timing
"Capital Depletion Without Safeguards"  -- from risk_safeguard_environment
"Absence of Fraudulent Intent"  -- from knowledge_awareness_state
"Stock-for-Asset Substitution"  -- from policy_practice_environment
```

#### Opinion_Keywords Context JSON (Specific Details)
Store axis and specific reasoning in context:
```json
{
  "axis": "tst",
  "axis_name": "temporal_sequence_timing",
  "specific_reasoning": "Payments were made to the defendant while corporation continued operations and before bankruptcy adjudication",
  "generalized_pattern": "Payment During Ongoing Operations",
  "importance": "high"
}
```

### 2. Generalization Patterns by Axis

#### Actor Roles & Capacities (arc)
- Specific: "defendant as a surety for construction project"
- Generalized: "Party as Financial Guarantor"
- Pattern: `[Party Type] as [Role Category]`

#### Temporal Sequence & Timing (tst)
- Specific: "Payments made while corporation continued operations before bankruptcy"
- Generalized: "Transaction During Solvency Period"
- Pattern: `[Action] [Timing Relation] [Legal Event]`

#### Operational Setting & Conditions (osc)
- Specific: "erection of a building for county fair association"
- Generalized: "Construction Project Context"
- Pattern: `[Operation Type] [Setting Category]`

#### Action/Inaction Categories (aic)
- Specific: "Corporation paid funds to defendant from business receipts to satisfy debt"
- Generalized: "Third-Party Debt Payment"
- Pattern: `[Actor] [Action Type] [Object Category]`

#### Resource & Asset Context (rac)
- Specific: "$22,543.10 over contract price"
- Generalized: "Contract Price Excess"
- Pattern: `[Asset Type] [Relation to Baseline]`

#### Communication & Information Flow (cif)
- Specific: "Notice sent to all creditors before asset transfer"
- Generalized: "Creditor Notice Provided"
- Pattern: `[Information Type] [Flow Direction]`

#### Knowledge & Awareness State (kas)
- Specific: "Parties found to have no intent to defraud creditors"
- Generalized: "Absence of Fraudulent Intent"
- Pattern: `[Awareness Level] of [Subject Category]`

#### Relationship & Interaction Dynamics (rid)
- Specific: "Defendant and Wilcox had supplemental agreement"
- Generalized: "Supplemental Agreement Between Parties"
- Pattern: `[Relationship Type] [Interaction Category]`

#### Policy & Practice Environment (ppe)
- Specific: "Capital stock issued in lieu of purchase price"
- Generalized: "Stock-for-Asset Exchange"
- Pattern: `[Policy Type] [Compliance/Deviation]`

#### Risk & Safeguard Environment (rse)
- Specific: "Corporate capital diminished without protective safeguards"
- Generalized: "Capital Depletion Without Safeguards"
- Pattern: `[Risk Type] [Safeguard Presence]`

#### Outcome & Consequence Patterns (ocp)
- Specific: "Transaction resulted in preferential treatment of certain creditors"
- Generalized: "Creditor Preference Result"
- Pattern: `[Outcome Type] [Impact Category]`

#### Standard & Threshold Criteria (stc)
- Specific: "Payment exceeded reasonable compensation by 40%"
- Generalized: "Compensation Reasonableness Exceeded"
- Pattern: `[Standard Type] [Threshold Status]`

### 3. Implementation Approach

#### Phase 1: Update Prompt Templates
Modify Pass 1 prompt to generate both specific and generalized versions:
```json
{
  "d": [
    {
      "a": "arc",
      "r": "defendant as a surety",  // specific
      "g": "Party as Financial Guarantor",  // generalized
      "i": "high"
    }
  ]
}
```

#### Phase 2: Update Schema
Add optional generalized field:
```json
{
  "a": { "type": "string", "enum": ["arc", "tst", ...] },
  "r": { "type": "string" },  // specific reasoning
  "g": { "type": "string" },  // generalized pattern (optional)
  "i": { "type": "string", "enum": ["high", "medium", "low"] }
}
```

#### Phase 3: Update Database Logic
- Store generalized pattern as keyword_text
- Store full context (axis, specific, generalized) in opinion_keywords.context

#### Phase 4: Create Pattern Library
Build a reference library of common patterns per axis for prompt examples.

### 4. Benefits

1. **Searchability**: Find cases with similar fact patterns
2. **Analytics**: Identify common patterns across jurisdictions
3. **Reusability**: Keywords become meaningful legal concepts
4. **Context Preservation**: Full details available in context JSON
5. **Scalability**: Patterns emerge and stabilize over time

### 5. Example Query Use Cases

```sql
-- Find all cases with "Third-Party Debt Payment" pattern
SELECT o.* 
FROM opinions o
JOIN opinion_keywords ok ON o.id = ok.opinion_id
JOIN keywords k ON ok.keyword_id = k.id
WHERE k.keyword_text = 'Third-Party Debt Payment'
  AND k.tier = 'distinguishing_factor';

-- Find all temporal timing issues around bankruptcy
SELECT o.*, ok.context->>'specific_reasoning' as details
FROM opinions o
JOIN opinion_keywords ok ON o.id = ok.opinion_id
JOIN keywords k ON ok.keyword_id = k.id
WHERE k.keyword_text LIKE '%During Solvency Period%'
  AND ok.context->>'axis' = 'tst';
```
