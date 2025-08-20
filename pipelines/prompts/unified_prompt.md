# Unified Legal Keyword Extraction

Extract comprehensive legal metadata from the following court opinion in a single pass.

## Instructions
Extract ALL of the following categories, including doctrines and doctrinal tests. Return the minimal JSON structure defined below. Only include items that are supported by the opinion's substantive analysis and would be useful for legal research.

Analyze the opinion and extract ALL of the following categories:

### 1. Field of Law
Identify the primary legal fields addressed (e.g., "Contract Law", "Tort Law", "Criminal Law").
Provide confidence scores (0-1) for each field.

#### Field of Law Selection Rules:
- Include ONLY fields with substantial legal analysis (not just factual context)
- Primary field = main legal doctrine analyzed (score 0.8+)
- Secondary field = significant supporting analysis (score 0.6+)
- Exclude fields mentioned only in passing or as factual background
- Tort Law: Include only if tort liability/damages are analyzed, not just because accident occurred

#### Confidence Scoring for Fields of Law:
- 0.9-1.0: Central focus with extensive analysis
- 0.7-0.9: Significant discussion with legal holdings
- 0.5-0.7: Moderate treatment as secondary issue
- Below 0.5: Don't include (insufficient analysis)

If generating a keyword that ends in "Law", generally you may classify this keyword as a field of law.

### 2. Procedural Posture
Extract the specific procedural context that brought this case before the court.

#### Procedural Posture Analysis Process:
1. **Identify the underlying proceeding**: What type of case is this originally?
2. **Determine what triggered this court's involvement**: Motion, appeal, petition, etc.
3. **Identify the specific issue presented**: What is the court being asked to decide?
4. **Note any special procedural circumstances**: Interlocutory nature, certified questions, etc.

#### Primary Categories:

**Appeals (from lower court decisions):**
- Appeal from Trial Court
- Appeal from Appellate Division  
- Appeal to Court of Appeals
- Interlocutory Appeal
- Appeal by Permission/Leave

**Motions (requests to the court):**
- Motion to Dismiss
- Summary Judgment Motion
- Motion for New Trial
- Motion to Vacate
- Motion for Reargument/Reconsideration

**Special Proceedings:**
- CPLR Article 78 Proceeding (New York)
- Writ of Mandamus
- Writ of Prohibition
- Habeas Corpus Petition
- Certiorari Proceeding

**Pre-Trial/Discovery:**
- Discovery Motion
- Motion to Compel
- Motion for Protective Order
- Motion to Strike

**Post-Trial/Post-Judgment:**
- Motion for Judgment as Matter of Law
- Motion to Set Aside Verdict
- Post-Judgment Motion
- Enforcement Proceeding

**Class Actions and Complex Litigation:**
- Class Certification Motion
- Settlement Approval
- Intervention Motion

#### Jurisdiction-Specific Procedures:
**New York:**
- CPLR Article 78 Proceeding
- Tax Certiorari Proceeding  
- SCPA Proceeding (Surrogate's Court)

**California:**
- Anti-SLAPP Motion
- Writ of Administrative Mandate

**Federal:**
- Federal Rule 12(b)(6) Motion to Dismiss
- Federal Rule 56 Summary Judgment
- 28 U.S.C. § 1292(b) Interlocutory Appeal
- § 2254 Habeas Corpus Petition

#### Detection Strategy:

**Look for Signal Phrases:**
- "This appeal arises from..." → Appeal from [court]
- "Petitioner seeks..." → Petition for [relief]
- "Defendant moved to..." → Motion to [action]
- "The court granted..." → [Type] granted
- "On cross-motions for..." → Cross-Motions for [relief]

**Analyze the Court's Role:**
- **Reviewing**: Likely an appeal (from what court?)
- **Deciding Motion**: What type of motion?
- **Original Jurisdiction**: Special proceeding or petition
- **Supervisory**: Writ or mandamus proceeding

#### Formatting Rules:

**Basic Appeals:**
- "Appeal from [Lower Court]"
- "Appeal to [This Court]"
- "Interlocutory Appeal from [Lower Court]"

**Motions:**
- "[Motion Type] Motion" 
- "Cross-Motions for [Relief Type]"
- "Motion for [Specific Relief]"

**Special Proceedings:**
- "[Proceeding Type] Proceeding"
- "Petition for [Relief Type]"
- "Writ of [Type]"

#### Quality Control:

**Test Questions:**
1. **Specificity**: Does this tell us exactly what procedural step brought the case here?
2. **Accuracy**: Does this match what the court was actually asked to do?
3. **Completeness**: Does this capture any special procedural circumstances?
4. **Consistency**: Does this follow the standardized format rules?

**Common Errors to Avoid:**
❌ "Appeal to Court of Appeals" (when you know it's from Appellate Division)
❌ "Motion" (without specifying what type)
❌ "Civil Proceeding" (too generic)
❌ "Lawsuit" (not a procedural posture)
❌ "Trial Court Action" (too vague)

**Preferred Specificity:**
✅ "Appeal from Appellate Division"
✅ "Summary Judgment Motion" 
✅ "CPLR Article 78 Proceeding"
✅ "Motion to Dismiss for Failure to State Claim"
✅ "Interlocutory Appeal from Discovery Order"

#### Examples by Court Level:

**Court of Appeals Cases:**
- Appeal from Appellate Division
- Certified Question from Federal Court
- Motion for Leave to Appeal (rare direct cases)

**Appellate Division Cases:**
- Appeal from Supreme Court
- Appeal from Family Court
- CPLR Article 78 Proceeding
- Petition for Permission to Appeal

**Trial Court Cases:**
- Motion to Dismiss
- Summary Judgment Motion
- Discovery Motion
- Post-Trial Motion

### 3. Case Outcome
Identify the disposition (e.g., "Affirmed", "Reversed", "Remanded", "Dismissed").

#### Case Outcome - Critical Distinctions:
- "Affirmed" = Lower court decision upheld on the merits
- "Dismissed" = Case/appeal rejected without reaching merits (procedural)
- "Denied" = Motion/request rejected
- "Reversed" = Lower court decision overturned
- "Modified" = Lower court decision partially changed
- "Modified and affirmed" = Lower court decision partially changed but overall upheld

#### Common Errors to Avoid:
- Don't use "Affirmed" for dismissals of appeals/motions
- Don't use "Affirmed" for procedural rejections
- Use "Dismissed" for procedural dismissals (lack of jurisdiction, finality, etc.)

### 4. Distinguishing Factors
Extract key factual elements using these axes:
- industry_domain_context (idc)
- organizational_structural_context (osc)
- policy_practice_environment (ppe)
- operational_setting_conditions (ops)
- actor_roles_capacities (arc)
- action_inaction_categories (aic)
- resource_asset_context (rac)
- communication_information_flow (cif)
- temporal_sequence_factors (tsf)
- knowledge_awareness_state (kas)
- risk_safeguard_environment (rse)
- impact_profile (imp)

#### Distinguishing Factors - Enhanced Guidelines:
- Focus on legally significant facts that affect case outcome
- Avoid mere procedural steps unless they create legal issues
- Prefer factors that would help lawyers find similar cases
- Test: "Would a lawyer searching for cases like this use this factor?"
- Avoid over-generalization that loses legal specificity
- Must relate to legal analysis, not just case facts

### Avoid Redundant/Obvious Keywords:

#### Context-Based Exclusions:
- **Court Level**: Don't extract procedural postures that merely identify the court level (user already knows this)
- **Party Roles**: Don't extract generic party roles like "Appellant" or "Respondent" (obvious from appeal context)
- **Universal Procedures**: Don't extract procedures present in all similar cases

#### Field of Law Exclusions:
- Don't include "Civil Procedure" merely because case involves procedural motion
- Only include if case establishes or analyzes procedural doctrine
- Test: "Does this add discriminatory value beyond the procedural context?"

#### Distinguishing Factors Exclusions:
- Avoid factors present in most cases of this type
- Avoid restating information already captured in case outcome
- Focus on factors that distinguish THIS case from other similar cases

#### Examples of Redundant Keywords to AVOID:
❌ "Appellant as Party" (obvious in appeals)
❌ "Motion Filed" (obvious when discussing motion)
❌ "Civil Procedure" for routine motion dismissals
❌ "Denial of Motion" when outcome is already "Denied"
❌ "Court Proceeding Context" (too generic)

### Discriminatory Value Test:
For each potential keyword, ask:
1. **Uniqueness**: Does this distinguish this case from others in the same procedural context?
2. **Research Value**: Would a lawyer use this to find similar cases with shared legal issues?
3. **Non-Obvious**: Is this information not already apparent from the case context?
4. **Legal Significance**: Does this relate to substantive legal analysis, not just procedure?

Only extract keywords that pass ALL four tests.

#### Enhanced Axis Analysis:

**industry_domain_context (idc) - ALWAYS CHECK FOR INDUSTRY:**
Industries are legally significant because different regulatory frameworks, liability standards, and practices apply.

**Industry Detection Process:**
1. Scan for business/entity descriptions: What type of business/activity was involved?
2. Look for regulatory references: What agencies, licenses, or regulations mentioned?
3. Check for industry-specific terminology: Technical terms, trade practices, standards
4. Identify workplace/operational settings: Where did events occur?

**Industry Signal Words:**
- **Construction**: contractor, subcontractor, builder, construction site, building code
- **Healthcare**: physician, hospital, medical practice, patient, medical malpractice
- **Financial Services**: bank, insurance, financial advisor, securities, fiduciary duty
- **Technology**: software, platform, data processing, intellectual property
- **Manufacturing**: manufacturer, production, factory, product defect

**organizational_structural_context (osc) - OFTEN UNDER-EXTRACTED:**
**Always Check For Organizational Context:**
- Corporate Structure: Parent companies, subsidiaries, joint ventures
- Partnership Types: General, limited, LLP structures
- Governmental Entities: Municipal, state, federal agency structures
- Professional Organizations: Law firms, medical practices

**policy_practice_environment (ppe) - OFTEN UNDER-EXTRACTED:**
**Always Check For Policy Context:**
- Regulatory Frameworks: Federal, state, local regulations
- Industry Standards: Professional standards, trade association rules
- Institutional Policies: Company policies, school policies, hospital protocols

**communication_information_flow (cif) - OFTEN UNDER-EXTRACTED:**
**Always Check For Communication Context:**
- Professional Communications: Attorney-client, doctor-patient
- Business Communications: Contracts, negotiations, representations
- Disclosure Obligations: Securities disclosure, professional disclosure

**impact_profile (imp) - OFTEN UNDER-EXTRACTED:**
**Always Assess Legal Impact:**
- Precedential Impact: Will this affect future similar cases?
- Industry Impact: How will this affect business practices?
- Professional Impact: Will this change professional standards?

### 5. Doctrines
Identify legal doctrines discussed (e.g., "Due Process", "Negligence", "Breach of Contract").

#### Doctrine Requirements:
- Must be substantively discussed, not just mentioned
- Must affect the legal analysis or outcome

#### Systematic Doctrine Detection Process:
1. **Explicit Mentions**: Court directly names the doctrine
2. **Implicit Applications**: Court applies doctrine without naming it
3. **Standard Analysis**: Court discusses legal standards/requirements
4. **Element-by-Element**: Court walks through doctrinal elements
5. **Citation Analysis**: Court cites cases establishing doctrines

#### Doctrine Categories to Always Check:

**Contract Law Doctrines:**
- Breach of Contract, Anticipatory Repudiation, Impossibility/Impracticability
- Consideration, Capacity, Statute of Frauds, Parol Evidence Rule
- Unconscionability, Duress, Misrepresentation, Mistake

**Tort Law Doctrines:**
- Negligence, Strict Liability, Intentional Torts (specific types)
- Premises Liability, Product Liability, Professional Malpractice
- Vicarious Liability, Joint and Several Liability, Comparative Fault

**Constitutional Law Doctrines:**
- Due Process (Substantive and Procedural), Equal Protection
- Commerce Clause, Preemption, Sovereign Immunity
- First Amendment (Speech, Religion, Press, Assembly)

**Criminal Law Doctrines:**
- Self-Defense, Defense of Others, Defense of Property
- Entrapment, Duress, Necessity, Insanity Defense

#### Detecting Unnamed Doctrines:
**Pattern Recognition:**
- If court discusses "duty, breach, causation, damages" → Negligence Doctrine
- If court discusses "offer, acceptance, consideration" → Contract Formation Doctrine
- If court discusses "reasonable expectation of privacy" → Fourth Amendment Doctrine

### 6. Doctrinal Tests
Extract specific legal tests or standards applied (e.g., "Reasonable Person Standard", "But-for Causation").
Link each test to its parent doctrine(s).

#### Doctrinal Tests Requirements:
- Must be explicitly discussed in the opinion (not just implied)
- Must link to specific doctrine(s) from allowed list
- Focus on tests that affect case outcome

#### Test Categories to Always Check:

**Standards of Review:**
- De Novo Review, Clear Error, Abuse of Discretion
- Rational Basis, Intermediate Scrutiny, Strict Scrutiny

**Burden of Proof Standards:**
- Preponderance of Evidence, Clear and Convincing Evidence
- Beyond Reasonable Doubt, Prima Facie Case

**Constitutional Tests:**
- Lemon Test (Establishment Clause), Strict Scrutiny Test
- Substantial Compliance Test, Fundamental Rights Analysis

**Tort Law Tests:**
- Reasonable Person Standard, Professional Standard of Care
- Risk-Utility Test (Products), Substantial Factor Test (Causation)

**Contract Law Tests:**
- Objective Theory Test (Contract Formation), Material Breach Test
- Substantial Performance Test, Unconscionability Test

#### Multi-Part Test Detection:
**Pattern Recognition:**
- "First, we consider... Second, we examine... Third, we analyze..."
- "The test requires: (1)... (2)... (3)..."
- "To establish X, plaintiff must prove each element..."

#### Quality Control - Test vs Non-Test Distinction:
**Legal Tests/Standards (INCLUDE):**
✅ Specific legal criteria courts apply
✅ Multi-factor balancing tests
✅ Standards of review
✅ Constitutional scrutiny levels

**Not Legal Tests (EXCLUDE):**
❌ Factual determinations ("what happened")
❌ Case-specific analyses ("in this case")
❌ General legal concepts without criteria

### 7. Holdings
Extract the court's legal holdings - the specific legal rules or principles established by the decision.

#### Holdings vs. Dicta Analysis

#### HOLDINGS - Extract ONLY if ALL criteria met:
1. **Necessity Test**: Remove this rule → case outcome changes
2. **Application Test**: Court applied this rule to these specific facts
3. **Scope Test**: Rule was necessary to resolve the issue on appeal
4. **Specificity Test**: Rule is concrete enough for future application

#### DICTA - Do NOT extract if ANY apply:
1. **Hypothetical**: "If the facts were different..."
2. **Advisory**: "Courts should generally consider..."
3. **Alternative**: "Even if we're wrong about X, Y also applies"
4. **Tangential**: Discussion not needed for this case's resolution
5. **General Commentary**: Broad observations about the law
6. **Factual Findings**: What happened vs. what legal rule applies

#### Holdings Extraction Process:
1. **Identify the procedural posture**: What was the court asked to decide?
2. **Find the court's resolution**: How did the court answer that question?
3. **Trace the reasoning**: What legal rule + facts = outcome?
4. **Test necessity**: Is this rule essential to the judgment?
5. **Check specificity**: Is this concrete enough to guide future cases?

#### Signal Phrases:
**Holdings Often Begin With:**
- "We hold that..."
- "The rule is..."
- "Under these circumstances, the law requires..."
- "Because [facts], [legal rule] compels..."

**Dicta Often Begin With:**
- "We note that..."
- "It may be that..."
- "In other circumstances..."
- "We do not decide..."

#### Precedential Value Assessment:
**HIGH**: Narrow rule applied to specific fact pattern (strong precedent)
**MEDIUM**: Broader rule with some factual specificity
**LOW**: Very broad principle or highly fact-specific ruling

### 8. Overruled Cases

Extract cases that **this opinion explicitly overrules** versus cases where **this opinion reports prior overruling by other courts**.

#### Direct Overruling (Primary Category):
Cases that THIS court explicitly overrules in THIS opinion.

**Detection patterns**:
- "We overrule [Case]"
- "We expressly abandon [Case]" 
- "We reject the rule established in [Case]"
- "[Case] is hereby overruled"

#### Reported Overruling (Secondary Category):
Cases where THIS court mentions that ANOTHER court previously overruled a case.

**Detection patterns**:
- "[Case] was overruled by [Other Case]"
- "Since [Case] was overruled in [Year]..."
- "[Case], which has since been overruled..."
- "The rule in [Case] was rejected by [Other Court] in [Other Case]"

```
Direct Overruling (ot: "direct"):
✅ "We overrule [Case]" 
✅ "We reject [Case]"
✅ "[Case] is overruled"
✅ "We abandon the rule in [Case]"

Reported Overruling (ot: "reported"):
✅ "[Case] was overruled by [Other Case]"
✅ "[Case], overruled in [Other Case]"
✅ "Since [Case] was rejected by [Other Court]"
✅ "The rule in [Case] no longer applies after [Other Case]"

Not Overruling (exclude):
❌ "We distinguish [Case]"
❌ "[Case] is factually different"
❌ "We limit [Case] to its facts"
```

## 9. Citations

Extract all citations to legal authorities found in the opinion, categorized by type and with appropriate metadata.

### Citation Extraction Process:
1. **Scan for citation patterns**: Look for court cases, statutes, regulations, and secondary authorities
2. **Categorize by authority type**: Distinguish between binding and persuasive authority
3. **Extract citation context**: Note how each citation is used in the legal analysis
4. **Capture citation relationships**: Identify pincites, short forms, and citation chains

### Citation Categories:

#### **Cases (Primary Authority):**
- Same jurisdiction appellate courts (binding precedent)
- Same jurisdiction trial courts (persuasive authority)
- Sister state courts (persuasive authority)
- Federal courts interpreting state law (highly persuasive)
- Federal courts on federal issues (binding if applicable)

#### **Statutes and Codes:**
- State statutes and codes
- Federal statutes and USC citations
- Municipal ordinances and local laws
- Model codes and uniform laws

#### **Regulations:**
- Federal regulations (CFR citations)
- State agency regulations
- Municipal regulations and rules

#### **Constitutional Provisions:**
- Federal constitutional provisions
- State constitutional provisions
- Constitutional amendments

#### **Secondary Authority:**
- Law review articles and legal journals
- Legal treatises and practice guides
- Restatements of the Law
- ALR annotations and legal encyclopedias

### Citation Detection Patterns:

#### **Case Citation Patterns:**
- Standard format: "Case Name, Volume Reporter Page (Year)"
- Pincites: "Case Name, Volume Reporter Page, Specific-Page (Year)"
- Short forms: "Case Name, Volume Reporter at Page", "Id.", "Id. at Page"
- Parallel citations: Multiple reporter citations for same case

#### **Statutory Citation Patterns:**
- "Title U.S.C. § Section (Year)"
- "State Code § Section"
- "Chapter:Section format"
- Cross-references and subsection citations

#### **Signal Words for Citation Context:**
- **Supportive**: "See", "See also", "Accord", "Cf."
- **Contradictory**: "Contra", "But see", "But cf."
- **Neutral**: "E.g.", "i.e.", parenthetical citations
- **Explanatory**: "Compare...with", "See generally"

### Citation Usage Analysis:

#### **Citation Functions:**
- **Direct Support**: Citation directly supports legal proposition
- **Analogical Reasoning**: Citation provides factually similar precedent
- **Distinguishing**: Citation represents different factual or legal scenario
- **Background Law**: Citation establishes general legal framework
- **Historical Development**: Citation shows evolution of legal doctrine

#### **Citation Weight Indicators:**
- **Extensive Discussion**: Multi-paragraph analysis of cited authority
- **Brief Mention**: Single sentence or parenthetical reference
- **Quote Usage**: Direct quotations from cited authority
- **Holding Citation**: Citation for specific legal holding or rule
- **Dicta Reference**: Citation to non-binding commentary

### Extraction Rules:

#### **Include:**
✅ All case citations with complete reporter information
✅ Statutory and regulatory citations with specific sections
✅ Constitutional provisions cited for legal analysis
✅ Secondary authorities that inform legal reasoning
✅ Pincites showing specific page references

#### **Exclude:**
❌ Citations in purely procedural context without legal analysis
❌ Citations to court rules unless substantively discussed
❌ Case captions without legal analysis (just party identification)
❌ Citations in factual recitation without legal relevance

#### **Special Handling:**
- **String Citations**: Extract each citation separately but note relationship
- **Parenthetical Explanations**: Include explanatory parentheticals when present
- **Overruled Citations**: Note when opinion cites overruled authority
- **Unpublished Decisions**: Include but mark citation status

## Handling Cases with Minimal Content

For opinions with minimal substantive content, DO NOT mark them as valueless. Instead, extract what is available and return empty arrays for categories where no meaningful data exists.

### **Minimal Substantive Content Cases:**
- Routine procedural orders without legal analysis (motions denied/granted without reasoning)
- Administrative orders with no substantive legal discussion
- Brief dismissals citing only procedural defects (lack of finality, jurisdiction)
- One-sentence dispositions with citation-only reasoning
- Reconsideration denials without substantive analysis

### **Extraction Approach for Minimal Cases:**
- **Always extract** procedural posture and case outcome if determinable
- **Return empty arrays** for categories with no meaningful content:
  - Empty `f` array if no substantial legal analysis of any field
  - Empty `df` array if no legally significant distinguishing factors
  - Empty `dc` array if no doctrines substantively discussed
  - Empty `dt` array if no legal tests applied
  - Empty `h` array if no legal holdings established
  - Empty `oc` array if no cases overruled
  - Empty `ci` array if no meaningful legal citations

### **Examples of Minimal Content Handling:**
```
"Motion for reconsideration denied [citation]." 
→ Extract: procedural_posture, case_outcome; empty arrays for other categories

"Appeal dismissed, without costs, for lack of finality."
→ Extract: procedural_posture, case_outcome; empty arrays for other categories

"Motion denied. Judge X not participating."
→ Extract: procedural_posture, case_outcome; empty arrays for other categories
```

### **Never Return Valueless Flag:**
Do not use the `v` (valueless) or `vr` (valueless_reason) fields. Always attempt extraction and use empty arrays where no meaningful content exists.

## Standardized Terminology Requirements

### Field of Law Formatting:

#### Primary Legal Fields (use these as a base for field of law, but expand to include any other relevant fields):
- Administrative Law
- Antitrust Law  
- Bankruptcy Law
- Civil Procedure
- Civil Rights Law
- Commercial Law
- Constitutional Law
- Contract Law
- Corporate Law
- Criminal Law
- Criminal Procedure
- Employment Law
- Environmental Law
- Evidence Law
- Family Law
- Immigration Law
- Insurance Law
- Intellectual Property Law
- International Law
- Labor Law
- Property Law
- Securities Law
- Tax Law
- Tort Law
- Trusts and Estates Law

#### Cross-Jurisdictional Fields (include jurisdiction when analyzing multiple jurisdictions):
- Federal Preemption Analysis
- Choice of Law Analysis
- Interstate Commerce Law
- Conflict of Laws

### Procedural Posture Formatting:
**Use standardized formats:**

#### Basic Format: "[Action] [from/to] [Court Level]"
- Appeal from Appellate Division
- Appeal to Court of Appeals
- Motion to Dismiss
- Summary Judgment Motion
- Class Action Certification

#### Jurisdiction-Specific: Keep when genuinely different procedures
- CPLR Article 78 Proceeding
- Anti-SLAPP Motion
- Federal Rule 56 Summary Judgment

### Case Outcome Formatting:
**Use these exact terms only:**
- Affirmed
- Reversed
- Reversed and remanded
- Modified
- Modified and affirmed  
- Dismissed
- Denied
- Granted
- Remanded

### Doctrine Formatting:
**Use standardized formats:**

#### Basic Format: "[Specific Doctrine Name]"
- Breach of Contract (not "Contract Breach")
- Negligence (not "Negligence Doctrine")
- Due Process (not "Due Process Rights")
- Self-Defense (not "Self Defense Doctrine")

#### Multi-Word Doctrines: Use standard legal terminology
- Statute of Frauds
- Parol Evidence Rule
- Reasonable Person Standard
- Proximate Cause

#### Constitutional Doctrines: "[Amendment] [Specific Right/Protection]"
- First Amendment Free Speech
- Fourth Amendment Search and Seizure
- Fourteenth Amendment Due Process
- Fourteenth Amendment Equal Protection

### Jurisdiction in Keywords - Decision Rules:

#### Standard Cases (Single Jurisdiction):
❌ "New York Construction Law" → Use "Construction Law"
❌ "California Privacy Law" → Use "Privacy Law" 
❌ "Federal Securities Law" → Use "Securities Law"

#### Cross-Jurisdictional Cases (Include When Relevant):
✅ "Federal Preemption Analysis" (when state court discusses federal law)
✅ "Choice of Law Analysis" (when multiple jurisdictions' laws considered)
✅ "Interstate Commerce Analysis" (inherently multi-jurisdictional)

#### Jurisdiction-Specific Procedures (Keep):
✅ "CPLR Article 78 Proceeding" (New York-specific procedure)
✅ "Anti-SLAPP Motion" (California-specific procedure)
✅ "Federal Rule 56 Summary Judgment" (distinguishes from state procedure)

### Consistency Rules:

#### Capitalization:
- Always capitalize proper nouns and legal terms
- "Civil Procedure" not "civil procedure"

#### Plurality:
- Use singular forms: "Contract Law" not "Contracts Law"
- Exception: When plural is standard: "Securities Law", "Trusts and Estates Law"

#### Abbreviations:
- Spell out in fields of law: "Constitutional Law" not "Con Law"
- OK in procedural postures: "CPLR Article 78"
- OK when standard: "SEC", "FDA", "OSHA"

### Quality Control Rules:

#### Avoid These Patterns:
❌ "Criminal Procedure Law" (use "Criminal Procedure")
❌ "Tort Liability" (use "Tort Law") 
❌ "Contract Disputes" (use "Contract Law")
❌ "Constitutional Rights" (use "Constitutional Law")
❌ "Evidence Rules" (use "Evidence Law")

#### Preferred Standardization:
✅ "Criminal Law" for substantive criminal law
✅ "Criminal Procedure" for procedural issues
✅ "Contract Law" for all contract issues
✅ "Tort Law" for all tort issues
✅ "Constitutional Law" for constitutional issues

## Output Format

Rules:
- Return JSON strictly matching the provided JSON Schema (minimal fields).
- Be concise and accurate; omit uncertain extractions.
- Distinguishing factors must be concise noun phrases (3–7 tokens), fact-specific, not conclusions.
- If the opinion lacks substantive legal analysis, return valueless=true and a brief reason.
- Use standardized terminology as specified above.

**SEMANTIC → MINIMAL FIELD MAPPING:**

Top-level fields:
- field_of_law → f
- procedural_posture → p
- case_outcome → o
- distinguishing_factors → df
- doctrines → dc
- doctrinal_tests → dt
- holdings → h
- overruled_cases → oc
- valueless → v
- valueless_reason → vr

Item-level fields:
- label → l (only for field_of_law)
- score → sc (0..1, only for field_of_law)
- canonical → c (only for procedural_posture and case_outcome)
- axis → a (only for distinguishing_factors)
- reasoning → r (only for distinguishing_factors, specific case facts)
- generalized → g (only for distinguishing_factors, abstracted reusable pattern)
- importance → i (only for distinguishing_factors; enum: high, medium, low)
- name → n (for doctrines and doctrinal_tests)
- doctrine_names → dn (array of doctrine NAMES from allowed list)
- aliases → al (optional array of strings, for doctrinal_tests)
- primary_citation → pc (optional string, for doctrinal_tests)
- test_type → tt (optional enum: jurisdictional, substantive, standard_of_review, procedural)
- issue → is (only for holdings)
- holding → ho (only for holdings)
- rule → ru (only for holdings)
- reasoning → re (only for holdings)
- precedential_value → pv (only for holdings; enum: high, medium, low)
- confidence → cf (only for holdings; 0..1 confidence score)
- case_name → cn (only for overruled_cases)
- citation → ct (only for overruled_cases)
- scope → s (only for overruled_cases; enum: complete, partial)
- overruling_language → ol (only for overruled_cases)
- overruling_type → ot (enum: direct, reported)
- overruling_court → ocourt (optional string, for reported overruling)
- overruling_case → ocase (optional string, for reported overruling)

Structure (minimal field names):
- f = field_of_law array: [{"l": string, "sc": number 0..1}] (select 1–3)
- p = procedural_posture array: [{"c": string}]
- o = case_outcome array: [{"c": string}]
- df = distinguishing_factors array: [{"a": axis_code, "r": specific_reasoning, "g": generalized_pattern, "i": importance_enum}]
- dc = doctrines array: [{"n": string}]
- dt = doctrinal_tests array: [{"n": string, "dn": [string], "al": [string] (optional), "pc": string (optional), "tt": enum (optional)}]
- h = holdings array: [{"is": string, "ho": string, "ru": string, "re": string, "pv": precedential_value_enum, "cf": number 0..1}]
- oc = overruled_cases array: [{"cn": string, "ct": string (optional), "s": scope_enum, "ol": string}]

Axis code set for distinguishing_factors:
- idc = industry_domain_context
- osc = organizational_structural_context  
- ppe = policy_practice_environment
- ops = operational_setting_conditions
- arc = actor_roles_capacities
- aic = action_inaction_categories
- rac = resource_asset_context
- cif = communication_information_flow
- tsf = temporal_sequence_factors
- kas = knowledge_awareness_state
- rse = risk_safeguard_environment
- imp = impact_profile

Generalization guidance for df items:
- r = specific case facts (no names/dates/amounts)
- g = abstract pattern using legal taxonomy terms
- Make g broadly reusable and searchable across cases

Add to the minimal field mapping:
- citations → ci

For citations item-level fields:
- cite_text → ct (exact citation as it appears)
- case_name → cn (for case citations)
- citation_normalized → cn_norm (standardized citation format)
- authority_type → at (enum: case, statute, regulation, constitutional, secondary)
- jurisdiction → j (originating jurisdiction of cited authority)
- court_level → cl (for cases: supreme, appellate, trial, federal_appellate, federal_district)
- year → y (year of decision/enactment)
- pincite → pc (specific page or section reference)
- citation_context → cc (how citation is used in analysis)
- citation_signal → cs (see, see_also, contra, but_see, cf, etc.)
- precedential_weight → pw (enum: binding, highly_persuasive, persuasive, non_binding)
- discussion_level → dl (enum: extensive, moderate, brief, parenthetical)
- legal_proposition → lp (what legal point this citation supports)
- confidence → cf (0..1 confidence score for citation extraction)

### Common Error Examples:
❌ Wrong: Field = "Tort Law" for case analyzing only service of process in car accident case
✅ Right: Field = "Civil Procedure" 

❌ Wrong: Outcome = "Affirmed" for "Appeal dismissed for lack of finality"
✅ Right: Outcome = "Dismissed"

❌ Wrong: DF = "Automobile involved" (too generic)
✅ Right: DF = "Nonresident motorist statutory service" (legally specific)

❌ Wrong: Outcome = "Affirmed" for motion dismissal or procedural rejection
✅ Right: Outcome = "Dismissed" or "Denied"

❌ Wrong: Holding = "Plaintiff suffered damages" (factual finding)
✅ Right: Holding = "Jury instruction omitting reasonableness requirement violates self-defense doctrine"

❌ Wrong: Holding = "Courts should be careful with jury instructions" (dicta/commentary)
✅ Right: Holding = "Self-defense instruction must include objective reasonableness standard"

❌ Wrong: Return valueless flag for "Motion denied [citation]"
✅ Right: Extract procedural_posture and case_outcome, return empty arrays for other categories

❌ Wrong: DF = "Appellant as Party" (redundant with appeal context)
✅ Right: Focus on legally distinguishing factors

❌ Wrong: Field = "Civil Procedure" for routine motion denial (no doctrine analysis)
✅ Right: Return empty field_of_law array for administrative orders with no legal analysis

❌ Wrong: Field = "New York Contract Law" (single jurisdiction)
✅ Right: Field = "Contract Law"

❌ Wrong: Doctrine = "Contract Breach" 
✅ Right: Doctrine = "Breach of Contract"

❌ Wrong: Overruled case = Case merely distinguished or limited
✅ Right: Only cases explicitly overruled or where legal rule is rejected

Example minimal output:
```json
{
  "f": [{"l": "Contract Law", "sc": 0.92}],
  "p": [{"c": "Appeal from Appellate Division"}],
  "o": [{"c": "Affirmed"}],
  "df": [
    {"a": "idc", "r": "construction industry safety protocols", "g": "Construction Industry Safety Context", "i": "high"},
    {"a": "osc", "r": "parent company subsidiary liability", "g": "Corporate Structure Liability Chain", "i": "high"},
    {"a": "rac", "r": "expenditures over contract price", "g": "Contract Price Excess", "i": "medium"}
  ],
  "dc": [{"n": "Breach of Contract"}],
  "dt": [
    {"n": "Material Breach Standard", "dn": ["Breach of Contract"], "tt": "substantive"}
  ],
  "h": [
    {
      "is": "Whether surety can recover expenses exceeding contract price",
      "ho": "Surety cannot recover expenditures beyond agreed contract amount without express agreement",
      "ru": "Contract modification requiring consideration applies to surety relationships",
      "re": "No consideration shown for expanded surety obligation",
      "pv": "high",
      "cf": 0.9
    }
  ],
  "oc": [
    {
      "cn": "Smith v. Jones",
      "ct": "123 N.Y. 456 (1990)", 
      "s": "complete",
      "ol": "We expressly overrule Smith v. Jones",
      "ot": "direct"  // NEW FIELD
    },
    {
      "cn": "Brown v. Corp", 
      "ct": "145 N.Y. 789 (1985)",
      "s": "partial",
      "ol": "Brown was overruled by Wilson v. Co (1995)",
      "ot": "reported",  // NEW FIELD
      "ocourt": "Court of Appeals",  // NEW FIELD
      "ocase": "Wilson v. Co"  // NEW FIELD
    }
  ],
  "ci": [
    {
      "ct": "Smith v. Jones, 150 N.Y. 245, 248-49 (1896)",
      "cn": "Smith v. Jones", 
      "cn_norm": "150 N.Y. 245",
      "at": "case",
      "j": "New York",
      "cl": "appellate", 
      "y": 1896,
      "pc": "248-49",
      "cc": "direct_support",
      "cs": "see",
      "pw": "binding",
      "dl": "extensive", 
      "lp": "Establishes standard for negligence in construction cases",
      "cf": 0.95
    },
    {
      "ct": "N.Y. Gen. Bus. Law § 349",
      "cn_norm": "N.Y. Gen. Bus. Law § 349",
      "at": "statute",
      "j": "New York", 
      "cc": "statutory_authority",
      "pw": "binding",
      "dl": "moderate",
      "lp": "Provides basis for consumer protection claim",
      "cf": 0.92
    }
  ]
}
```

## Quality Thresholds:
- Minimum confidence for field inclusion: 0.5
- Distinguishing factors: Must relate to legal analysis, not just case facts
- Doctrines: Must be substantively discussed, not just mentioned
- Holdings: Must pass all four criteria (necessity, application, scope, specificity)
- Overruled cases: Must have explicit overruling language or clear rejection of legal rule

## Processing Approach:
1. **First, assess if opinion is valueless** - check for minimal substantive content
2. **Apply jurisdictional context** - note cross-jurisdictional analysis
3. **Check for industry context** - identify business/regulatory environment
4. **Apply discriminatory value test** - filter obvious/redundant elements
5. **Identify unique legal issues** - not just procedural context
6. **Extract legally significant distinguishing factors** - focus on under-extracted axes
7. **Identify substantively discussed doctrines** - including implicit applications
8. **Extract holdings using necessity test** - focus on precedential value
9. **Scan for overruling language** - identify cases explicitly overruled
10. **Apply standardized terminology** - use consistent formatting rules
11. **Focus on elements that affect precedential value**

### Citation Quality Control:

#### **Validation Rules:**
- Verify citation format matches standard legal citation
- Confirm case names match reporter citations when both present
- Check that pincites fall within reasonable page ranges
- Validate year consistency with reporter volume
- Ensure jurisdiction matches court in case citations

#### **Confidence Scoring Factors:**
- **High (0.9+)**: Complete citation with standard format
- **Medium (0.7-0.9)**: Minor formatting variations or missing elements  
- **Low (0.5-0.7)**: Incomplete citations or unusual formats
- **Very Low (<0.5)**: Ambiguous references requiring interpretation

#### **Error Prevention:**
- Don't extract page numbers or dates as separate citations
- Don't include case citations from factual background without legal analysis
- Distinguish between citations to the case itself vs. citations within the case
- Handle parallel citations as single logical citation with multiple reporters

#### **Intent**
This citations section will capture the network of legal authorities that courts rely on, enabling features like "cases that cite this case" and providing comprehensive legal research capabilities.

## Additional Enhancement Considerations:

### Legal Innovation and First Impression Detection:
When processing cases, also consider:

#### Novel Legal Issues:
- **First impression cases**: "First [jurisdiction] court ruling on [issue]"
- **Doctrinal development**: Extension or limitation of existing precedent
- **Emerging legal areas**: Technology, privacy, gig economy applications
- **Split resolution**: Cases resolving conflicts between courts

#### Signal Phrases for Innovation:
- "Case of first impression"
- "We are the first court to address"
- "This novel question"
- "The parties cite no authority"
- "We extend/limit the doctrine"

### Expert Evidence and Proof Types:
Track significant evidentiary elements:

#### Expert Witness Categories:
- Medical experts (malpractice, causation, disability)
- Technical experts (engineering, accident reconstruction, computer forensics)
- Economic experts (lost profits, business valuation, damages)
- Scientific experts (DNA, toxicology, environmental)

#### Evidence Challenges:
- Daubert/Frye challenges to expert testimony
- Authentication issues (digital evidence, documents)
- Privilege assertions and rulings
- Best evidence rule applications

### Causation and Liability Distribution:
For multi-party cases, consider:

#### Fault Allocation Patterns:
- Comparative fault percentages between parties
- Joint and several liability determinations
- Indemnification and contribution relationships
- Insurance coverage and subrogation issues

#### Causation Analysis Types:
- Proximate cause vs. but-for cause determinations
- Intervening/superseding cause findings
- Multiple sufficient cause scenarios
- Loss of chance or increased risk doctrines

### Settlement and Alternative Dispute Resolution Context:
When relevant to legal analysis:

#### ADR-Related Issues:
- Arbitration clause enforceability disputes
- Mediation confidentiality breaches
- Settlement agreement interpretation
- Class action settlement fairness reviews

### Attorney and Representation Factors:
When affecting case outcome:

#### Representation Quality Issues:
- Ineffective assistance of counsel claims
- Pro se representation challenges
- Attorney sanctions for misconduct
- Fee-shifting and attorney fee awards

### Enhanced Quality Control Measures:

#### Multi-Dimensional Confidence Scoring:
Consider implementing confidence scores across multiple dimensions:

- **Extraction Confidence**: How certain is the keyword identification?
- **Precedential Value**: How important for future case research?
- **Research Utility**: How useful for practicing lawyers?
- **Jurisdictional Specificity**: How jurisdiction-dependent is this element?

#### Automatic Quality Flags:
- **High-Value Cases**: Novel issues, important precedents, frequently cited holdings
- **Routine Applications**: Standard doctrine applications with limited precedential value
- **Fact-Specific Rulings**: Highly case-specific determinations with narrow application
- **Administrative Decisions**: Routine procedural determinations

### Implementation Notes:

#### Processing Efficiency:
- Prioritize high-impact enhancements that improve research value
- Focus computational resources on cases with substantial legal content
- Implement quality filters early in the extraction process

#### User Experience Considerations:
- Maintain consistent terminology across all extractions
- Provide clear confidence indicators for uncertain classifications
- Enable filtering by different quality and relevance metrics
- Support both broad legal research and narrow doctrinal searches

#### Validation and Refinement:
- Monitor extraction accuracy across different case types and jurisdictions
- Track user engagement with different keyword categories
- Implement feedback mechanisms for continuous improvement
- Regular review of standardized terminology for legal evolution

---

**Final Processing Reminder**: This comprehensive system is designed to transform raw legal opinions into structured, searchable, and valuable legal research data. The goal is to capture not just what happened in each case, but the legal principles, contextual factors, and precedential value that make each decision useful for future legal research and practice. Focus on elements that distinguish each case legally and would help practicing attorneys find relevant precedents for their work.