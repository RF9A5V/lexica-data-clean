-- 1. (Optional) Create the database itself (run this in psql, not inside a transaction)
-- CREATE DATABASE ny_court_of_appeals;

-- 2. Connect to the database
\c ny_court_of_appeals

-- 3. Enable pgvector extension for embeddings (if using vector type)
CREATE EXTENSION IF NOT EXISTS vector;

-- 4. Table: Cases
CREATE TABLE cases (
    id SERIAL PRIMARY KEY,
    case_name TEXT,
    case_name_full TEXT,
    citations JSONB,
    date_created TIMESTAMP,
    date_filed DATE,
    date_filed_is_approximate BOOLEAN,
    date_modified TIMESTAMP,
    sub_opinions JSONB,
    citation_count INTEGER
);

CREATE TABLE opinions (
    id SERIAL PRIMARY KEY,
    case_id INTEGER REFERENCES cases(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    url TEXT,
    binding_type TEXT,
    substantial BOOLEAN
);

-- 5. Table: Opinion Paragraphs
CREATE TABLE opinion_paragraphs (
    id SERIAL PRIMARY KEY,
    opinion_id INTEGER REFERENCES opinions(id) ON DELETE CASCADE,
    raw_text TEXT,
    embedding VECTOR(768) -- Adjust dimension to your embedding size
);

-- 6. Table: Classification (FIRAC)
CREATE TABLE classification (
    id SERIAL PRIMARY KEY,
    label TEXT UNIQUE -- e.g. 'Facts', 'Issue', 'Rule', 'Application', 'Conclusion'
);

-- 7. Table: Keywords
CREATE TABLE keywords (
    id SERIAL PRIMARY KEY,
    keyword TEXT
);

-- 8. Table: Paragraph-Keyword Join Table
CREATE TABLE paragraph_keywords (
    id SERIAL PRIMARY KEY,
    paragraph_id INTEGER REFERENCES opinion_paragraphs(id) ON DELETE CASCADE,
    keyword_id INTEGER REFERENCES keywords(id) ON DELETE CASCADE,
    similarity FLOAT NOT NULL, -- cosine similarity value, e.g. 0.0 - 1.0
    UNIQUE (paragraph_id, keyword_id)
);

-- -- Classification lookup table for FIRAC categories and subcategories
CREATE TABLE firac_classifications (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(100),
    description TEXT,
    sort_order INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique category/subcategory combinations
    UNIQUE(category, subcategory)
);

-- Insert the FIRAC classification categories
INSERT INTO firac_classifications (category, subcategory, description, sort_order) VALUES
-- Procedural History
('PROCEDURAL_HISTORY', NULL, 'All procedural events in chronological order', 10),
('PROCEDURAL_HISTORY', 'PRE_TRIAL', 'Pre-trial motions and rulings', 11),
('PROCEDURAL_HISTORY', 'TRIAL_PROCEEDINGS', 'Trial court proceedings and decisions', 12),
('PROCEDURAL_HISTORY', 'JURY_INSTRUCTIONS', 'Jury instructions and verdicts', 13),
('PROCEDURAL_HISTORY', 'POST_TRIAL', 'Post-trial motions', 14),
('PROCEDURAL_HISTORY', 'APPELLATE_PROCEEDINGS', 'Appellate court proceedings', 15),
('PROCEDURAL_HISTORY', 'PRESERVATION', 'Preservation of issues for appeal', 16),
('PROCEDURAL_HISTORY', 'CONSOLIDATION', 'Case consolidation information', 17),

-- Facts
('FACTS', NULL, 'Factual statements from the opinion', 20),
('FACTS', 'SUBSTANTIVE', 'Events, actions, and circumstances forming basis of legal dispute', 21),
('FACTS', 'PROCEDURAL', 'What happened during litigation', 22),
('FACTS', 'CONFLICTING_TESTIMONY', 'Conflicting witness testimony or evidence', 23),

-- Issues
('ISSUES', NULL, 'Legal questions addressed by the court', 30),
('ISSUES', 'PRIMARY', 'Main legal issues addressed', 31),
('ISSUES', 'SUBSIDIARY', 'Secondary or related legal issues', 32),
('ISSUES', 'LAW_QUESTIONS', 'Pure questions of law', 33),
('ISSUES', 'FACT_QUESTIONS', 'Questions of fact or mixed law/fact', 34),
('ISSUES', 'PRESERVED', 'Issues properly preserved for appeal', 35),
('ISSUES', 'UNPRESERVED', 'Issues not preserved for appeal', 36),

-- Rules
('RULES', NULL, 'Legal principles and standards', 40),
('RULES', 'ESTABLISHED_LAW', 'Existing legal rules cited or applied', 41),
('RULES', 'NEW_RULES', 'New legal standards announced by the court', 42),
('RULES', 'REVIEW_STANDARDS', 'Appellate review standards', 43),
('RULES', 'ELEMENTS', 'Required elements of causes of action or defenses', 44),
('RULES', 'STATUTORY', 'Statutory interpretation or application', 45),

-- Analysis
('ANALYSIS', NULL, 'Courts reasoning and application of law to facts', 50),
('ANALYSIS', 'COURT_REASONING', 'How the court applies legal rules to facts', 51),
('ANALYSIS', 'CASE_DISTINCTION', 'How court distinguishes or follows precedent', 52),
('ANALYSIS', 'POLICY_CONSIDERATIONS', 'Policy reasoning provided by court', 53),
('ANALYSIS', 'ALTERNATIVE_HOLDINGS', 'Secondary grounds for decision', 54),

-- Conclusion
('CONCLUSION', NULL, 'Courts holdings and dispositive rulings', 60),
('CONCLUSION', 'HOLDINGS', 'Courts answers to legal issues presented', 61),
('CONCLUSION', 'DISPOSITION', 'What the court orders (affirm, reverse, remand)', 62),
('CONCLUSION', 'REMEDIAL_ORDERS', 'Specific relief granted', 63);

-- Extracted sentences table
CREATE TABLE opinion_sentences (
    id SERIAL PRIMARY KEY,
    opinion_id INTEGER NOT NULL,
    classification_id INTEGER NOT NULL,
    sentence_text TEXT NOT NULL,
    embedding_vector VECTOR(768), -- For storing embeddings (adjust dimension as needed)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (opinion_id) REFERENCES opinions(id) ON DELETE CASCADE,
    FOREIGN KEY (classification_id) REFERENCES firac_classifications(id)
);

-- Indexes for efficient querying
CREATE INDEX idx_opinion_sentences_opinion_id ON opinion_sentences(opinion_id);
CREATE INDEX idx_opinion_sentences_classification_id ON opinion_sentences(classification_id);
CREATE INDEX idx_opinion_sentences_category ON opinion_sentences(classification_id) 
    INCLUDE (opinion_id);

-- Composite index for classification + opinion queries
CREATE INDEX idx_opinion_sentences_opinion_classification ON opinion_sentences(opinion_id, classification_id);

-- Index for vector similarity search (if using pgvector)
CREATE INDEX idx_opinion_sentences_embedding ON opinion_sentences 
    USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100);

-- View for easy querying with classification details
CREATE VIEW opinion_sentences_with_classification AS
SELECT 
    os.id,
    os.opinion_id,
    os.sentence_text,
    fc.category,
    fc.subcategory,
    fc.description as classification_description,
    os.created_at
FROM opinion_sentences os
JOIN firac_classifications fc ON os.classification_id = fc.id
ORDER BY os.opinion_id, fc.sort_order;