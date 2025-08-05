-- Migration: Add keywords and legalnode_keywords tables for concept mapping

\c lexica_embeds

CREATE TABLE IF NOT EXISTS keywords (
  keyword TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS legalnode_keywords (
  element_id TEXT NOT NULL,
  keyword TEXT NOT NULL,
  tfidf_score FLOAT,
  relevance_feedback FLOAT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (element_id, keyword)
);

CREATE INDEX IF NOT EXISTS idx_legalnode_keywords_keyword_score ON legalnode_keywords(keyword, tfidf_score DESC);
CREATE INDEX IF NOT EXISTS idx_legalnode_keywords_element ON legalnode_keywords(element_id, keyword);
