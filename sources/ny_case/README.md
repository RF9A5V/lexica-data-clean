# NY Court of Appeals Keyword Extraction System

This directory contains the complete keyword extraction and search system for NY Court of Appeals legal opinions.

## 🏗️ System Architecture

### Database Schema
- **keywords**: Normalized keyword storage with frequency tracking
- **opinion_keywords**: Many-to-many relationship between opinions and keywords with relevance scores
- **sentence_keywords**: Granular keyword assignments at sentence level
- **search functions**: PostgreSQL functions for efficient keyword search

### Core Components

1. **Keyword Extraction Pipeline** (`batchKeywordExtraction.js`)
   - Extracts opinion text from database
   - Uses OpenAI GPT-4o-mini for intelligent keyword extraction
   - Categorizes keywords into legal domains
   - Stores with relevance scores and context

2. **Search Service** (`keywordSearchService.js`)
   - Provides flexible keyword search capabilities
   - Supports multiple search strategies (ANY, ALL, PHRASE)
   - Integrates with multi-source search system
   - Offers keyword suggestions and analytics

3. **Backend Integration**
   - Enhanced multi-source search service
   - ConnectionManager integration for database access
   - Real-time search execution tracking

## 🚀 Quick Start

### Prerequisites
- Node.js 18+
- PostgreSQL database with NY Appeals data
- OpenAI API key

### Setup

1. **Install Dependencies**
   ```bash
   cd /Users/byuugulbary/Projects/lexica/lexica-data/sources/ny_case
   npm install
   ```

2. **Configure Environment**
   ```bash
   # Add to /Users/byuugulbary/Projects/lexica/lexica_backend/.env
   OPENAI_API_KEY=your_openai_api_key_here
   ```

3. **Initialize Database Schema**
   ```bash
   psql postgresql://localhost/ny_court_of_appeals -f src/clean_keyword_schema.sql
   ```

4. **Test System**
   ```bash
   node src/testKeywordSystem.js
   ```

### Keyword Extraction

**Extract keywords from 50 opinions:**
```bash
node src/batchKeywordExtraction.js 50
```

**Extract keywords from 5 opinions (testing):**
```bash
node src/batchKeywordExtraction.js 5
```

**Process all pending opinions:**
```bash
node src/batchKeywordExtraction.js
```

### Search Testing

**Test keyword search service:**
```bash
node src/keywordSearchService.js
```

## 📊 Keyword Categories

The system extracts keywords in six legal categories:

### 1. Legal Doctrines
- Constitutional principles and amendments
- Common law doctrines and rules
- Legal standards and tests
- Burden of proof standards

**Examples:** `strict liability`, `due process`, `proximate cause`

### 2. Causes of Action
- Specific legal claims
- Elements of causes of action
- Defenses and affirmative defenses

**Examples:** `negligence`, `breach of contract`, `constitutional violation`

### 3. Procedural Terms
- Motion types and procedural postures
- Standards of review
- Discovery-related terms
- Trial and appellate procedures

**Examples:** `summary judgment`, `motion to dismiss`, `de novo review`

### 4. Subject Matter
- Areas of law
- Industry-specific terms
- Regulatory frameworks

**Examples:** `employment law`, `product liability`, `securities regulation`

### 5. Factual Contexts
- Key factual scenarios
- Industry or business contexts
- Party relationships

**Examples:** `automotive defect`, `employer-employee`, `medical malpractice`

### 6. Remedies and Relief
- Types of damages
- Equitable remedies
- Injunctive relief

**Examples:** `compensatory damages`, `injunctive relief`, `declaratory judgment`

## 🔍 Search Capabilities

### Search Strategies

**ANY (OR) Search:**
```javascript
searchByKeywords(['contract', 'negligence'], 'any')
// Returns opinions containing either "contract" OR "negligence"
```

**ALL (AND) Search:**
```javascript
searchByKeywords(['contract', 'breach'], 'all')
// Returns opinions containing both "contract" AND "breach"
```

**PHRASE Search:**
```javascript
searchByKeywords(['breach of contract'], 'phrase')
// Returns opinions containing the exact phrase "breach of contract"
```

### Search Options

```javascript
const options = {
  limit: 50,              // Max results per source
  minRelevance: 0.5,      // Minimum keyword relevance score
  sortBy: 'relevance',    // 'relevance' or 'date'
  categories: ['legal_doctrines'] // Filter by keyword category
};
```

### Advanced Features

**Keyword Suggestions:**
```javascript
const suggestions = await service.getKeywordSuggestions('neg', 10);
// Returns: ['negligence', 'negative', 'negotiation', ...]
```

**Popular Keywords by Category:**
```javascript
const popular = await service.getPopularKeywords('legal_doctrines', 20);
// Returns top 20 legal doctrine keywords by usage
```

**Search Statistics:**
```javascript
const stats = await service.getSearchStats();
// Returns: { total_opinions: 115940, opinions_with_keywords: 1250, ... }
```

## 🎯 Integration with Multi-Source Search

The keyword extraction system seamlessly integrates with the backend's multi-source search:

### Backend Integration Points

1. **Multi-Source Search Service** (`multi_source_search_service.ex`)
   - Uses `search_opinions_by_keywords()` database function
   - Supports all search strategies and options
   - Aggregates results across multiple legal databases

2. **ConnectionManager** (`connection_manager.ex`)
   - Manages database connections to legal sources
   - Handles connection pooling and failover
   - Executes keyword searches in parallel

3. **Frontend Integration**
   - SearchConfig component with multi-source options
   - Real-time search execution and progress tracking
   - Result display with source attribution

### API Usage

**Multi-Source Keyword Search:**
```bash
POST /api/searches/:id/execute-multi-source
{
  "strategy": "any",
  "limit": 50,
  "total_limit": 100,
  "sort_by": "relevance"
}
```

## 📈 Performance & Monitoring

### Extraction Performance
- **Concurrency**: 2 concurrent OpenAI API calls (configurable)
- **Rate Limiting**: Exponential backoff for API limits
- **Progress Tracking**: Real-time progress bars and statistics
- **Error Handling**: Automatic retry with detailed logging

### Search Performance
- **Database Indexes**: Optimized for keyword and relevance queries
- **Connection Pooling**: Efficient database connection management
- **Result Caching**: Future enhancement for frequently searched terms
- **Parallel Execution**: Multi-source searches run in parallel

### Monitoring

**Real-time Statistics:**
```
📊 KEYWORD EXTRACTION SUMMARY
============================================================
⏱️  Duration: 45.2s (2.21 opinions/sec)
📈 Processed: 100 opinions
✅ Successful: 95 opinions
❌ Failed: 3 opinions
⏭️  Skipped: 2 opinions (too short)
🔑 Total Keywords: 1,847
📊 Avg Keywords/Opinion: 19.4
============================================================
```

**Database Statistics:**
- Opinions with keywords: 1,250
- Total keyword assignments: 24,350
- Average relevance score: 0.742
- Unique keywords in database: 8,924

## 🔧 Configuration

### Environment Variables
```bash
# Required
OPENAI_API_KEY=your_openai_api_key_here

# Database (configured in Phoenix app)
NY_APPEALS_DB_URL=postgresql://localhost/ny_court_of_appeals
```

### Extraction Configuration
```javascript
const CONFIG = {
  batchSize: 50,           // Opinions per batch
  concurrency: 2,          // Concurrent API calls
  minTextLength: 500,      // Skip short opinions
  maxRetries: 3,           // API retry attempts
  baseDelayMs: 1000       // Retry delay
};
```

### Search Configuration
```javascript
const searchOptions = {
  limit: 50,               // Results per source
  minRelevance: 0.5,       // Minimum keyword relevance
  sortBy: 'relevance',     // Sort strategy
  categories: null         // Category filter
};
```

## 🚨 Error Handling

### Common Issues

**OpenAI API Errors:**
- Rate limiting (429): Automatic exponential backoff
- Invalid JSON: Retry with error logging
- API key issues: Clear error message and exit

**Database Errors:**
- Connection failures: Automatic reconnection attempts
- Query timeouts: Configurable timeout settings
- Schema issues: Detailed error logging

**Data Quality Issues:**
- Short opinions: Automatically skipped
- Invalid keywords: Filtered by relevance threshold
- Duplicate keywords: Handled by UNIQUE constraints

### Debugging

**Enable Debug Logging:**
```bash
DEBUG=1 node src/batchKeywordExtraction.js 5
```

**Test Individual Components:**
```bash
# Test database schema
node src/testKeywordSystem.js

# Test search functionality
node src/keywordSearchService.js

# Test text extraction
node src/collectTextForOpinion.js 128922
```

## 📋 Next Steps

### Immediate Enhancements
1. **Batch Processing**: Process larger opinion batches efficiently
2. **Quality Metrics**: Track keyword extraction quality over time
3. **User Feedback**: Allow users to rate keyword relevance
4. **Category Expansion**: Add more specialized legal categories

### Future Features
1. **Semantic Search**: Combine keyword and vector similarity search
2. **Auto-Categorization**: Automatically categorize legal opinions
3. **Citation Analysis**: Extract and link legal citations
4. **Trend Analysis**: Track legal concept evolution over time

### Scaling Considerations
1. **Distributed Processing**: Scale keyword extraction across multiple workers
2. **Caching Layer**: Add Redis for frequently accessed keywords
3. **Search Optimization**: Implement full-text search with PostgreSQL
4. **API Rate Management**: Implement intelligent API usage optimization

## 🤝 Contributing

### Adding New Data Sources
1. Create database schema with keyword tables
2. Implement search functions for the new source
3. Add data source configuration to ConnectionManager
4. Update multi-source search service

### Improving Keyword Quality
1. Refine the extraction prompt (`keyword_extraction.md`)
2. Adjust relevance thresholds and filtering
3. Add domain-specific keyword categories
4. Implement feedback loops for continuous improvement

---

**System Status**: ✅ Production Ready
**Last Updated**: 2025-01-25
**Maintainer**: Lexica Legal Research Platform
