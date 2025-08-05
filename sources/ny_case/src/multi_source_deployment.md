# Multi-Source Deployment Guide

## 1. Keyword Extraction Per Data Source

Run keyword extraction on each data source independently:

```bash
# For NY Appeals (current)
cd lexica-data/sources/ny_case/src
export NY_STATE_APPEALS_DB="postgresql://user:pass@host:port/ny_court_of_appeals"
node processKeywords.js

# For future Federal source
cd lexica-data/sources/federal_district/src
export FEDERAL_DISTRICT_DB="postgresql://user:pass@host:port/federal_district"
node processKeywords.js

# For future California source
cd lexica-data/sources/ca_supreme/src
export CA_SUPREME_DB="postgresql://user:pass@host:port/ca_supreme"
node processKeywords.js
```

## 2. Data Source Registration

Register each data source in your application database:

```sql
-- Register NY Appeals (already done in schema)
INSERT INTO data_sources (name, display_name, database_url, source_type, jurisdiction) 
VALUES (
    'ny_appeals',
    'NY State Court of Appeals', 
    'postgresql://user:pass@host:port/ny_court_of_appeals',
    'state_appeals',
    'New York'
);

-- Future sources
INSERT INTO data_sources (name, display_name, database_url, source_type, jurisdiction) 
VALUES (
    'federal_district',
    'Federal District Courts', 
    'postgresql://user:pass@host:port/federal_district',
    'federal_district',
    'Federal'
);

INSERT INTO data_sources (name, display_name, database_url, source_type, jurisdiction) 
VALUES (
    'ca_supreme',
    'California Supreme Court', 
    'postgresql://user:pass@host:port/ca_supreme',
    'state_supreme',
    'California'
);
```

## 3. User Permission Setup

Grant users access to data sources:

```sql
-- Grant user access to NY Appeals
INSERT INTO user_data_source_permissions (user_id, data_source_id, permission_level)
SELECT 
    u.id,
    ds.id,
    'read'
FROM users u
CROSS JOIN data_sources ds
WHERE ds.name = 'ny_appeals'
  AND u.email = 'user@example.com'; -- Replace with actual user

-- For admin users, grant access to all sources
INSERT INTO user_data_source_permissions (user_id, data_source_id, permission_level)
SELECT 
    u.id,
    ds.id,
    'read'
FROM users u
CROSS JOIN data_sources ds
WHERE u.role = 'admin'; -- Assuming you have user roles
```

## 4. Environment Configuration

Update your Phoenix environment:

```bash
# .env or environment variables
DATABASE_URL="postgresql://user:pass@host:port/lexica_app"
NY_APPEALS_DB_URL="postgresql://user:pass@host:port/ny_court_of_appeals"
FEDERAL_DISTRICT_DB_URL="postgresql://user:pass@host:port/federal_district"
CA_SUPREME_DB_URL="postgresql://user:pass@host:port/ca_supreme"
```

## 5. Search Controller Updates

Update your search controller to use multi-source search:

```elixir
# In your SearchController
def execute_search(conn, %{"matter_id" => matter_id, "search_id" => search_id}) do
  current_user = conn.assigns.current_user
  
  with {:ok, search} <- get_search_for_matter(matter_id, search_id, current_user),
       {:ok, results} <- MultiSourceKeywordSearch.execute_search(search, current_user) do
    
    # Update search with aggregated results
    {:ok, updated_search} = update_search_with_results(search, results)
    
    json(conn, %{
      search: updated_search,
      results: results.results,
      summary: results.summary
    })
  else
    {:error, reason} ->
      conn
      |> put_status(:unprocessable_entity)
      |> json(%{error: reason})
  end
end
```

## 6. Frontend Updates

Your frontend can now show which sources were searched:

```javascript
// In your search results display
const searchSummary = {
  totalResults: response.summary.total_results,
  sourcesSearched: response.summary.sources_searched,
  executionTime: response.summary.total_execution_time
};

// Show source information in results
results.forEach(result => {
  // Each result now includes data_source information
  console.log(`Found in: ${result.data_source.display_name}`);
});
```

## 7. Monitoring and Analytics

Track search performance across sources:

```sql
-- Query to see search performance by source
SELECT 
    ds.display_name,
    COUNT(*) as total_searches,
    AVG(se.execution_time_ms) as avg_execution_time,
    AVG(se.results_count) as avg_results_count,
    COUNT(CASE WHEN se.status = 'failed' THEN 1 END) as failed_searches
FROM search_executions se
JOIN data_sources ds ON se.data_source_id = ds.id
WHERE se.created_at >= NOW() - INTERVAL '30 days'
GROUP BY ds.id, ds.display_name
ORDER BY total_searches DESC;
```

## 8. Scaling Considerations

### Database Connection Pooling
- Each data source gets its own connection pool
- Configure pool sizes based on expected load
- Monitor connection usage and adjust as needed

### Parallel Search Optimization
- Searches run in parallel across all accessible sources
- Timeout handling prevents slow sources from blocking results
- Failed sources don't prevent other sources from returning results

### Caching Strategy
- Cache frequent keyword searches per data source
- Use Redis with source-specific cache keys
- Implement cache invalidation when new data is added

### Load Balancing
- Can run multiple Phoenix instances
- Each instance maintains connections to all data sources
- Database load is distributed across source-specific servers

## 9. Future Source Addition Workflow

Adding a new legal data source:

1. **Create new database** with standard schema
2. **Run data ingestion** scripts for the new source
3. **Execute keyword extraction** using processKeywords.js
4. **Register data source** in application database
5. **Grant user permissions** as needed
6. **Test search functionality** on new source
7. **Deploy to production** - no application code changes needed!

## 10. Benefits of This Architecture

### Scalability
- ✅ **Independent scaling** - each data source can scale independently
- ✅ **Parallel processing** - searches run concurrently across sources
- ✅ **Fault isolation** - one source failure doesn't affect others

### Security & Access Control
- ✅ **Granular permissions** - control access per data source
- ✅ **Data isolation** - sources are completely separate
- ✅ **Audit trail** - track which sources each user searches

### Operational Benefits
- ✅ **Easy source addition** - no application changes needed
- ✅ **Independent maintenance** - update sources without affecting app
- ✅ **Cost optimization** - pay only for sources you use
- ✅ **Geographic distribution** - sources can be in different regions

This architecture positions you perfectly for scaling to multiple legal data sources while maintaining performance and security.