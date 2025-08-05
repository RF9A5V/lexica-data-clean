# Backend Integration Guide for Keyword Search

## Overview
This guide shows how to integrate the keyword search system with your existing Lexica backend Phoenix application.

## 1. Database Migration

First, run the schema additions in your Phoenix app:

```bash
# In your lexica_backend directory
mix ecto.create_migration add_keyword_search_tables
```

Copy the contents of `keyword_schema_additions.sql` into the migration file:

```elixir
defmodule LexicaBackend.Repo.Migrations.AddKeywordSearchTables do
  use Ecto.Migration

  def up do
    # Keywords table (normalized keyword storage)
    create table(:keywords) do
      add :keyword_text, :text, null: false
      add :frequency, :integer, default: 1
      timestamps()
    end
    
    create unique_index(:keywords, [:keyword_text])
    create index(:keywords, [:frequency])

    # Opinion-level keywords
    create table(:opinion_keywords) do
      add :opinion_id, references(:opinions, on_delete: :delete_all), null: false
      add :keyword_id, references(:keywords, on_delete: :delete_all), null: false
      add :relevance_score, :float, null: false
      add :extraction_method, :string, default: "llm_generated"
      timestamps()
    end
    
    create unique_index(:opinion_keywords, [:opinion_id, :keyword_id])
    create index(:opinion_keywords, [:opinion_id])
    create index(:opinion_keywords, [:keyword_id])
    create index(:opinion_keywords, [:relevance_score])

    # Sentence-level keywords (optional for future use)
    create table(:sentence_keywords) do
      add :sentence_id, references(:opinion_sentences, on_delete: :delete_all), null: false
      add :keyword_id, references(:keywords, on_delete: :delete_all), null: false
      add :relevance_score, :float, null: false
      add :extraction_method, :string, default: "llm_generated"
      timestamps()
    end
    
    create unique_index(:sentence_keywords, [:sentence_id, :keyword_id])
    create index(:sentence_keywords, [:sentence_id])
    create index(:sentence_keywords, [:keyword_id])

    # Function to get or create keyword
    execute """
    CREATE OR REPLACE FUNCTION get_or_create_keyword(keyword_text TEXT)
    RETURNS INTEGER AS $$
    DECLARE
        keyword_id INTEGER;
    BEGIN
        SELECT id INTO keyword_id FROM keywords WHERE keywords.keyword_text = get_or_create_keyword.keyword_text;
        
        IF keyword_id IS NULL THEN
            INSERT INTO keywords (keyword_text, inserted_at, updated_at) 
            VALUES (keyword_text, NOW(), NOW()) 
            RETURNING id INTO keyword_id;
        ELSE
            UPDATE keywords SET frequency = frequency + 1, updated_at = NOW() 
            WHERE id = keyword_id;
        END IF;
        
        RETURN keyword_id;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  def down do
    execute "DROP FUNCTION IF EXISTS get_or_create_keyword(TEXT);"
    drop table(:sentence_keywords)
    drop table(:opinion_keywords)
    drop table(:keywords)
  end
end
```

## 2. Elixir Schemas

Create the schemas in your Phoenix app:

```elixir
# lib/lexica_backend/search/keyword.ex
defmodule LexicaBackend.Search.Keyword do
  use Ecto.Schema
  import Ecto.Changeset

  schema "keywords" do
    field :keyword_text, :string
    field :frequency, :integer, default: 1
    
    many_to_many :opinions, LexicaBackend.Cases.Opinion, 
      join_through: LexicaBackend.Search.OpinionKeyword
    
    timestamps()
  end

  def changeset(keyword, attrs) do
    keyword
    |> cast(attrs, [:keyword_text, :frequency])
    |> validate_required([:keyword_text])
    |> validate_length(:keyword_text, min: 1, max: 255)
    |> unique_constraint(:keyword_text)
  end
end

# lib/lexica_backend/search/opinion_keyword.ex
defmodule LexicaBackend.Search.OpinionKeyword do
  use Ecto.Schema
  import Ecto.Changeset

  schema "opinion_keywords" do
    belongs_to :opinion, LexicaBackend.Cases.Opinion
    belongs_to :keyword, LexicaBackend.Search.Keyword
    field :relevance_score, :float
    field :extraction_method, :string, default: "llm_generated"
    
    timestamps()
  end

  def changeset(opinion_keyword, attrs) do
    opinion_keyword
    |> cast(attrs, [:opinion_id, :keyword_id, :relevance_score, :extraction_method])
    |> validate_required([:opinion_id, :keyword_id, :relevance_score])
    |> validate_number(:relevance_score, greater_than_or_equal_to: 0.0, less_than_or_equal_to: 1.0)
    |> validate_inclusion(:extraction_method, ["llm_generated", "tf_idf", "manual"])
    |> unique_constraint([:opinion_id, :keyword_id])
  end
end
```

## 3. Keyword Search Service

Create a search service in your Phoenix app:

```elixir
# lib/lexica_backend/search/services/keyword_search_service.ex
defmodule LexicaBackend.Search.Services.KeywordSearchService do
  import Ecto.Query
  alias LexicaBackend.Repo
  alias LexicaBackend.Cases.{Case, Opinion}
  alias LexicaBackend.Search.{Keyword, OpinionKeyword}

  @doc """
  Search for cases using keywords with various matching strategies
  """
  def search_by_keywords(keywords, opts \\ []) do
    strategy = Keyword.get(opts, :match_strategy, :any)
    min_relevance = Keyword.get(opts, :min_relevance, 0.5)
    max_results = Keyword.get(opts, :max_results, 50)
    
    normalized_keywords = 
      keywords
      |> Enum.map(&String.downcase/1)
      |> Enum.map(&String.trim/1)
      |> Enum.filter(&(String.length(&1) > 0))
    
    case strategy do
      :any -> search_any_keywords(normalized_keywords, min_relevance, max_results)
      :all -> search_all_keywords(normalized_keywords, min_relevance, max_results)
      :phrase -> search_phrase(Enum.join(normalized_keywords, " "), min_relevance, max_results)
    end
  end

  defp search_any_keywords(keywords, min_relevance, max_results) do
    query = """
    WITH keyword_matches AS (
      SELECT 
        o.id as opinion_id,
        c.id as case_id,
        c.case_name,
        c.citation_count,
        c.date_filed,
        k.keyword_text,
        ok.relevance_score,
        (ok.relevance_score * (1.0 / GREATEST(k.frequency, 1))) as match_score
      FROM opinions o
      JOIN cases c ON o.case_id = c.id
      JOIN opinion_keywords ok ON o.id = ok.opinion_id
      JOIN keywords k ON ok.keyword_id = k.id
      WHERE k.keyword_text = ANY($1)
        AND ok.relevance_score >= $2
    ),
    aggregated_results AS (
      SELECT 
        case_id,
        opinion_id,
        case_name,
        citation_count,
        date_filed,
        COUNT(DISTINCT keyword_text) as matched_keywords,
        AVG(match_score) as avg_match_score,
        SUM(match_score) as total_match_score,
        ARRAY_AGG(DISTINCT keyword_text ORDER BY match_score DESC) as matching_keywords
      FROM keyword_matches
      GROUP BY case_id, opinion_id, case_name, citation_count, date_filed
    )
    SELECT *,
      (total_match_score * LOG(matched_keywords + 1)) as final_relevance
    FROM aggregated_results
    ORDER BY final_relevance DESC, citation_count DESC
    LIMIT $3
    """
    
    result = Repo.query!(query, [keywords, min_relevance, max_results])
    process_search_results(result.rows)
  end

  defp search_all_keywords(keywords, min_relevance, max_results) do
    query = """
    WITH keyword_matches AS (
      SELECT 
        o.id as opinion_id,
        c.id as case_id,
        c.case_name,
        c.citation_count,
        c.date_filed,
        k.keyword_text,
        ok.relevance_score
      FROM opinions o
      JOIN cases c ON o.case_id = c.id
      JOIN opinion_keywords ok ON o.id = ok.opinion_id
      JOIN keywords k ON ok.keyword_id = k.id
      WHERE k.keyword_text = ANY($1)
        AND ok.relevance_score >= $2
    ),
    opinion_keyword_counts AS (
      SELECT 
        opinion_id,
        case_id,
        case_name,
        citation_count,
        date_filed,
        COUNT(DISTINCT keyword_text) as matched_keywords,
        AVG(relevance_score) as avg_relevance,
        ARRAY_AGG(DISTINCT keyword_text ORDER BY relevance_score DESC) as matching_keywords
      FROM keyword_matches
      GROUP BY opinion_id, case_id, case_name, citation_count, date_filed
      HAVING COUNT(DISTINCT keyword_text) = $3
    )
    SELECT *,
      avg_relevance as final_relevance
    FROM opinion_keyword_counts
    ORDER BY avg_relevance DESC, citation_count DESC
    LIMIT $4
    """
    
    result = Repo.query!(query, [keywords, min_relevance, length(keywords), max_results])
    process_search_results(result.rows)
  end

  defp process_search_results(rows) do
    Enum.map(rows, fn row ->
      %{
        case_id: Enum.at(row, 0),
        opinion_id: Enum.at(row, 1),
        case_name: Enum.at(row, 2),
        citation_count: Enum.at(row, 3),
        date_filed: Enum.at(row, 4),
        matched_keywords: Enum.at(row, 5),
        relevance_score: Enum.at(row, 8) |> Decimal.to_float(),
        matching_keywords: Enum.at(row, 7)
      }
    end)
  end

  @doc """
  Get related keywords for query expansion
  """
  def get_related_keywords(keywords, limit \\ 10) do
    normalized_keywords = Enum.map(keywords, &String.downcase/1)
    
    query = """
    WITH base_opinions AS (
      SELECT DISTINCT ok.opinion_id
      FROM opinion_keywords ok
      JOIN keywords k ON ok.keyword_id = k.id
      WHERE k.keyword_text = ANY($1)
    ),
    related_keywords AS (
      SELECT 
        k.keyword_text,
        COUNT(*) as co_occurrence_count,
        AVG(ok.relevance_score) as avg_relevance
      FROM base_opinions bo
      JOIN opinion_keywords ok ON bo.opinion_id = ok.opinion_id
      JOIN keywords k ON ok.keyword_id = k.id
      WHERE k.keyword_text != ALL($1)
      GROUP BY k.keyword_text
      HAVING COUNT(*) >= 2
    )
    SELECT 
      keyword_text,
      co_occurrence_count,
      avg_relevance,
      (co_occurrence_count * avg_relevance) as relevance_score
    FROM related_keywords
    ORDER BY relevance_score DESC
    LIMIT $2
    """
    
    result = Repo.query!(query, [normalized_keywords, limit])
    
    Enum.map(result.rows, fn [keyword_text, co_occurrence, avg_relevance, relevance_score] ->
      %{
        keyword_text: keyword_text,
        co_occurrence_count: co_occurrence,
        avg_relevance: Decimal.to_float(avg_relevance),
        relevance_score: Decimal.to_float(relevance_score)
      }
    end)
  end
end
```

## 4. Controller Integration

Update your search controller to support keyword search:

```elixir
# In your existing SearchController
alias LexicaBackend.Search.Services.KeywordSearchService

def execute_keyword_search(conn, %{"matter_id" => matter_id, "search_id" => search_id}) do
  with {:ok, search} <- get_search_for_matter(matter_id, search_id),
       {:ok, keywords} <- extract_keywords_from_search(search),
       results <- KeywordSearchService.search_by_keywords(keywords, match_strategy: :any) do
    
    # Store results in your existing format
    search_results = format_keyword_results(results)
    
    # Update search state and store results
    {:ok, updated_search} = update_search_results(search, search_results)
    
    json(conn, %{
      search: updated_search,
      results: search_results
    })
  else
    {:error, reason} ->
      conn
      |> put_status(:unprocessable_entity)
      |> json(%{error: reason})
  end
end

defp extract_keywords_from_search(search) do
  # Extract keywords from your search statements
  keywords = 
    search.search_statements
    |> Enum.map(& &1.text)
    |> Enum.flat_map(&String.split(&1, ~r/\s+/))
    |> Enum.map(&String.downcase/1)
    |> Enum.uniq()
  
  {:ok, keywords}
end

defp format_keyword_results(keyword_results) do
  # Convert keyword search results to your existing format
  Enum.map(keyword_results, fn result ->
    %{
      case_id: result.case_id,
      opinion_id: result.opinion_id,
      case_name: result.case_name,
      relevance_score: result.relevance_score,
      matched_keywords: result.matching_keywords,
      # Add other fields as needed for compatibility
    }
  end)
end
```

## 5. Deployment Steps

1. **Run keyword extraction**: Use `processKeywords.js` to populate your database
2. **Test the search**: Use `keywordSearch.js` to verify functionality
3. **Update your frontend**: Modify search execution to use keyword search endpoint
4. **Monitor performance**: Add logging and metrics for keyword search performance

## 6. Performance Considerations

- **Index optimization**: Ensure proper indexes on keyword tables
- **Caching**: Consider caching frequent keyword searches
- **Batch processing**: Process keyword extraction in batches
- **Query optimization**: Monitor slow queries and optimize as needed

## 7. Future Enhancements

- **Hybrid search**: Combine keyword and embedding search
- **Query expansion**: Use related keywords to improve results
- **User feedback**: Allow users to rate search result relevance
- **Analytics**: Track keyword search patterns and performance