# Multi-Database Architecture Implementation

## 1. Application Database Schema Updates

Add these tables to your main Phoenix application database:

```sql
-- Data source configuration
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL UNIQUE, -- e.g., 'ny_appeals', 'federal_district'
    display_name VARCHAR(200) NOT NULL, -- e.g., 'NY State Court of Appeals'
    database_url TEXT NOT NULL, -- Connection string to source database
    source_type VARCHAR(50) NOT NULL, -- e.g., 'state_appeals', 'federal_district'
    jurisdiction VARCHAR(100), -- e.g., 'New York', 'Federal'
    is_active BOOLEAN DEFAULT true,
    search_enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User access permissions per data source
CREATE TABLE user_data_source_permissions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    data_source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    permission_level VARCHAR(20) NOT NULL DEFAULT 'read', -- 'read', 'admin'
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    granted_by UUID REFERENCES users(id),
    
    UNIQUE(user_id, data_source_id)
);

-- Search execution tracking across sources
CREATE TABLE search_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    search_id UUID NOT NULL REFERENCES searches(id) ON DELETE CASCADE,
    data_source_id UUID NOT NULL REFERENCES data_sources(id),
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'running', 'completed', 'failed'
    results_count INTEGER DEFAULT 0,
    execution_time_ms INTEGER,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_user_permissions_user_id ON user_data_source_permissions(user_id);
CREATE INDEX idx_user_permissions_source_id ON user_data_source_permissions(data_source_id);
CREATE INDEX idx_search_executions_search_id ON search_executions(search_id);
CREATE INDEX idx_search_executions_source_id ON search_executions(data_source_id);
CREATE INDEX idx_search_executions_status ON search_executions(status);

-- Insert NY Appeals data source
INSERT INTO data_sources (name, display_name, database_url, source_type, jurisdiction) 
VALUES (
    'ny_appeals',
    'NY State Court of Appeals', 
    'postgresql://user:pass@host:port/ny_court_of_appeals', -- Replace with actual connection
    'state_appeals',
    'New York'
);
```

## 2. Phoenix Configuration for Multiple Databases

Update your Phoenix config to support multiple database connections:

```elixir
# config/config.exs
config :lexica_backend, LexicaBackend.Repo,
  # Your main application database
  url: database_url,
  pool_size: String.to_integer(System.get_env("POOL_SIZE") || "10")

# Dynamic data source connections will be managed by DataSourceManager
config :lexica_backend, :data_sources, [
  ny_appeals: [
    url: System.get_env("NY_APPEALS_DB_URL"),
    pool_size: 5,
    pool_timeout: 15_000,
    timeout: 30_000
  ]
  # Future sources will be added here or loaded dynamically
]
```

## 3. Elixir Schemas for Multi-DB Support

```elixir
# lib/lexica_backend/data_sources/data_source.ex
defmodule LexicaBackend.DataSources.DataSource do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "data_sources" do
    field :name, :string
    field :display_name, :string
    field :database_url, :string
    field :source_type, :string
    field :jurisdiction, :string
    field :is_active, :boolean, default: true
    field :search_enabled, :boolean, default: true
    
    has_many :user_permissions, LexicaBackend.DataSources.UserDataSourcePermission
    has_many :search_executions, LexicaBackend.Search.SearchExecution
    
    timestamps()
  end

  def changeset(data_source, attrs) do
    data_source
    |> cast(attrs, [:name, :display_name, :database_url, :source_type, :jurisdiction, :is_active, :search_enabled])
    |> validate_required([:name, :display_name, :database_url, :source_type])
    |> unique_constraint(:name)
    |> validate_format(:database_url, ~r/^postgresql:\/\//)
  end
end

# lib/lexica_backend/data_sources/user_data_source_permission.ex
defmodule LexicaBackend.DataSources.UserDataSourcePermission do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "user_data_source_permissions" do
    belongs_to :user, LexicaBackend.Accounts.User, type: :binary_id
    belongs_to :data_source, LexicaBackend.DataSources.DataSource, type: :binary_id
    field :permission_level, :string, default: "read"
    field :granted_at, :utc_datetime
    belongs_to :granted_by, LexicaBackend.Accounts.User, type: :binary_id
  end

  def changeset(permission, attrs) do
    permission
    |> cast(attrs, [:user_id, :data_source_id, :permission_level, :granted_by])
    |> validate_required([:user_id, :data_source_id])
    |> validate_inclusion(:permission_level, ["read", "admin"])
    |> unique_constraint([:user_id, :data_source_id])
  end
end

# lib/lexica_backend/search/search_execution.ex
defmodule LexicaBackend.Search.SearchExecution do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  schema "search_executions" do
    belongs_to :search, LexicaBackend.Search.Search, type: :binary_id
    belongs_to :data_source, LexicaBackend.DataSources.DataSource, type: :binary_id
    field :status, :string, default: "pending"
    field :results_count, :integer, default: 0
    field :execution_time_ms, :integer
    field :error_message, :string
    field :started_at, :utc_datetime
    field :completed_at, :utc_datetime
    
    timestamps()
  end

  def changeset(execution, attrs) do
    execution
    |> cast(attrs, [:search_id, :data_source_id, :status, :results_count, :execution_time_ms, :error_message, :started_at, :completed_at])
    |> validate_required([:search_id, :data_source_id])
    |> validate_inclusion(:status, ["pending", "running", "completed", "failed"])
  end
end
```

## 4. Data Source Connection Manager

```elixir
# lib/lexica_backend/data_sources/connection_manager.ex
defmodule LexicaBackend.DataSources.ConnectionManager do
  @moduledoc """
  Manages dynamic database connections to legal data sources
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    # Load data source configurations and establish connections
    data_sources = load_data_sources()
    connections = establish_connections(data_sources)
    
    {:ok, %{connections: connections, data_sources: data_sources}}
  end

  @doc """
  Get a database connection for a specific data source
  """
  def get_connection(data_source_name) do
    GenServer.call(__MODULE__, {:get_connection, data_source_name})
  end

  @doc """
  Execute a query on a specific data source
  """
  def query(data_source_name, sql, params \\ []) do
    case get_connection(data_source_name) do
      {:ok, conn} ->
        Postgrex.query(conn, sql, params)
      {:error, reason} ->
        {:error, reason}
    end
  end

  def handle_call({:get_connection, data_source_name}, _from, state) do
    connection = Map.get(state.connections, data_source_name)
    reply = if connection, do: {:ok, connection}, else: {:error, :not_found}
    {:reply, reply, state}
  end

  defp load_data_sources do
    # Load from database or config
    LexicaBackend.Repo.all(LexicaBackend.DataSources.DataSource)
    |> Enum.filter(& &1.is_active)
  end

  defp establish_connections(data_sources) do
    Enum.reduce(data_sources, %{}, fn source, acc ->
      case establish_connection(source) do
        {:ok, conn} ->
          Map.put(acc, source.name, conn)
        {:error, reason} ->
          Logger.error("Failed to connect to #{source.name}: #{inspect(reason)}")
          acc
      end
    end)
  end

  defp establish_connection(data_source) do
    opts = [
      hostname: extract_hostname(data_source.database_url),
      database: extract_database(data_source.database_url),
      username: extract_username(data_source.database_url),
      password: extract_password(data_source.database_url),
      pool_size: 5
    ]
    
    Postgrex.start_link(opts)
  end

  # Helper functions to parse database URL
  defp extract_hostname(url), do: # Parse hostname from URL
  defp extract_database(url), do: # Parse database name from URL
  defp extract_username(url), do: # Parse username from URL
  defp extract_password(url), do: # Parse password from URL
end
```

## 5. Multi-Source Keyword Search Service

```elixir
# lib/lexica_backend/search/services/multi_source_keyword_search.ex
defmodule LexicaBackend.Search.Services.MultiSourceKeywordSearch do
  alias LexicaBackend.DataSources.ConnectionManager
  alias LexicaBackend.Search.SearchExecution
  alias LexicaBackend.Repo
  require Logger

  @doc """
  Execute keyword search across multiple data sources in parallel
  """
  def execute_search(search, user) do
    # Get accessible data sources for user
    accessible_sources = get_accessible_sources(user)
    
    # Create search execution records
    executions = create_search_executions(search, accessible_sources)
    
    # Execute searches in parallel
    tasks = Enum.map(accessible_sources, fn source ->
      Task.async(fn ->
        execute_source_search(search, source, executions[source.id])
      end)
    end)
    
    # Collect results with timeout
    results = Task.yield_many(tasks, 30_000)
    
    # Process and aggregate results
    aggregate_results(results, search)
  end

  defp get_accessible_sources(user) do
    query = """
    SELECT ds.* 
    FROM data_sources ds
    JOIN user_data_source_permissions udsp ON ds.id = udsp.data_source_id
    WHERE udsp.user_id = $1 
      AND ds.is_active = true 
      AND ds.search_enabled = true
    """
    
    result = Repo.query!(query, [user.id])
    # Convert to DataSource structs
  end

  defp create_search_executions(search, data_sources) do
    Enum.reduce(data_sources, %{}, fn source, acc ->
      {:ok, execution} = 
        %SearchExecution{}
        |> SearchExecution.changeset(%{
          search_id: search.id,
          data_source_id: source.id,
          status: "pending"
        })
        |> Repo.insert()
      
      Map.put(acc, source.id, execution)
    end)
  end

  defp execute_source_search(search, data_source, execution) do
    start_time = System.monotonic_time(:millisecond)
    
    # Update execution status
    update_execution_status(execution, "running", %{started_at: DateTime.utc_now()})
    
    try do
      # Extract keywords from search statements
      keywords = extract_keywords_from_search(search)
      
      # Execute keyword search on this data source
      results = execute_keyword_search_on_source(data_source, keywords)
      
      execution_time = System.monotonic_time(:millisecond) - start_time
      
      # Update execution as completed
      update_execution_status(execution, "completed", %{
        completed_at: DateTime.utc_now(),
        results_count: length(results),
        execution_time_ms: execution_time
      })
      
      {:ok, %{data_source: data_source, results: results, execution_time: execution_time}}
      
    rescue
      error ->
        execution_time = System.monotonic_time(:millisecond) - start_time
        
        update_execution_status(execution, "failed", %{
          completed_at: DateTime.utc_now(),
          error_message: Exception.message(error),
          execution_time_ms: execution_time
        })
        
        Logger.error("Search failed for #{data_source.name}: #{Exception.message(error)}")
        {:error, %{data_source: data_source, error: error}}
    end
  end

  defp execute_keyword_search_on_source(data_source, keywords) do
    # Use the keyword search query we built earlier, but execute on specific data source
    query = build_keyword_search_query(keywords)
    
    case ConnectionManager.query(data_source.name, query.sql, query.params) do
      {:ok, result} ->
        process_source_results(result.rows, data_source)
      {:error, reason} ->
        raise "Database query failed: #{inspect(reason)}"
    end
  end

  defp aggregate_results(task_results, search) do
    # Collect successful results
    successful_results = 
      task_results
      |> Enum.map(fn {task, result} ->
        case result do
          {:ok, {:ok, data}} -> data
          _ -> nil
        end
      end)
      |> Enum.filter(& &1)
    
    # Combine and sort results by relevance
    all_results = 
      successful_results
      |> Enum.flat_map(& &1.results)
      |> Enum.sort_by(& &1.relevance_score, :desc)
      |> Enum.take(100) # Limit total results
    
    # Calculate summary statistics
    summary = %{
      total_results: length(all_results),
      sources_searched: length(successful_results),
      total_execution_time: Enum.sum(Enum.map(successful_results, & &1.execution_time))
    }
    
    {:ok, %{results: all_results, summary: summary}}
  end

  defp update_execution_status(execution, status, attrs) do
    execution
    |> SearchExecution.changeset(Map.put(attrs, :status, status))
    |> Repo.update!()
  end
end
```