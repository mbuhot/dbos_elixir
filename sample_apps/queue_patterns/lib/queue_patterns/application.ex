defmodule QueuePatterns.Application do
  @moduledoc "Boots the Postgres pool and the Dbos engine backing every pattern module."

  use Application

  alias QueuePatterns.Debouncing
  alias QueuePatterns.Deduplication
  alias QueuePatterns.FanOut
  alias QueuePatterns.Priority
  alias QueuePatterns.RateLimitedApi
  alias QueuePatterns.SerializedPerKey
  alias QueuePatterns.TenantFairness

  @impl true
  def start(_type, _args) do
    children = [
      {Postgrex, postgrex_opts()},
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Postgrex, QueuePatterns.Repo},
       workflows: [
         FanOut,
         RateLimitedApi,
         TenantFairness,
         Priority,
         Deduplication,
         Debouncing,
         SerializedPerKey
       ],
       queues: queues(),
       migrations: :create_if_absent}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: QueuePatterns.Supervisor)
  end

  @doc "Every queue every pattern in this sample declares, for reuse from `mix` tasks and tests."
  def queues do
    [
      Dbos.Queue.new("fan_out", worker_concurrency: 5, base_polling_interval_ms: 200),
      Dbos.Queue.new("rate_limited_api",
        rate_limit: %{limit: 2, period_ms: 1_000},
        base_polling_interval_ms: 200
      ),
      Dbos.Queue.new("tenant_jobs",
        partition_queue: true,
        worker_concurrency: 1,
        base_polling_interval_ms: 200
      ),
      Dbos.Queue.new("global_jobs", global_concurrency: 3, base_polling_interval_ms: 200),
      Dbos.Queue.new("priority_queue",
        priority_enabled: true,
        worker_concurrency: 1,
        base_polling_interval_ms: 200
      ),
      Dbos.Queue.new("dedup_queue", base_polling_interval_ms: 200),
      Dbos.Queue.new("debounce_queue", base_polling_interval_ms: 200),
      Dbos.Queue.new("serial_per_key",
        partition_queue: true,
        worker_concurrency: 1,
        base_polling_interval_ms: 200
      )
    ]
  end

  defp postgrex_opts do
    [
      name: QueuePatterns.Repo,
      hostname: System.get_env("PGHOST", "localhost"),
      port: System.get_env("PGPORT", "5432") |> String.to_integer(),
      username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
      password: System.get_env("PGPASSWORD"),
      database: System.get_env("PGDATABASE", "queue_patterns_dev"),
      pool_size: 10
    ]
  end
end
