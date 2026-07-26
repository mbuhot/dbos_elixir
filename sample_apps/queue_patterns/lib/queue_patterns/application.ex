defmodule QueuePatterns.Application do
  @moduledoc "Boots this app's Ecto repo and the Dbos engine backing every pattern module."

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      QueuePatterns.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, QueuePatterns.Repo},
       otp_app: :queue_patterns,
       queues: queues(),
       migrations: :verify}
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
end
