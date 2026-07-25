defmodule S3Mirror.Application do
  @moduledoc "Starts a Postgrex pool and the `Dbos` engine that runs `S3Mirror.Workflows`."

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one, name: S3Mirror.Supervisor)
  end

  @doc false
  def children do
    if Mix.env() == :test do
      []
    else
      [
        {Postgrex, name: S3Mirror.Repo, database: database_name(), hostname: pg_hostname()},
        {Dbos.Supervisor,
         name: Dbos,
         db: {Dbos.DB.Postgrex, S3Mirror.Repo},
         workflows: [S3Mirror.Workflows],
         queues: [
           Dbos.Queue.new(S3Mirror.Workflows.queue_name(),
             worker_concurrency: 5,
             rate_limit: %{limit: 20, period_ms: 1_000}
           )
         ],
         migrations: :create_if_absent}
      ]
    end
  end

  defp database_name, do: System.get_env("S3_MIRROR_DATABASE") || "s3_mirror_dev"
  defp pg_hostname, do: System.get_env("PGHOST") || "localhost"
end
