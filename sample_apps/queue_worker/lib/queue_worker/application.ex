defmodule QueueWorker.Application do
  @moduledoc "Boots this app's Ecto repo and the Dbos engine that runs `QueueWorker.Tasks`."

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      QueueWorker.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, QueueWorker.Repo},
       otp_app: :queue_worker,
       queues: [
         Dbos.Queue.new("tasks",
           worker_concurrency: worker_concurrency(),
           base_polling_interval_ms: 200
         )
       ],
       migrations: :verify}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: QueueWorker.Supervisor)
  end

  defp worker_concurrency do
    "QUEUE_WORKER_CONCURRENCY" |> System.get_env("3") |> String.to_integer()
  end
end
