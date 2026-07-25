# Registers this engine's declared queues and supervises one Dbos.Queue.Runner per queue, plus one
# runner for the always-present internal queue (Dbos.Queue.internal_queue_name/0) — the target
# Dbos.resume/2 and Dbos.fork/3 re-enqueue onto by default. Declared queues are static, from
# Dbos.Supervisor's :queues option; this does not reconcile against queues added to the queues
# table by another process at runtime.
defmodule Dbos.Queue.Sup do
  @moduledoc false

  use Supervisor

  alias Dbos.Queue
  alias Dbos.SystemDb

  @doc "Starts the queue supervisor for the engine named `opts[:name]`, registering `opts[:queues]`."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :name)
    Supervisor.start_link(__MODULE__, opts, name: process_name(engine))
  end

  @impl true
  def init(opts) do
    engine = Keyword.fetch!(opts, :name)
    queues = [internal_queue() | Keyword.get(opts, :queues, [])]
    config = Dbos.config(engine)

    Enum.each(queues, &SystemDb.register_queue(config, &1))

    children = [
      {Registry, keys: :unique, name: Queue.Runner.registry_name(engine)}
      | Enum.map(queues, &runner_child_spec(engine, &1))
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp internal_queue, do: %Queue{name: Queue.internal_queue_name()}

  defp runner_child_spec(engine, %Queue{} = queue) do
    Supervisor.child_spec({Queue.Runner, engine: engine, queue: queue},
      id: {Queue.Runner, queue.name}
    )
  end

  defp process_name(engine), do: Module.concat(engine, Queue.Sup)
end
