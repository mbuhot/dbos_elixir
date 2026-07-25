defmodule Dbos.WorkflowSup do
  @moduledoc """
  The `DynamicSupervisor` owning every running workflow process for one engine. Children are
  `:temporary`: a workflow process that dies is recovered from its checkpoints by
  `Dbos.Recovery`, never by an OTP restart, since a restart would re-invoke the workflow body
  outside the replay path.
  """

  use DynamicSupervisor

  @doc "Starts the workflow supervisor for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    DynamicSupervisor.start_link(__MODULE__, [], name: process_name(engine_name))
  end

  @impl true
  def init(_init_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Starts a workflow process running `mfa` with `args` under `workflow_id`. `opts`: `:replay`
  (default `false`), `:queue_name`, `:partition_key` (both default `nil`, used only for the
  live-count registry the queue dequeue needs).
  """
  def start_workflow(engine_name, workflow_id, mfa, args, opts \\ []) do
    process_args = %{
      config: Dbos.config(engine_name),
      engine: engine_name,
      workflow_id: workflow_id,
      mfa: mfa,
      args: args,
      replay: Keyword.get(opts, :replay, false),
      queue_name: Keyword.get(opts, :queue_name),
      partition_key: Keyword.get(opts, :partition_key),
      caller: self()
    }

    case DynamicSupervisor.start_child(
           process_name(engine_name),
           {Dbos.WorkflowProcess, process_args}
         ) do
      {:ok, pid} = ok ->
        await_registration(pid)
        ok

      other ->
        other
    end
  end

  defp await_registration(pid) do
    receive do
      {:dbos_workflow_registered, ^pid} -> :ok
    after
      1_000 -> :ok
    end
  end

  @doc "The pid of the running workflow process for `workflow_id`, if any. `:error` if the engine's process registry is no longer running."
  def whereis(engine_name, workflow_id) do
    case Registry.lookup(process_registry_name(engine_name), workflow_id) do
      [{pid, _queue_key}] -> {:ok, pid}
      [] -> :error
    end
  rescue
    ArgumentError -> :error
  end

  @doc "How many workflow processes are currently running for `queue_name`/`partition_key`."
  def count_running(engine_name, queue_name, partition_key) do
    match_spec = [{{:_, :_, {queue_name, partition_key}}, [], [true]}]
    Registry.count_select(process_registry_name(engine_name), match_spec)
  end

  @doc "The name of this engine's `DynamicSupervisor`."
  def process_name(engine_name), do: Module.concat(engine_name, WorkflowSup)

  @doc "The name of this engine's process-tracking `Registry`, keyed by workflow id."
  def process_registry_name(engine_name), do: Module.concat(engine_name, ProcessRegistry)
end
