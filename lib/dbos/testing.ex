defmodule Dbos.Testing do
  @moduledoc """
  Synchronous drivers for an `:inline`/`:manual` testing-mode engine (`Dbos.Supervisor`'s
  `:testing` option). Everything here runs on the calling process, touching only the connection
  that process already owns — the same property that makes these modes compatible with
  `Ecto.Adapters.SQL.Sandbox`.
  """

  alias Dbos.Queue
  alias Dbos.Registry, as: WorkflowRegistry
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  @doc """
  Claims and runs, synchronously in the caller, every currently dispatchable workflow on
  `queue_name`. `opts[:engine]` defaults to `Dbos`. Returns how many ran.
  """
  def drain_queue(queue_name, opts \\ []) do
    config = engine_config(opts)
    queue = fetch_queue!(config, queue_name)

    SystemDb.transition_delayed_workflows(config)

    config
    |> partition_keys(queue)
    |> Enum.reduce(0, fn partition_key, total ->
      total + drain_partition(config, queue, partition_key)
    end)
  end

  @doc "Drains every declared queue (including the internal queue), synchronously. Returns the total run."
  def drain_all(opts \\ []) do
    config = engine_config(opts)

    config.queues
    |> Enum.map(& &1.name)
    |> Enum.reduce(0, fn queue_name, total -> total + drain_queue(queue_name, opts) end)
  end

  @doc """
  Runs a synchronous recovery pass for `opts[:engine]` (default `Dbos`): every `PENDING`
  workflow this executor owns is redispatched in the caller. Returns how many it acted on.
  """
  def recover_pending(opts \\ []) do
    opts
    |> Keyword.get(:engine, Dbos)
    |> Dbos.Recovery.recover_pending()
    |> length()
  end

  defp engine_config(opts), do: opts |> Keyword.get(:engine, Dbos) |> Dbos.config()

  defp fetch_queue!(config, queue_name) do
    Enum.find(config.queues, &(&1.name == queue_name)) ||
      raise ArgumentError,
            "queue #{inspect(queue_name)} is not declared on engine #{inspect(config.name)}"
  end

  defp partition_keys(_config, %Queue{partition_queue: false}), do: [nil]

  defp partition_keys(config, %Queue{partition_queue: true} = queue),
    do: SystemDb.get_queue_partitions(config, queue.name)

  defp drain_partition(config, queue, partition_key) do
    case SystemDb.dequeue_workflows(config, queue,
           partition_key: partition_key,
           local_running_count: 0
         ) do
      [] ->
        0

      claimed ->
        Enum.each(claimed, &dispatch(config, &1, queue.name, partition_key))
        length(claimed) + drain_partition(config, queue, partition_key)
    end
  end

  defp dispatch(config, %{workflow_id: id, name: name, inputs: inputs}, queue_name, partition_key) do
    case WorkflowRegistry.lookup(config.name, name) do
      {:ok, mfa} ->
        WorkflowSup.start_workflow(config.name, id, mfa, inputs,
          queue_name: queue_name,
          partition_key: partition_key
        )

      :error ->
        raise "dbos: queue #{inspect(queue_name)}: workflow #{inspect(name)} (#{id}) is not " <>
                "registered on this executor"
    end
  end
end
