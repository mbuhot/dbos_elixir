defmodule DocumentPipeline.PipelineTest do
  use ExUnit.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup
  alias DocumentPipeline.Chunk
  alias DocumentPipeline.Pipeline
  alias DocumentPipeline.Repo

  setup do
    Repo.delete_all(Chunk)

    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Ecto, Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Pipeline],
       queues: [Dbos.Queue.new(Pipeline.queue_name(), worker_concurrency: 4)],
       migrations: :create_if_absent},
      id: name
    )

    Recovery.await_boot_recovery(name)

    counter = :counters.new(1, [])
    handler_id = "embed-counter-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:document_pipeline, :embed],
      fn _event, _measurements, _metadata, counter -> :counters.add(counter, 1, 1) end,
      counter
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, engine: name, config: Dbos.config(name), counter: counter}
  end

  test "recovering a document killed mid-ingestion re-embeds nothing already checkpointed", %{
    engine: engine,
    config: config,
    counter: counter
  } do
    text = String.duplicate("word ", 400)
    document_id = "doc-#{System.unique_integer([:positive])}"

    {:ok, handle} =
      Dbos.start("ingest_document", [document_id, {:text, text}],
        engine: engine,
        workflow_id: document_id
      )

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, document_id)
      length(steps) >= 3
    end)

    {:ok, pid} = WorkflowSup.whereis(engine, document_id)
    Process.exit(pid, :kill)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, document_id)
      status.status == :pending
    end)

    Recovery.recover_pending(engine)

    assert {:ok, _result} = Dbos.await(handle, timeout_ms: 15_000)

    {:ok, steps} = SystemDb.get_workflow_steps(config, document_id)
    embed_steps = Enum.filter(steps, &(&1.function_name == "embed_chunk/3"))
    store_steps = Enum.filter(steps, &(&1.function_name == "store_chunk/4"))
    chunks = Repo.all(Chunk)

    assert length(embed_steps) > 1
    assert length(store_steps) == length(embed_steps)
    assert length(chunks) == length(embed_steps)
    assert :counters.get(counter, 1) == length(embed_steps)
  end

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end
end
