defmodule Dbos.EngineBenchTest do
  @moduledoc """
  Performance measurements, excluded from the default run and reported through `IO.puts` rather than
  asserted on — a wall-clock threshold on a developer machine is a flake, so these carry loose
  sanity bounds and leave the numbers to be read.

  Run with `mix test.bench`.

  Each measurement mirrors a workload the upstream Go implementation exercises, so the numbers can be
  read beside it: the sequential two-step workflow is the shape `chaos_tests` runs ten thousand of,
  and the round-trip count is comparable to the statements upstream's `CheckOperationExecution` and
  `RecordOperationResult` issue per step.
  """

  use Dbos.Case, async: false

  @moduletag :bench
  @moduletag timeout: 600_000

  alias Dbos.Config
  alias Dbos.CountingDB
  alias Dbos.Queue
  alias Dbos.Recovery
  alias Dbos.SystemDb
  alias Dbos.Uuid

  @sequential_workflows 200
  @queued_workflows 500
  @status_rows 1_000
  @status_rounds 5
  @idle_seconds 30

  defmodule Workflows do
    @moduledoc false
    use Dbos

    defworkflow two_steps(x), name: "bench.two_steps" do
      x |> step_one() |> step_two()
    end

    defstep(step_one(x), do: x + 1)
    defstep(step_two(x), do: x + 2)
  end

  defp start_engine(opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    defaults = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "bench-#{System.unique_integer([:positive])}",
      workflows: [Workflows],
      lease_sweep: [enabled: false],
      migrations: :skip,
      notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
    ]

    start_supervised!({Dbos.Supervisor, Keyword.merge(defaults, opts)}, id: name)
    Recovery.await_boot_recovery(name)
    name
  end

  defp config do
    %Config{
      db: Dbos.DB.Postgrex,
      conn: Dbos.TestConn,
      executor_id: "bench",
      application_version: "v1"
    }
  end

  defp report(title, rows) do
    IO.puts("\n#{title}")

    Enum.each(rows, fn {label, value} ->
      IO.puts("  #{String.pad_trailing(label, 34)}#{value}")
    end)
  end

  defp ms(microseconds), do: Float.round(microseconds / 1_000, 1)

  defp per_op(microseconds, count), do: Float.round(microseconds / count / 1_000, 3)

  defp run_sequentially(engine, count) do
    Enum.each(1..count, fn i ->
      {:ok, handle} = Dbos.start("bench.two_steps", [i], engine: engine)
      {:ok, result} = Dbos.await(handle, timeout_ms: 30_000)
      ^result = i + 3
    end)
  end

  test "sequential two-step workflows: latency and Postgres round-trips per workflow" do
    CountingDB.reset()
    engine = start_engine(db: {CountingDB, {Dbos.DB.Postgrex, Dbos.TestConn}})

    {elapsed, :ok} = :timer.tc(fn -> run_sequentially(engine, @sequential_workflows) end)
    counts = CountingDB.counts()

    per_workflow = counts.round_trips / @sequential_workflows

    report("sequential two-step workflows (n=#{@sequential_workflows})", [
      {"total", "#{ms(elapsed)} ms"},
      {"per workflow", "#{per_op(elapsed, @sequential_workflows)} ms"},
      {"throughput", "#{Float.round(@sequential_workflows / (elapsed / 1_000_000), 1)} /s"},
      {"statements", counts.statements},
      {"transactions", counts.transactions},
      {"round-trips per workflow", Float.round(per_workflow, 2)}
      | Enum.map(CountingDB.tally(), fn {sql, count} ->
          {"  #{sql}", Float.round(count / @sequential_workflows, 2)}
        end)
    ])

    assert per_workflow < 30, "a two-step workflow should not cost 30 Postgres round-trips"
  end

  # A runner claims at most its free capacity per pass, and a completing workflow notifies nobody, so
  # the next claim waits for the next tick. Throughput is bounded by capacity per polling interval,
  # which these two configurations bracket.
  test "queued workflows: end-to-end throughput through one queue" do
    for {label, queue_opts} <- [
          {"base_polling_interval_ms=1000", [worker_concurrency: 20]},
          {"base_polling_interval_ms=50",
           [worker_concurrency: 20, base_polling_interval_ms: 50, max_polling_interval_ms: 50]}
        ] do
      truncate_tables(Dbos.TestConn)
      queue_name = "bench-q-#{System.unique_integer([:positive])}"
      engine = start_engine(queues: [Queue.new(queue_name, queue_opts)])

      {elapsed, :ok} = :timer.tc(fn -> drain_queue(engine, queue_name) end)

      report("queued workflows (n=#{@queued_workflows}, worker_concurrency=20, #{label})", [
        {"total", "#{ms(elapsed)} ms"},
        {"throughput", "#{Float.round(@queued_workflows / (elapsed / 1_000_000), 1)} /s"}
      ])
    end
  end

  defp drain_queue(engine, queue_name) do
    handles =
      Enum.map(1..@queued_workflows, fn i ->
        {:ok, handle} =
          Dbos.enqueue("bench.two_steps", [i], queue_name: queue_name, engine: engine)

        handle
      end)

    Enum.each(handles, fn handle ->
      {:ok, _result} = Dbos.await(handle, timeout_ms: 120_000)
    end)
  end

  test "status-row writes: the cost the notification triggers add" do
    listening = with_listener(&time_status_writes/0)
    report_trigger_cost("a LISTENer attached", listening)

    quiet = time_status_writes()
    report_trigger_cost("no LISTENer", quiet)
  end

  defp report_trigger_cost(condition, timings) do
    report(
      "workflow_status insert + outcome (n=#{@status_rows} x #{@status_rounds} rounds, #{condition})",
      [
        {"triggers enabled (best round)", "#{ms(timings.enabled)} ms"},
        {"triggers disabled (best round)", "#{ms(timings.disabled)} ms"},
        {"added per workflow", "#{per_op(timings.enabled - timings.disabled, @status_rows)} ms"},
        {"overhead", "#{percent(timings)}%"}
      ]
    )
  end

  test "idle engine: statements issued while there is no work" do
    CountingDB.reset()

    engine =
      start_engine(
        db: {CountingDB, {Dbos.DB.Postgrex, Dbos.TestConn}},
        queues: [Queue.new("idle-q")]
      )

    await_listening(engine)
    CountingDB.reset()
    Process.sleep(5_000)
    first = CountingDB.counts()
    Process.sleep((@idle_seconds - 5) * 1_000)
    whole = CountingDB.counts()

    tail_seconds = @idle_seconds - 5
    tail_rate = (whole.round_trips - first.round_trips) / tail_seconds

    report("idle engine with one queue (#{@idle_seconds}s, LISTEN established)", [
      {"round-trips, first 5s", first.round_trips},
      {"round-trips, whole window", whole.round_trips},
      {"round-trips per second, first 5s", Float.round(first.round_trips / 5, 2)},
      {"round-trips per second, last #{tail_seconds}s", Float.round(tail_rate, 2)}
      | Enum.map(CountingDB.tally(), fn {sql, count} -> {"  #{sql}", count} end)
    ])

    assert tail_rate < 2, "an idle engine should settle below two round-trips a second"
  end

  defp percent(%{enabled: enabled, disabled: disabled}) do
    Float.round((enabled - disabled) / disabled * 100, 1)
  end

  # Alternating rounds, reported as the best of each: a background process stealing the machine
  # inflates a round, and one inflated round in the wrong arm is enough to invert the comparison.
  defp time_status_writes do
    rounds =
      Enum.map(1..@status_rounds, fn _round ->
        enabled = time_status_writes_once()
        set_triggers(:disable)
        disabled = time_status_writes_once()
        set_triggers(:enable)
        %{enabled: enabled, disabled: disabled}
      end)

    %{
      enabled: rounds |> Enum.map(& &1.enabled) |> Enum.min(),
      disabled: rounds |> Enum.map(& &1.disabled) |> Enum.min()
    }
  end

  defp time_status_writes_once do
    config = config()
    ids = Enum.map(1..@status_rows, fn _ -> Uuid.v4() end)

    {elapsed, :ok} =
      :timer.tc(fn ->
        Enum.each(ids, fn id ->
          SystemDb.insert_workflow_status(config, %{
            workflow_id: id,
            status: :pending,
            name: "bench.two_steps"
          })

          SystemDb.update_workflow_outcome(config, id, %{status: :success, output: nil})
        end)
      end)

    truncate_tables(Dbos.TestConn)
    elapsed
  end

  @triggers ~w(
    dbos_ex_queue_insert_trigger
    dbos_ex_queue_update_trigger
    dbos_ex_workflow_status_insert_trigger
    dbos_ex_workflow_status_update_trigger
  )

  defp set_triggers(action) do
    verb = if action == :disable, do: "DISABLE", else: "ENABLE"

    Enum.each(@triggers, fn trigger ->
      Postgrex.query!(
        Dbos.TestConn,
        ~s(ALTER TABLE "dbos".workflow_status #{verb} TRIGGER #{trigger}),
        []
      )
    end)
  end

  defp with_listener(fun) do
    {:ok, pid} =
      Postgrex.Notifications.start_link(database: Application.fetch_env!(:dbos, :test_database))

    {:ok, _} = Postgrex.Notifications.listen(pid, "dbos_workflow_status_channel")
    {:ok, _} = Postgrex.Notifications.listen(pid, "dbos_queue_channel")

    try do
      fun.()
    after
      GenServer.stop(pid)
    end
  end

  defp await_listening(engine, attempts \\ 200)
  defp await_listening(_engine, 0), do: flunk("engine never established its LISTEN connection")

  defp await_listening(engine, attempts) do
    if Dbos.Notifications.mode(engine) == :listen do
      :ok
    else
      Process.sleep(10)
      await_listening(engine, attempts - 1)
    end
  end
end
