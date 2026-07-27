defmodule Dbos.WorkflowLatencyBenchTest do
  @moduledoc """
  The workload from `dbos-inc/dbos-workflow-benchmarks`, ported: a workflow that invokes one
  transaction `i` times, timed server-side around the start-and-await, reported as a latency
  distribution over `n` iterations.

  Run with `mix test.bench`.

  The transaction is theirs statement for statement — read `greet_count` for a name, then `UPDATE` it
  or `INSERT` the row — against their `dbos_hello` table, on the same Postgres the engine keeps its
  system tables in, which is how their DBOS Cloud deployment is arranged.

  Wall-clock here is not comparable to a number published from DBOS Cloud: a different machine, and a
  local socket in place of a network hop. What does compare is the slope — the marginal cost of one
  more transaction in the same workflow — and the round-trips that slope is made of.

  The round-trip counts cover the engine's own statements. The body's two go through the `Repo`
  directly, so add two per transaction for the total a connection carries.
  """

  use Dbos.Case, async: false

  @moduletag :bench
  @moduletag timeout: 600_000

  alias Dbos.CountingDB
  alias Dbos.Recovery

  @transactions_per_workflow [1, 2, 5, 10, 20]
  @iterations 100
  @warmup 20

  defmodule Bench do
    @moduledoc false
    use Dbos, repo: Dbos.TestRepo

    defworkflow greet_many(count), name: "bench.greet_many" do
      Enum.reduce(1..count, nil, fn _iteration, _previous -> greet("dbos") end)
    end

    deftransaction greet(user) do
      %{rows: [[greet_count]]} =
        Dbos.TestRepo.query!(
          "SELECT COALESCE((SELECT greet_count FROM dbos_hello WHERE name = $1), 0)",
          [user]
        )

      write =
        if greet_count > 0 do
          "UPDATE dbos_hello SET greet_count = $1 WHERE name = $2"
        else
          "INSERT INTO dbos_hello (greet_count, name) VALUES ($1, $2)"
        end

      Dbos.TestRepo.query!(write, [greet_count + 1, user])
      "Hello, #{user}! You have been greeted #{greet_count + 1} times.\n"
    end
  end

  setup do
    Postgrex.query!(Dbos.TestConn, "DROP TABLE IF EXISTS dbos_hello", [])

    Postgrex.query!(
      Dbos.TestConn,
      "CREATE TABLE dbos_hello (name TEXT PRIMARY KEY, greet_count INTEGER DEFAULT 0)",
      []
    )

    :ok
  end

  defp start_engine(inner) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       [
         name: name,
         db: inner,
         executor_id: "bench-#{System.unique_integer([:positive])}",
         workflows: [Bench],
         lease_sweep: [enabled: false],
         migrations: :skip,
         notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
       ]},
      id: name
    )

    Recovery.await_boot_recovery(name)
    name
  end

  # The handler upstream times: start the clock, invoke the workflow, await its result, stop.
  defp server_side_duration_us(engine, count) do
    {elapsed, {:ok, _output}} =
      :timer.tc(fn ->
        {:ok, handle} = Dbos.start("bench.greet_many", [count], engine: engine)
        Dbos.await(handle, timeout_ms: 60_000)
      end)

    elapsed
  end

  defp latencies(engine, count, iterations) do
    Enum.each(1..@warmup, fn _ -> server_side_duration_us(engine, count) end)
    Enum.map(1..iterations, fn _ -> server_side_duration_us(engine, count) end)
  end

  defp summarise(latencies) do
    sorted = Enum.sort(latencies)
    n = length(sorted)

    %{
      mean: Enum.sum(sorted) / n,
      min: hd(sorted),
      median: Enum.at(sorted, div(n, 2)),
      p95: Enum.at(sorted, min(n - 1, ceil(0.95 * n) - 1)),
      max: List.last(sorted)
    }
  end

  defp ms(microseconds), do: :erlang.float_to_binary(microseconds / 1_000, decimals: 2)

  test "workflow latency by transactions per workflow" do
    engine = start_engine({Dbos.DB.Ecto, Dbos.TestRepo})

    measurements =
      Enum.map(@transactions_per_workflow, fn count ->
        {count, summarise(latencies(engine, count, @iterations))}
      end)

    IO.puts("\nserver-side workflow duration, ms (n=#{@iterations} after #{@warmup} warmup)")
    IO.puts("  txns  mean     min      median   p95      max")

    Enum.each(measurements, fn {count, stats} ->
      IO.puts(
        "  " <>
          String.pad_trailing(to_string(count), 6) <>
          Enum.map_join([stats.mean, stats.min, stats.median, stats.p95, stats.max], fn value ->
            String.pad_trailing(ms(value), 9)
          end)
      )
    end)

    report_slope(measurements)

    assert Enum.all?(measurements, fn {_count, stats} -> stats.mean > 0 end)
  end

  # Least squares through the medians, which a single stalled iteration cannot drag. The intercept is
  # what a workflow costs before its first transaction — the status insert, the outcome, the await —
  # and the slope is one transaction.
  defp report_slope(measurements) do
    points = Enum.map(measurements, fn {count, stats} -> {count, stats.median / 1_000} end)
    n = length(points)
    sum_x = Enum.sum(Enum.map(points, &elem(&1, 0)))
    sum_y = Enum.sum(Enum.map(points, &elem(&1, 1)))
    sum_xy = Enum.sum(Enum.map(points, fn {x, y} -> x * y end))
    sum_xx = Enum.sum(Enum.map(points, fn {x, _y} -> x * x end))

    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
    intercept = (sum_y - slope * sum_x) / n

    IO.puts("\n  per transaction (slope)           #{Float.round(slope, 3)} ms")
    IO.puts("  fixed per workflow (intercept)    #{Float.round(intercept, 3)} ms")
  end

  test "round-trips by transactions per workflow" do
    CountingDB.reset()
    engine = start_engine({CountingDB, {Dbos.DB.Ecto, Dbos.TestRepo}})

    counted =
      Enum.map(@transactions_per_workflow, fn count ->
        CountingDB.reset()
        Enum.each(1..10, fn _ -> server_side_duration_us(engine, count) end)
        {count, CountingDB.counts().round_trips / 10}
      end)

    IO.puts("\nPostgres round-trips per workflow")
    IO.puts("  txns  round-trips")

    Enum.each(counted, fn {count, round_trips} ->
      IO.puts("  " <> String.pad_trailing(to_string(count), 6) <> to_string(round_trips))
    end)

    {low_count, low} = hd(counted)
    {high_count, high} = List.last(counted)
    marginal = (high - low) / (high_count - low_count)

    IO.puts("\n  per transaction                   #{Float.round(marginal, 2)} round-trips")
    IO.puts("  fixed per workflow                #{Float.round(low - marginal, 2)} round-trips")

    IO.puts("\n  statements at #{high_count} transactions per workflow")

    Enum.each(CountingDB.tally(), fn {sql, count} ->
      IO.puts("    #{String.pad_trailing(sql, 62)}#{Float.round(count / 10, 2)}")
    end)

    assert marginal < 12, "a transaction should not cost twelve Postgres round-trips"
  end
end
