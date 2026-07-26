# :telemetry.span/3 wrappers for the engine's instrumented spans — workflow execution, step
# execution, queue dequeue, recovery passes, and blocking waits. Every event, its measurements,
# and its metadata are catalogued in docs/telemetry.md.
defmodule Dbos.Telemetry do
  @moduledoc false

  @doc "Spans a workflow body's execution: `[:dbos, :workflow, :start | :stop | :exception]`."
  def span_workflow(metadata, fun) do
    :telemetry.span([:dbos, :workflow], metadata, fn -> {fun.(), metadata} end)
  end

  @doc "Spans one step's actual execution (skipped entirely on replay): `[:dbos, :step, ...]`."
  def span_step(metadata, fun) do
    :telemetry.span([:dbos, :step], metadata, fn -> {fun.(), metadata} end)
  end

  @doc "Spans one queue's per-tick dequeue call: `[:dbos, :queue, :dequeue, ...]`."
  def span_dequeue(metadata, fun) do
    :telemetry.span([:dbos, :queue, :dequeue], metadata, fn ->
      claimed = fun.()
      {claimed, Map.put(metadata, :count, length(claimed))}
    end)
  end

  @doc "Spans one recovery/reclaim pass: `[:dbos, :recovery, ...]`."
  def span_recovery(metadata, fun) do
    :telemetry.span([:dbos, :recovery], metadata, fn -> {fun.(), metadata} end)
  end

  @doc """
  Spans one blocking wait — sleep, recv, or get_event: `[:dbos, :wait, ...]`. `fun` returns
  `{result, outcome}` and `outcome` (`:resolved` or `:timeout`) is merged into the `:stop`
  metadata. A `Dbos.Waits.Parked` unwinding the wait stops the span with `outcome: :parked` and
  re-raises, so parking reads as an end to the wait rather than a failure.
  """
  def span_wait(metadata, fun) do
    start_time = System.monotonic_time()
    :telemetry.execute([:dbos, :wait, :start], %{system_time: System.system_time()}, metadata)

    try do
      fun.()
    rescue
      parked in Dbos.Waits.Parked ->
        stop_wait(start_time, metadata, :parked)
        reraise parked, __STACKTRACE__
    catch
      kind, reason ->
        :telemetry.execute(
          [:dbos, :wait, :exception],
          %{duration: System.monotonic_time() - start_time},
          Map.merge(metadata, %{kind: kind, reason: reason, stacktrace: __STACKTRACE__})
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    else
      {result, outcome} ->
        stop_wait(start_time, metadata, outcome)
        result
    end
  end

  defp stop_wait(start_time, metadata, outcome) do
    :telemetry.execute(
      [:dbos, :wait, :stop],
      %{duration: System.monotonic_time() - start_time},
      Map.put(metadata, :outcome, outcome)
    )
  end
end
