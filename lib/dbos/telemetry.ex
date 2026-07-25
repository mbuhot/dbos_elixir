defmodule Dbos.Telemetry do
  @moduledoc """
  `:telemetry.span/3` wrappers for the engine's four instrumented spans — workflow execution,
  step execution, queue dequeue, and recovery passes. Every event, its measurements, and its
  metadata are catalogued in `docs/telemetry.md`.
  """

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
end
