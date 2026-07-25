defmodule Outbox.ExternalSystem do
  @moduledoc """
  A stand-in for whatever this outbox actually publishes to — a message broker, a webhook, a
  downstream API. `fail_next/1` arms one event type to raise on its very next `publish/2` call,
  so tests and the demo task can exercise the retry path on demand instead of it always
  succeeding. `attempts/1` reports how many times `publish/2` has actually been invoked for an
  event type, so a test can prove a failed attempt was retried rather than silently dropped.
  """

  require Logger

  @fail_key {__MODULE__, :failing_event_types}
  @attempts_key {__MODULE__, :attempts}

  @doc "Publishes `payload` for `event_type`. Raises if that event type was armed via `fail_next/1`."
  def publish(event_type, payload) do
    count_attempt(event_type)

    if armed_to_fail?(event_type) do
      disarm(event_type)
      raise "external system rejected #{event_type}"
    else
      Logger.info("ExternalSystem: published #{event_type} #{inspect(payload)}")
      :ok
    end
  end

  @doc "Arms `event_type` to raise on its very next `publish/2` call, then behave normally again."
  def fail_next(event_type) do
    :persistent_term.put(@fail_key, armed_types() |> MapSet.put(event_type))
  end

  @doc "How many times `publish/2` has been called for `event_type` so far."
  def attempts(event_type), do: attempt_counts() |> Map.get(event_type, 0)

  @doc "Clears every armed failure and every attempt counter. For test isolation."
  def reset! do
    :persistent_term.put(@fail_key, MapSet.new())
    :persistent_term.put(@attempts_key, %{})
  end

  defp count_attempt(event_type) do
    counts = attempt_counts()
    :persistent_term.put(@attempts_key, Map.update(counts, event_type, 1, &(&1 + 1)))
  end

  defp attempt_counts, do: :persistent_term.get(@attempts_key, %{})

  defp armed_to_fail?(event_type), do: MapSet.member?(armed_types(), event_type)
  defp disarm(event_type), do: :persistent_term.put(@fail_key, armed_types() |> MapSet.delete(event_type))
  defp armed_types, do: :persistent_term.get(@fail_key, MapSet.new())
end
