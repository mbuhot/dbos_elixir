defmodule Dbos.Status do
  @moduledoc "The seven workflow statuses stored as strings in `workflow_status.status`, as atoms."

  @type t ::
          :pending
          | :enqueued
          | :delayed
          | :success
          | :error
          | :cancelled
          | :max_recovery_attempts_exceeded

  @strings %{
    pending: "PENDING",
    enqueued: "ENQUEUED",
    delayed: "DELAYED",
    success: "SUCCESS",
    error: "ERROR",
    cancelled: "CANCELLED",
    max_recovery_attempts_exceeded: "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
  }

  @atoms Map.new(@strings, fn {atom, string} -> {string, atom} end)

  @terminal [:success, :error, :cancelled, :max_recovery_attempts_exceeded]

  @retryable [:error, :cancelled, :max_recovery_attempts_exceeded]

  @doc "Converts a status atom to its stored string."
  def to_string(status), do: Map.fetch!(@strings, status)

  @doc "Parses a stored status string, raising `ArgumentError` on an unknown value."
  def from_string(string) do
    case Map.fetch(@atoms, string) do
      {:ok, status} -> status
      :error -> raise ArgumentError, "unknown workflow status #{inspect(string)}"
    end
  end

  @doc "Whether a status is terminal (no further transition is possible)."
  def terminal?(status), do: status in @terminal

  @doc """
  The statuses `Dbos.retry/2` restarts a workflow from: every terminal status except `:success`.
  A `:success` row's output has already been observed by its callers, so its run is final.
  """
  def retryable, do: @retryable

  @doc "Whether `Dbos.retry/2` restarts a workflow at this status."
  def retryable?(status), do: status in @retryable
end
