# Backoff retry around a Dbos.DB adapter call, for transient connection-level failures.
#
# A Postgres connection dropped underneath a live pool — a killed backend, a restarted server, a
# pool member mid-reconnect — surfaces to the caller as an ordinary {:error, reason} rather than a
# raise. Retrying such a call reaches a healthy connection once the pool has re-established one.
#
# Two error classes are recognised. retryable_connection_error?/1 covers connection-level
# failures: DBConnection.ConnectionError, the 08*/57P0* SQLSTATEs, and driver-level Postgrex.Errors
# carrying no server response. retryable_transaction_error?/1 covers serialization_failure and
# deadlock_detected, which abort the whole transaction and are opted into per call site.
#
# The schedule is exponential with jitter, configurable under
# config :dbos, :system_db_retry, max_attempts: _, base_delay_ms: _, max_delay_ms: _, factor: _.
# Attempts are bounded so an unreachable database surfaces an error instead of blocking forever.
defmodule Dbos.DB.Retry do
  @moduledoc false

  @jitter_min 0.95
  @jitter_max 1.05

  @defaults [max_attempts: 10, base_delay_ms: 100, max_delay_ms: 30_000, factor: 2.0]

  @connection_sqlstates ~w(
    connection_exception
    connection_does_not_exist
    connection_failure
    sqlclient_unable_to_establish_sqlconnection
    sqlserver_rejected_establishment_of_sqlconnection
    transaction_resolution_unknown
    protocol_violation
    admin_shutdown
    crash_shutdown
    cannot_connect_now
  )a

  @doc """
  Runs `sql` against `config`'s adapter, retrying transient connection failures. `opts[:retry]`
  is `true` (the default) or `false` to disable retrying for a non-idempotent statement.
  """
  def query(config, sql, params, opts \\ []) do
    run(config, opts, fn -> config.db.query(config.conn, sql, params) end)
  end

  @doc """
  Runs `fun` inside a transaction on `config`'s adapter, retrying the whole transaction.
  `opts[:retry_conflicts]` additionally retries serialization failures and deadlocks.
  """
  def transaction(config, tx_opts, fun, opts \\ []) do
    run(config, opts, fn -> config.db.transaction(config.conn, tx_opts, fun) end)
  end

  @doc "Whether `error` is a transient connection-level failure."
  def retryable_connection_error?(%Dbos.SystemDbError{cause: cause}),
    do: retryable_connection_error?(cause)

  def retryable_connection_error?(%DBConnection.ConnectionError{}), do: true
  def retryable_connection_error?(%{postgres: %{code: code}}), do: code in @connection_sqlstates
  def retryable_connection_error?(%Postgrex.Error{postgres: nil}), do: true
  def retryable_connection_error?(_error), do: false

  @doc "Whether `error` is a transaction-level conflict that a fresh transaction may win."
  def retryable_transaction_error?(%Dbos.SystemDbError{cause: cause}),
    do: retryable_transaction_error?(cause)

  def retryable_transaction_error?(%{postgres: %{code: code}}),
    do: code in [:serialization_failure, :deadlock_detected]

  def retryable_transaction_error?(_error), do: false

  defp run(config, opts, work) do
    if Keyword.get(opts, :retry, true) do
      attempt(config, opts, work, 1)
    else
      work.()
    end
  end

  defp attempt(config, opts, work, attempt_number) do
    try do
      work.()
    rescue
      error ->
        if retry_again?(config, opts, attempt_number, error) do
          attempt(config, opts, work, attempt_number + 1)
        else
          reraise error, __STACKTRACE__
        end
    else
      {:error, error} -> retry_or_return(config, opts, work, attempt_number, error)
      other -> other
    end
  end

  defp retry_or_return(config, opts, work, attempt_number, error) do
    if retry_again?(config, opts, attempt_number, error) do
      attempt(config, opts, work, attempt_number + 1)
    else
      {:error, error}
    end
  end

  defp retry_again?(config, opts, attempt_number, error) do
    if attempt_number < max_attempts() and retry?(config, opts, error) do
      Process.sleep(delay_ms(attempt_number))
      true
    else
      false
    end
  end

  defp retry?(config, opts, error) do
    not inside_transaction?(config) and classified_retryable?(opts, error)
  end

  defp inside_transaction?(config) do
    config.db.in_transaction?(config.conn)
  rescue
    _error -> false
  end

  defp classified_retryable?(opts, error) do
    retryable_connection_error?(error) or
      (Keyword.get(opts, :retry_conflicts, false) and retryable_transaction_error?(error))
  end

  defp delay_ms(attempt_number) do
    settings = settings()

    base =
      Keyword.fetch!(settings, :base_delay_ms) * :math.pow(settings[:factor], attempt_number - 1)

    capped = min(base, Keyword.fetch!(settings, :max_delay_ms))
    round(capped * (@jitter_min + :rand.uniform() * (@jitter_max - @jitter_min)))
  end

  defp max_attempts, do: Keyword.fetch!(settings(), :max_attempts)

  defp settings, do: Keyword.merge(@defaults, Application.get_env(:dbos, :system_db_retry, []))
end
