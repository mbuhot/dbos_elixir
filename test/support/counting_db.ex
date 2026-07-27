defmodule Dbos.CountingDB do
  @moduledoc """
  `Dbos.DB` adapter counting the statements and transactions an engine issues, delegating each to an
  inner adapter. Round-trips to Postgres are `statements + 2 * transactions`, the two being the
  `BEGIN` and `COMMIT` a transaction adds.

  Configure it with the inner adapter as its connection, so it wraps either backend:

      db: {Dbos.CountingDB, {Dbos.DB.Postgrex, Dbos.TestConn}}
      db: {Dbos.CountingDB, {Dbos.DB.Ecto, Dbos.TestRepo}}
  """

  @behaviour Dbos.DB

  @statements {__MODULE__, :statements}
  @transactions {__MODULE__, :transactions}
  @tally __MODULE__.Tally

  @doc "Zeroes both counters and the per-statement tally."
  def reset do
    :persistent_term.put(@statements, :counters.new(1, [:write_concurrency]))
    :persistent_term.put(@transactions, :counters.new(1, [:write_concurrency]))

    case :ets.whereis(@tally) do
      :undefined -> :ets.new(@tally, [:public, :named_table, :set, write_concurrency: true])
      _table -> :ets.delete_all_objects(@tally)
    end

    :ok
  end

  @doc "The counts since the last `reset/0`."
  def counts do
    statements = :counters.get(:persistent_term.get(@statements), 1)
    transactions = :counters.get(:persistent_term.get(@transactions), 1)

    %{
      statements: statements,
      transactions: transactions,
      round_trips: statements + 2 * transactions
    }
  end

  @doc "Statement counts by the first line of each statement, most frequent first."
  def tally do
    @tally
    |> :ets.tab2list()
    |> Enum.sort_by(fn {_sql, count} -> -count end)
  end

  @impl Dbos.DB
  def query({adapter, conn}, sql, params) do
    :counters.add(:persistent_term.get(@statements), 1, 1)
    record(sql)
    adapter.query(conn, sql, params)
  end

  @impl Dbos.DB
  def transaction({adapter, conn}, opts, fun) do
    :counters.add(:persistent_term.get(@transactions), 1, 1)
    adapter.transaction(conn, opts, fn tx_conn -> fun.({adapter, tx_conn}) end)
  end

  @impl Dbos.DB
  def in_transaction?({adapter, conn}), do: adapter.in_transaction?(conn)

  @impl Dbos.DB
  def rollback({adapter, conn}, reason), do: adapter.rollback(conn, reason)

  # An engine outliving the process that called reset/0 still issues statements — a lease expiring on
  # shutdown, say — and by then the tally table has gone with its owner.
  defp record(sql) do
    case :ets.whereis(@tally) do
      :undefined -> :ok
      _table -> :ets.update_counter(@tally, fingerprint(sql), 1, {fingerprint(sql), 0})
    end
  end

  defp fingerprint(sql) do
    sql |> String.split("\n", parts: 2) |> hd() |> String.trim()
  end
end
