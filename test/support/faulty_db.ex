defmodule Dbos.Test.FaultyDB do
  @moduledoc """
  A `Dbos.DB` adapter wrapping `Dbos.DB.Postgrex` with two fault modes.

  A *fault target* makes every `operation_outputs` insert naming a given `workflow_uuid` raise,
  simulating a crash mid-transaction so tests can assert what does (and does not) get durably
  committed around it.

  An *injection* makes the next `n` statements matching a SQL fragment return
  `{:error, %DBConnection.ConnectionError{}}` without reaching the database, simulating the
  transient failure a pool member surfaces while reconnecting.
  """

  @behaviour Dbos.DB

  @fault_key {__MODULE__, :fault_target}
  @injection_key {__MODULE__, :injection}

  @doc "Every `operation_outputs` insert naming `workflow_id` as its `workflow_uuid` raises instead of executing."
  def set_fault_target(workflow_id), do: :persistent_term.put(@fault_key, workflow_id)

  @doc "Clears any configured fault target."
  def clear_fault_target, do: :persistent_term.erase(@fault_key)

  @doc """
  Fails the next statements whose SQL contains every fragment in `sql_fragments` with a transient
  connection error. `opts[:times]` (default `1`) bounds how many; `opts[:param]`, when given,
  further requires that value to appear in the statement's parameters.
  """
  def inject_connection_error(sql_fragments, opts \\ []) do
    counter = :counters.new(2, [:atomics])
    :counters.put(counter, 1, Keyword.get(opts, :times, 1))

    :persistent_term.put(@injection_key, %{
      fragments: List.wrap(sql_fragments),
      param: Keyword.get(opts, :param),
      counter: counter
    })

    :ok
  end

  @doc "Clears any configured injection."
  def clear_injection, do: :persistent_term.erase(@injection_key)

  @doc "How many statements the current injection has failed so far."
  def injected_count do
    case :persistent_term.get(@injection_key, nil) do
      nil -> 0
      %{counter: counter} -> :counters.get(counter, 2)
    end
  end

  @impl Dbos.DB
  def query(conn, sql, params) do
    cond do
      faulted?(sql, params) ->
        raise "simulated crash recording an operation_outputs row"

      inject?(sql, params) ->
        {:error, %DBConnection.ConnectionError{message: "simulated transient connection loss"}}

      true ->
        Dbos.DB.Postgrex.query(conn, sql, params)
    end
  end

  @impl Dbos.DB
  def transaction(conn, opts, fun), do: Dbos.DB.Postgrex.transaction(conn, opts, fun)

  @impl Dbos.DB
  def in_transaction?(conn), do: Dbos.DB.Postgrex.in_transaction?(conn)

  @impl Dbos.DB
  def rollback(conn, reason), do: Dbos.DB.Postgrex.rollback(conn, reason)

  defp operation_outputs_insert?(sql),
    do: String.contains?(sql, "INSERT INTO") and String.contains?(sql, "operation_outputs")

  defp faulted?(sql, params) do
    case :persistent_term.get(@fault_key, nil) do
      nil -> false
      workflow_id -> operation_outputs_insert?(sql) and workflow_id in params
    end
  end

  defp inject?(sql, params) do
    case :persistent_term.get(@injection_key, nil) do
      nil -> false
      injection -> matches?(injection, sql, params) and take(injection.counter)
    end
  end

  defp matches?(%{fragments: fragments, param: param}, sql, params) do
    Enum.all?(fragments, &String.contains?(sql, &1)) and (is_nil(param) or param in params)
  end

  defp take(counter) do
    if :counters.get(counter, 1) > 0 do
      :counters.sub(counter, 1, 1)
      :counters.add(counter, 2, 1)
      true
    else
      false
    end
  end
end
