defmodule Dbos.Test.FaultyDB do
  @moduledoc """
  A `Dbos.DB` adapter wrapping `Dbos.DB.Postgrex` that raises when writing an
  `operation_outputs` row for a configured `workflow_uuid`, simulating a crash mid-transaction
  so tests can assert what does (and does not) get durably committed around it.
  """

  @behaviour Dbos.DB

  @key {__MODULE__, :fault_target}

  @doc "Every `operation_outputs` insert naming `workflow_id` as its `workflow_uuid` raises instead of executing."
  def set_fault_target(workflow_id), do: :persistent_term.put(@key, workflow_id)

  @doc "Clears any configured fault target."
  def clear_fault_target, do: :persistent_term.erase(@key)

  @impl Dbos.DB
  def query(conn, sql, params) do
    if operation_outputs_insert?(sql) and fault_target() in params do
      raise "simulated crash recording an operation_outputs row"
    else
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

  defp fault_target, do: :persistent_term.get(@key, nil)
end
