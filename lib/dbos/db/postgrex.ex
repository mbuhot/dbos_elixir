defmodule Dbos.DB.Postgrex do
  @moduledoc "`Dbos.DB` adapter over a bare Postgrex pool pid or name."

  @behaviour Dbos.DB

  alias Dbos.DB.Isolation

  @impl Dbos.DB
  def query(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> {:ok, %{rows: result.rows || [], num_rows: result.num_rows}}
      {:error, error} -> {:error, error}
    end
  end

  @doc "Like `query/3`, raising on error. Convenient inside a transaction function."
  def query!(conn, sql, params) do
    {:ok, result} = query(conn, sql, params)
    result
  end

  @impl Dbos.DB
  def transaction(conn, opts, fun) do
    Postgrex.transaction(conn, fn tx_conn ->
      set_isolation_level(tx_conn, opts[:isolation])
      fun.(tx_conn)
    end)
  end

  @impl Dbos.DB
  def in_transaction?(conn), do: DBConnection.status(conn) in [:transaction, :error]

  @impl Dbos.DB
  def rollback(conn, reason), do: Postgrex.rollback(conn, reason)

  defp set_isolation_level(_conn, nil), do: :ok

  defp set_isolation_level(conn, isolation) do
    query!(conn, Isolation.sql(isolation), [])
  end
end
