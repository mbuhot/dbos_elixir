if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.DB.Ecto do
    @moduledoc """
    `Dbos.DB` adapter over a host application's Ecto repo. Transactions open with
    `Repo.transaction/2`, never `Postgrex.transaction/3` on the underlying pool, so a user's own
    `Repo` calls made inside a transactional step enlist on the same connection.
    """

    @behaviour Dbos.DB

    @isolation_sql %{
      read_committed: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
      repeatable_read: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
      serializable: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    }

    @impl Dbos.DB
    def query(repo, sql, params) do
      case repo.query(sql, params) do
        {:ok, result} -> {:ok, %{rows: result.rows || [], num_rows: result.num_rows}}
        {:error, error} -> {:error, error}
      end
    end

    @doc "Like `query/3`, raising on error. Convenient inside a transaction function."
    def query!(repo, sql, params) do
      {:ok, result} = query(repo, sql, params)
      result
    end

    @impl Dbos.DB
    def transaction(repo, opts, fun) do
      repo.transaction(fn ->
        set_isolation_level(repo, opts[:isolation])
        fun.(repo)
      end)
    end

    @impl Dbos.DB
    def in_transaction?(repo), do: repo.in_transaction?()

    defp set_isolation_level(_repo, nil), do: :ok

    defp set_isolation_level(repo, isolation) do
      query!(repo, Map.fetch!(@isolation_sql, isolation), [])
    end
  end
end
