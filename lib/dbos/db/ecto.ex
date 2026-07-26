if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.DB.Ecto do
    @moduledoc """
    `Dbos.DB` adapter over a host application's Ecto repo. Transactions open with
    `Repo.transaction/2`, never `Postgrex.transaction/3` on the underlying pool, so a user's own
    `Repo` calls made inside a transactional step enlist on the same connection.

    A repo pooled on `Ecto.Adapters.SQL.Sandbox` skips `SET TRANSACTION ISOLATION LEVEL`, which
    Postgres accepts only as a transaction's first statement and the sandbox holds one open for
    the whole test. A sandbox runs serially, so the `:repeatable_read` that queue claiming asks
    for protects against a concurrent claimer that cannot exist there.
    """

    @behaviour Dbos.DB

    alias Dbos.DB.Isolation

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
      sandboxed? = sandboxed?(repo)

      repo.transaction(fn ->
        set_isolation_level(repo, opts[:isolation], sandboxed?)
        fun.(repo)
      end)
    end

    @impl Dbos.DB
    def in_transaction?(repo), do: repo.in_transaction?()

    @impl Dbos.DB
    def rollback(repo, reason), do: repo.rollback(reason)

    defp sandboxed?(repo), do: repo.config()[:pool] == Ecto.Adapters.SQL.Sandbox

    defp set_isolation_level(_repo, nil, _sandboxed?), do: :ok

    defp set_isolation_level(_repo, _isolation, true), do: :ok

    defp set_isolation_level(repo, isolation, false) do
      query!(repo, Isolation.sql(isolation), [])
    end
  end
end
