defmodule Dbos.DB do
  @moduledoc """
  Behaviour abstracting the SQL connection `Dbos.SystemDb` runs against, so the same queries work
  over a bare Postgrex pool or over a host application's Ecto repo.
  """

  @type conn :: term
  @type isolation :: :read_committed | :repeatable_read | :serializable

  @callback query(conn, sql :: String.t(), params :: [term]) ::
              {:ok, %{rows: [[term]], num_rows: non_neg_integer}} | {:error, term}

  @callback transaction(conn, opts :: keyword, (conn -> term)) :: {:ok, term} | {:error, term}

  @callback in_transaction?(conn) :: boolean
end
