# The SET TRANSACTION ISOLATION LEVEL SQL for each Dbos.DB.isolation level, shared by every
# Dbos.DB adapter.
defmodule Dbos.DB.Isolation do
  @moduledoc false

  @isolation_sql %{
    read_committed: "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
    repeatable_read: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    serializable: "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
  }

  @doc "The SQL statement for `isolation`."
  def sql(isolation), do: Map.fetch!(@isolation_sql, isolation)
end
