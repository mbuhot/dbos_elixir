defmodule Dbos.SystemDbError do
  @moduledoc """
  Raised when a system-database statement fails and `Dbos.DB.Retry` cannot recover it. Carries the
  statement and the underlying driver error, so the failure is attributable to a query rather than
  surfacing as a bare `MatchError`.
  """

  defexception [:sql, :params, :cause]

  @impl true
  def message(%__MODULE__{sql: sql, cause: cause}) do
    "dbos system database query failed: #{describe(cause)}\n\n#{String.trim(sql)}"
  end

  defp describe(%{__exception__: true} = cause), do: Exception.message(cause)
  defp describe(cause), do: inspect(cause)
end
