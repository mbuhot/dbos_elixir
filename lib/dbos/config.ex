defmodule Dbos.Config do
  @moduledoc """
  Configuration a `Dbos.SystemDb` call needs to reach the system database: which adapter and
  connection to use, which schema the `dbos` tables live under, and this executor's identity.
  """

  @enforce_keys [:db, :conn]
  defstruct name: Dbos,
            db: nil,
            conn: nil,
            schema: "dbos",
            executor_id: nil,
            application_version: nil,
            max_recovery_attempts: 3

  @type t :: %__MODULE__{
          name: atom,
          db: module,
          conn: term,
          schema: String.t(),
          executor_id: String.t() | nil,
          application_version: String.t() | nil,
          max_recovery_attempts: non_neg_integer
        }
end
