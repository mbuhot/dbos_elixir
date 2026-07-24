if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.TestRepo do
    @moduledoc "Ecto repo pointed at the `dbos_test` scratch database, used to exercise `Dbos.DB.Ecto`."
    use Ecto.Repo, otp_app: :dbos, adapter: Ecto.Adapters.Postgres
  end
end
