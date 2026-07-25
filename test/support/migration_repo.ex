if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.MigrationTestRepo do
    @moduledoc "Ecto repo pointed at a scratch database used to exercise `Dbos.Migration`."
    use Ecto.Repo, otp_app: :dbos, adapter: Ecto.Adapters.Postgres
  end
end
