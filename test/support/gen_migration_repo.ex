if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.GenMigrationTestRepo do
    @moduledoc "Ecto repo exercising `mix dbos.gen.migration`'s generated file end to end."
    use Ecto.Repo, otp_app: :dbos, adapter: Ecto.Adapters.Postgres
  end
end
