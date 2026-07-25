if Code.ensure_loaded?(Ecto) do
  defmodule Dbos.SandboxRepo do
    @moduledoc """
    An `Ecto.Repo` started with the `Ecto.Adapters.SQL.Sandbox` pool, used only by
    `Dbos.TestingModeTest`'s acceptance test proving `:manual` testing mode works end to end
    under a real sandbox ownership checkout.
    """

    use Ecto.Repo, otp_app: :dbos, adapter: Ecto.Adapters.Postgres
  end
end
