defmodule AgentInbox.Repo do
  @moduledoc "The Ecto repo backing this app's `Dbos` engine — no tables of its own besides `Dbos`'s."

  use Ecto.Repo, otp_app: :agent_inbox, adapter: Ecto.Adapters.Postgres
end
