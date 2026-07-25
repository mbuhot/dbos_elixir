defmodule AgentInbox.Db do
  @moduledoc "The bare Postgrex pool backing this app's `Dbos` engine — no Ecto needed, since this sample keeps no tables of its own besides `Dbos`'s."

  @pool_name __MODULE__.Pool

  @doc "This pool's process name, the `conn` half of `Dbos`'s `{Dbos.DB.Postgrex, conn}` pair."
  def pool_name, do: @pool_name

  @doc "The pool's child spec, pointed at `AGENT_INBOX_DATABASE` (default `agent_inbox_dev`)."
  def child_spec(_opts) do
    Postgrex.child_spec(name: @pool_name, database: database_name())
  end

  @doc "The configured database name."
  def database_name, do: System.get_env("AGENT_INBOX_DATABASE") || "agent_inbox_dev"
end
