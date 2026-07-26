defmodule AgentInbox.Cli do
  @moduledoc """
  Shared setup for this app's Mix tasks. Listing pending approvals and sending a decision only
  read/write a couple of rows — neither needs a running `Dbos.Supervisor`, so these tasks start a
  one-off `AgentInbox.Repo` and build a `Dbos.Config` directly, rather than starting the full
  engine (its registry, workflow supervisor, recovery pass) just to run a CLI command.
  """

  alias Dbos.Config

  @doc "Builds a `Dbos.Config` against a fresh one-off `AgentInbox.Repo`, for one Mix task invocation."
  def config do
    {:ok, _pid} = AgentInbox.Repo.start_link()

    config = %Config{
      name: __MODULE__,
      db: Dbos.DB.Ecto,
      conn: AgentInbox.Repo,
      executor_id: "cli"
    }

    Dbos.put_config(config)
    config
  end
end
