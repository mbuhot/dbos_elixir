defmodule Outbox.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Outbox.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, Outbox.Repo},
       workflows: [Outbox.Workflows],
       migrations: :verify,
       scheduler_poll_interval_ms: 1_000}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Outbox.Supervisor)
  end
end
