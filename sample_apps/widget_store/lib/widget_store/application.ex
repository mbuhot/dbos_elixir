defmodule WidgetStore.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      WidgetStore.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, WidgetStore.Repo},
       workflows: [WidgetStore.Checkout],
       migrations: :verify}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: WidgetStore.Supervisor)
  end
end
