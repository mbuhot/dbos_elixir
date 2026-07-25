defmodule CustomerServiceAgent.Application do
  @moduledoc """
  Boots the order store, the Postgres pool, and the Dbos engine that runs
  `CustomerServiceAgent.Support`.

  Passes `notifications_conn_opts` explicitly: a bare `Dbos.DB.Postgrex` pool (unlike an
  `Ecto.Repo`) carries no config `Dbos.Supervisor` can introspect on its own, and
  `approval_workflow/1`'s long wait needs a real dedicated `LISTEN` connection to wake promptly
  on approval rather than only at its multi-hour deadline.
  """

  use Application

  alias CustomerServiceAgent.OrderStore
  alias CustomerServiceAgent.Support

  @impl true
  def start(_type, _args) do
    children = [
      OrderStore,
      {Postgrex, postgrex_opts()},
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Postgrex, CustomerServiceAgent.Repo},
       workflows: [Support],
       migrations: :create_if_absent,
       notifications_conn_opts: notifications_conn_opts()}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: CustomerServiceAgent.Supervisor)
  end

  defp postgrex_opts do
    [{:name, CustomerServiceAgent.Repo}, {:pool_size, 10} | connection_opts()]
  end

  defp notifications_conn_opts, do: connection_opts()

  defp connection_opts do
    [
      hostname: System.get_env("PGHOST", "localhost"),
      port: System.get_env("PGPORT", "5432") |> String.to_integer(),
      username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
      password: System.get_env("PGPASSWORD"),
      database: System.get_env("PGDATABASE", "customer_service_agent_dev")
    ]
  end
end
