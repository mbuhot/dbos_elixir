defmodule Dbos.Supervisor do
  @moduledoc """
  The host application's entry point: resolves a `Dbos.Config`, verifies (or creates) the
  system database schema, then starts this engine's registry, workflow supervisor, and
  recovery pass. Multiple engines can run in one BEAM, each under its own `name`.
  """

  use Supervisor

  alias Dbos.Config
  alias Dbos.Migrator
  alias Dbos.SystemDb
  alias Dbos.Version
  alias Dbos.WorkflowSup

  @doc """
  Starts the engine. `opts`: `:name` (default `Dbos`), `:db` (`{adapter_module, conn}`),
  `:executor_id`, `:application_version`, `:schema` (default `"dbos"`), `:workflows` (a list of
  `{name, {module, function, arity}}`), `:queues` (a list of `Dbos.Queue`, default `[]`),
  `:migrations` (`:verify` (default), `:create_if_absent`, or `:skip`), `:max_recovery_attempts`
  (default `3`).
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, Dbos)
    Supervisor.start_link(__MODULE__, opts, name: process_name(name))
  end

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, Dbos)
    {db_module, conn} = Keyword.fetch!(opts, :db)
    workflows = Keyword.get(opts, :workflows, [])

    config = %Config{
      name: name,
      db: db_module,
      conn: conn,
      schema: Keyword.get(opts, :schema, "dbos"),
      executor_id: resolve_executor_id(opts),
      application_version: resolve_application_version(opts, workflows),
      max_recovery_attempts: Keyword.get(opts, :max_recovery_attempts, 3)
    }

    Dbos.put_config(config)
    run_migrations(Keyword.get(opts, :migrations, :verify), config)
    SystemDb.create_application_version(config, config.application_version)

    queues = Keyword.get(opts, :queues, [])

    children = [
      {Dbos.Registry, name: name, workflows: workflows},
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
      {WorkflowSup, name: name},
      {Dbos.Recovery, name: name},
      {Dbos.Queue.Sup, name: name, queues: queues}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp resolve_executor_id(opts) do
    Keyword.get(opts, :executor_id) || System.get_env("DBOS__VMID") || node_executor_id()
  end

  defp node_executor_id, do: to_string(node())

  defp resolve_application_version(opts, workflows) do
    Keyword.get(opts, :application_version) || System.get_env("DBOS__APPVERSION") ||
      Version.compute(Enum.map(workflows, fn {_name, {module, _fun, _arity}} -> module end))
  end

  defp run_migrations(:verify, config), do: Migrator.verify!(config)
  defp run_migrations(:skip, _config), do: :ok

  defp run_migrations(:create_if_absent, config) do
    Migrator.verify!(config)
  rescue
    _error ->
      Migrator.create!(config)
      Migrator.verify!(config)
  end

  defp process_name(name), do: Module.concat(name, Supervisor)
end
