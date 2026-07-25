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
  (default `3`), `:cluster` (see below).

  `:notifications` (default `:listen`) — `:listen` starts a dedicated `LISTEN` connection
  (`Dbos.Notifications`) for `recv`/`getEvent`/streams to wake on, falling back to `:poll` and
  logging a warning if one can't be established; `:poll` skips the listener entirely.
  `:notifications_conn_opts` overrides the Postgrex connection options used for that dedicated
  connection (otherwise derived from the Ecto repo's own config, when `:db` is `Dbos.DB.Ecto`).

  `:cluster` opts, all off by default (see `docs/clustering.md`): `:enabled` — joins a `:pg`
  group and reclaims a departed node's `PENDING` workflows on `:nodedown`; `:batch_size` (default
  `50`) — how many rows one reclaim pass claims; `:group` (default `Dbos.Cluster.Group`) — the
  shared `:pg` group name every engine that should see each other's roster must agree on (several
  engines in one deployment sharing the default is what makes the node-to-executor-ids mapping
  many-to-one); `:orphan_sweep` — a further `[:enabled, :interval_ms (default 300_000),
  :threshold_ms (default 300_000)]` for the periodic scan that catches executors no live node
  ever saw depart.

  `:admin_server` opts, off by default: `:enabled` — starts `Dbos.AdminServer`; `:port` (default
  `3001`). `:scheduler_poll_interval_ms` (default `30_000`) controls how often `Dbos.Scheduler`
  reconciles cron schedules from `workflow_schedules`.

  `:park_exit_threshold_ms` (default `60_000`, or `:infinity` to disable parking) — a workflow
  blocked in `sleep`/`recv_message`/`get_event` with more than this much wait remaining exits its
  process instead of staying resident, per `Dbos.Waits`. `:park_replay_ceiling` (default `500`)
  caps this by how many steps the workflow has already completed in this run, since a parked
  workflow rehydrates by replaying from the top.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, Dbos)
    Supervisor.start_link(__MODULE__, opts, name: process_name(name))
  end

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, Dbos)
    {db_module, conn} = Keyword.fetch!(opts, :db)
    workflow_entries = Keyword.get(opts, :workflows, [])
    workflows = Enum.flat_map(workflow_entries, &normalize_workflow_entry/1)
    schedules = Enum.flat_map(workflow_entries, &collect_schedules/1)

    cluster_opts = Keyword.get(opts, :cluster, [])
    orphan_sweep_opts = Keyword.get(cluster_opts, :orphan_sweep, [])

    config = %Config{
      name: name,
      db: db_module,
      conn: conn,
      schema: Keyword.get(opts, :schema, "dbos"),
      executor_id: resolve_executor_id(opts),
      application_version: resolve_application_version(opts, workflows),
      max_recovery_attempts: Keyword.get(opts, :max_recovery_attempts, 3),
      cluster_mode: resolve_cluster_mode(cluster_opts, orphan_sweep_opts),
      cluster_group: Keyword.get(cluster_opts, :group, Dbos.Cluster.Group),
      reclaim_batch_size: Keyword.get(cluster_opts, :batch_size, 50),
      orphan_sweep_interval_ms: Keyword.get(orphan_sweep_opts, :interval_ms, 300_000),
      orphan_sweep_threshold_ms: Keyword.get(orphan_sweep_opts, :threshold_ms, 300_000),
      notifications: Keyword.get(opts, :notifications, :listen),
      notifications_conn_opts: Keyword.get(opts, :notifications_conn_opts),
      scheduler_poll_interval_ms: Keyword.get(opts, :scheduler_poll_interval_ms, 30_000),
      park_exit_threshold_ms: Keyword.get(opts, :park_exit_threshold_ms, 60_000),
      park_replay_ceiling: Keyword.get(opts, :park_replay_ceiling, 500)
    }

    Dbos.put_config(config)
    run_migrations(Keyword.get(opts, :migrations, :verify), config)
    SystemDb.create_application_version(config, config.application_version)

    queues = Keyword.get(opts, :queues, [])

    children =
      [
        {Dbos.Registry, name: name, workflows: workflows},
        {Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
        {Registry, keys: :unique, name: Dbos.Notifications.recv_registry_name(name)},
        {Registry, keys: :duplicate, name: Dbos.Notifications.wait_registry_name(name)},
        {Dbos.Notifications, name: name},
        {Dbos.Waits.Table, name: name},
        {Dbos.Waits, name: name},
        {WorkflowSup, name: name},
        {Dbos.Recovery, name: name},
        {Dbos.Queue.Sup, name: name, queues: queues},
        {Dbos.Scheduler,
         name: name, schedules: schedules, poll_interval_ms: config.scheduler_poll_interval_ms}
      ] ++
        cluster_children(config) ++
        admin_server_children(name, Keyword.get(opts, :admin_server, []))

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp admin_server_children(_name, opts) when opts in [nil, false], do: []

  defp admin_server_children(name, opts) do
    if Keyword.get(opts, :enabled, false) do
      [{Dbos.AdminServer, name: name, port: Keyword.get(opts, :port, 3001)}]
    else
      []
    end
  end

  defp cluster_children(%Config{cluster_mode: :disabled}), do: []

  defp cluster_children(%Config{name: name} = config) do
    [
      {Dbos.Cluster, name: name},
      {Dbos.Cluster.NodeWatcher, name: name}
    ] ++ orphan_sweep_children(config)
  end

  defp orphan_sweep_children(%Config{cluster_mode: :cluster_and_orphan_sweep, name: name}),
    do: [{Dbos.Cluster.OrphanSweep, name: name}]

  defp orphan_sweep_children(%Config{}), do: []

  defp resolve_cluster_mode(cluster_opts, orphan_sweep_opts) do
    cond do
      not Keyword.get(cluster_opts, :enabled, false) -> :disabled
      Keyword.get(orphan_sweep_opts, :enabled, false) -> :cluster_and_orphan_sweep
      true -> :cluster_only
    end
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

  defp normalize_workflow_entry({_name, {_module, _fun, _arity}} = entry), do: [entry]

  defp normalize_workflow_entry(module) when is_atom(module) do
    Enum.map(module.__dbos_workflows__(), fn {name, mfa, _ast} -> {name, mfa} end)
  end

  defp collect_schedules(module) when is_atom(module) do
    if function_exported?(module, :__dbos_schedules__, 0),
      do: module.__dbos_schedules__(),
      else: []
  end

  defp collect_schedules(_entry), do: []
end
