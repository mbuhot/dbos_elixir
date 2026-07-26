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
  `:executor_id`, `:application_version`, `:schema` (default `"dbos"`), `:otp_app` (discovers
  workflow modules automatically — see below), `:workflows` (a list of modules or explicit
  `{name, {module, function, arity}}` entries, additive with `:otp_app`), `:queues` (a list of
  `Dbos.Queue`, default `[]`), `:migrations` (`:verify` (default), `:create_if_absent`, or
  `:skip`), `:max_recovery_attempts` (default `3`), `:testing` (see below).

  `:otp_app` — resolves to every module the named OTP application has compiled
  (`:application.get_key/2`), keeping the ones that export `__dbos_workflows__/0` (every module
  with at least one `defworkflow`). This is the primary way to declare workflows: a module added
  to the app is discovered automatically, so it can never sit un-registered and stuck `PENDING`
  because a `workflows:` list wasn't updated. `:workflows` is additive on top of discovery —
  duplicates between the two are harmless — and is the way to bring in a workflow module that
  lives in a dependency rather than this OTP application. At least one of `:otp_app` or
  `:workflows` is required; an engine given neither raises, since an engine with no workflows is
  always a mistake.

  `:testing` (default `nil`, meaning off) — `:inline` runs `Dbos.start/3` and `Dbos.enqueue/3`
  synchronously, in the calling process, and returns a handle to an already-finished workflow;
  `:manual` runs `Dbos.start/3` the same way but leaves an `Dbos.enqueue/3`'d row untouched until
  `Dbos.Testing.drain_queue/2` or `Dbos.Testing.drain_all/1` claims it. Either mode starts none
  of `Dbos.Notifications`, the wait parking table, the executor lease, the queue runners,
  `Dbos.Scheduler`, the boot recovery scan, the lease sweep, or `Dbos.AdminServer` — the point is
  removing every
  process that could touch the database outside the caller's own connection, which is what makes
  these modes compatible with `Ecto.Adapters.SQL.Sandbox`. See `guides/tutorials/testing.md`.

  `:notifications` (default `:listen`) — `:listen` starts a dedicated `LISTEN` connection
  (`Dbos.Notifications`) for `recv`/`getEvent`/streams to wake on, falling back to `:poll` and
  logging a warning if one can't be established; `:poll` skips the listener entirely.
  `:notifications_conn_opts` overrides the Postgrex connection options used for that dedicated
  connection (otherwise derived from the Ecto repo's own config, when `:db` is `Dbos.DB.Ecto`).

  `:lease` opts (see `docs/executor-leases.md`): `:ttl_ms` (default `60_000`) — how long this
  executor's lease stays valid without a renewal; `:renew_interval_ms` (default `10_000`) — how
  often it renews. The lease is written at boot, before recovery runs, and is the sole authority
  the lease sweep consults to decide an executor is dead.

  `:lease_sweep` opts, on by default: `:enabled` (default `true`) — periodically reclaims
  `PENDING` rows whose executor's lease has expired or is entirely absent; `:interval_ms` (default
  `30_000`) — how often it scans; `:batch_size` (default `50`) — how many rows one reclaim pass
  claims.

  `:admin_server` opts, off by default: `:enabled` — starts `Dbos.AdminServer`; `:port` (default
  `3001`). `:scheduler_poll_interval_ms` (default `30_000`) controls how often `Dbos.Scheduler`
  reconciles cron schedules from `workflow_schedules`.

  `:park_exit_threshold_ms` (default `60_000`, or `:infinity` to disable parking) — a workflow
  blocked in `sleep`/`recv_message`/`get_event` with more than this much wait remaining exits its
  process instead of staying resident. `:park_replay_ceiling` (default `500`)
  caps this by how many steps the workflow has already completed in this run, since a parked
  workflow rehydrates by replaying from the top.
  """
  def start_link(opts) do
    name = Keyword.get(opts, :name, Dbos)
    prepare(opts)
    Supervisor.start_link(__MODULE__, opts, name: process_name(name))
  end

  # Everything that touches the database at startup — verifying the schema, recording the
  # application version, registering queues — runs here, in the process calling start_link,
  # rather than in the supervisor. A caller holding an Ecto.Adapters.SQL.Sandbox connection can
  # then start an engine: ownership does not reach a supervisor spawned from that process.
  defp prepare(opts) do
    config = build_config(opts)

    Dbos.put_config(config)
    run_migrations(Keyword.get(opts, :migrations, :verify), config)
    SystemDb.create_application_version(config, config.application_version)

    if config.testing in [:inline, :manual] do
      Enum.each(config.queues, &SystemDb.register_queue(config, &1))
    end

    config
  end

  @impl true
  def init(opts) do
    name = Keyword.get(opts, :name, Dbos)
    workflow_entries = resolve_workflow_entries!(opts)
    workflows = Enum.flat_map(workflow_entries, &normalize_workflow_entry/1)
    schedules = Enum.flat_map(workflow_entries, &collect_schedules/1)
    config = build_config(opts)
    testing = config.testing

    children =
      if testing in [:inline, :manual] do
        testing_children(name, workflows)
      else
        full_children(name, workflows, schedules, config, Keyword.get(opts, :queues, []), opts)
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp build_config(opts) do
    name = Keyword.get(opts, :name, Dbos)
    {db_module, conn} = Keyword.fetch!(opts, :db)
    workflow_entries = resolve_workflow_entries!(opts)
    workflows = Enum.flat_map(workflow_entries, &normalize_workflow_entry/1)

    lease_sweep_opts = Keyword.get(opts, :lease_sweep, [])
    lease_opts = Keyword.get(opts, :lease, [])
    testing = fetch_testing_mode!(opts)
    queues = [internal_queue() | Keyword.get(opts, :queues, [])]

    %Config{
      name: name,
      db: db_module,
      conn: conn,
      schema: Keyword.get(opts, :schema, "dbos"),
      executor_id: resolve_executor_id(opts),
      application_version: resolve_application_version(opts, workflows),
      max_recovery_attempts: Keyword.get(opts, :max_recovery_attempts, 3),
      reclaim_batch_size: Keyword.get(lease_sweep_opts, :batch_size, 50),
      lease_sweep_enabled: Keyword.get(lease_sweep_opts, :enabled, true),
      lease_sweep_interval_ms: Keyword.get(lease_sweep_opts, :interval_ms, 30_000),
      lease_ttl_ms: Keyword.get(lease_opts, :ttl_ms, 60_000),
      lease_renew_interval_ms: Keyword.get(lease_opts, :renew_interval_ms, 10_000),
      notifications: Keyword.get(opts, :notifications, :listen),
      notifications_conn_opts: Keyword.get(opts, :notifications_conn_opts),
      scheduler_poll_interval_ms: Keyword.get(opts, :scheduler_poll_interval_ms, 30_000),
      park_exit_threshold_ms: Keyword.get(opts, :park_exit_threshold_ms, 60_000),
      park_replay_ceiling: Keyword.get(opts, :park_replay_ceiling, 500),
      testing: testing,
      queues: queues
    }
  end

  defp testing_children(name, workflows) do
    [
      {Dbos.Registry, name: name, workflows: workflows},
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)}
    ]
  end

  defp full_children(name, workflows, schedules, config, declared_queues, opts) do
    [
      {Dbos.Registry, name: name, workflows: workflows},
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
      {Registry, keys: :unique, name: Dbos.Notifications.recv_registry_name(name)},
      {Registry, keys: :duplicate, name: Dbos.Notifications.wait_registry_name(name)},
      {Dbos.Notifications, name: name},
      {Dbos.Waits.Table, name: name},
      {Dbos.Waits, name: name},
      {WorkflowSup, name: name},
      {Dbos.Lease, name: name},
      {Dbos.Recovery, name: name},
      {Dbos.Queue.Sup, name: name, queues: declared_queues},
      {Dbos.Scheduler,
       name: name, schedules: schedules, poll_interval_ms: config.scheduler_poll_interval_ms}
    ] ++
      lease_sweep_children(config) ++
      admin_server_children(name, Keyword.get(opts, :admin_server, []))
  end

  defp internal_queue, do: %Dbos.Queue{name: Dbos.Queue.internal_queue_name()}

  defp fetch_testing_mode!(opts) do
    case Keyword.get(opts, :testing) do
      mode when mode in [nil, :inline, :manual] ->
        mode

      other ->
        raise ArgumentError,
              "Dbos.Supervisor's :testing option must be nil, :inline, or :manual, got: " <>
                inspect(other)
    end
  end

  defp resolve_workflow_entries!(opts) do
    otp_app = Keyword.get(opts, :otp_app)

    unless otp_app || Keyword.has_key?(opts, :workflows) do
      raise ArgumentError,
            "Dbos.Supervisor requires :otp_app, :workflows, or both — pass :otp_app to " <>
              "discover every workflow module in that OTP application automatically, or " <>
              ":workflows to list modules (or explicit {name, {module, function, arity}} " <>
              "entries) by hand. An engine given neither has no workflows to run, which is " <>
              "always a mistake."
    end

    Enum.uniq(discover_workflow_modules(otp_app) ++ Keyword.get(opts, :workflows, []))
  end

  defp discover_workflow_modules(nil), do: []

  defp discover_workflow_modules(otp_app) do
    case :application.get_key(otp_app, :modules) do
      {:ok, modules} -> Enum.filter(modules, &workflow_module?/1)
      :undefined -> []
    end
  end

  defp workflow_module?(module),
    do: Code.ensure_loaded?(module) and function_exported?(module, :__dbos_workflows__, 0)

  defp admin_server_children(_name, opts) when opts in [nil, false], do: []

  defp admin_server_children(name, opts) do
    if Keyword.get(opts, :enabled, false) do
      [{Dbos.AdminServer, name: name, port: Keyword.get(opts, :port, 3001)}]
    else
      []
    end
  end

  defp lease_sweep_children(%Config{lease_sweep_enabled: false}), do: []

  defp lease_sweep_children(%Config{lease_sweep_enabled: true, name: name}),
    do: [{Dbos.LeaseSweep, name: name}]

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
