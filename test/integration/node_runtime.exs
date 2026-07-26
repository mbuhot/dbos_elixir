Code.require_file("support/workflows.ex", __DIR__)

{:ok, conn} =
  Postgrex.start_link(
    hostname: System.get_env("PGHOST", "postgres"),
    port: String.to_integer(System.get_env("PGPORT", "5432")),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    database: System.get_env("PGDATABASE", "dbos_integration"),
    pool_size: 20,
    queue_target: 5_000,
    queue_interval: 30_000,
    timeout: 30_000
  )

_recording_conn = Dbos.Integration.Workflows.start_conn!()

shared_workflows = [
  {"hard_kill_workflow/1", {Dbos.Integration.Workflows, :hard_kill_workflow, 1}},
  {"concurrent_start_workflow/1", {Dbos.Integration.Workflows, :concurrent_start_workflow, 1}},
  {"queue_competition_workflow/1", {Dbos.Integration.Workflows, :queue_competition_workflow, 1}}
]

exclusive_workflows =
  if System.get_env("DBOS_HOST_EXCLUSIVE_WORKFLOW") == "true" do
    [{"exclusive_workflow/1", {Dbos.Integration.Workflows, :exclusive_workflow, 1}}]
  else
    []
  end

{:ok, _pid} =
  Dbos.Supervisor.start_link(
    name: Dbos,
    db: {Dbos.DB.Postgrex, conn},
    executor_id: System.get_env("DBOS__VMID", "unknown"),
    migrations: :verify,
    workflows: shared_workflows ++ exclusive_workflows,
    queues: [Dbos.Queue.new("queue-competition", base_polling_interval_ms: 200)],
    lease: [ttl_ms: 5_000, renew_interval_ms: 1_000],
    lease_sweep: [interval_ms: 1_000]
  )

Process.register(self(), :dbos_integration_ready)

Process.sleep(:infinity)
