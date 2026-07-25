Code.require_file("support/workflows.ex", __DIR__)

{:ok, conn} =
  Postgrex.start_link(
    hostname: System.get_env("PGHOST", "postgres"),
    port: String.to_integer(System.get_env("PGPORT", "5432")),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    database: System.get_env("PGDATABASE", "dbos_integration")
  )

{:ok, _pid} =
  Dbos.Supervisor.start_link(
    name: Dbos,
    db: {Dbos.DB.Postgrex, conn},
    executor_id: System.get_env("DBOS__VMID", "unknown"),
    migrations: :verify,
    workflows: [
      {"hard_kill_workflow/1", {Dbos.Integration.Workflows, :hard_kill_workflow, 1}},
      {"concurrent_start_workflow/1", {Dbos.Integration.Workflows, :concurrent_start_workflow, 1}}
    ]
  )

Process.sleep(:infinity)
