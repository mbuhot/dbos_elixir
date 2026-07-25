defmodule Dbos.SchedulerTest do
  use Dbos.Case, async: false

  alias Dbos.ScheduledWorkflows
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip,
        scheduler_poll_interval_ms: 50
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  defp success_count(config, prefix) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{prefix}%"]
      )

    count
  end

  test "a per-second cron schedule fires and its workflow runs" do
    engine = start_engine([ScheduledWorkflows])
    config = Dbos.config(engine)

    wait_until(fn -> success_count(config, "sched-sched_tick-") >= 1 end)
    assert success_count(config, "sched-sched_tick-") >= 1
  end

  test "the same schedule shared by two engines fires each occurrence exactly once" do
    workflows = [ScheduledWorkflows]
    engine_a = start_engine(workflows)
    _engine_b = start_engine(workflows)
    config = Dbos.config(engine_a)

    wait_until(fn -> success_count(config, "sched-sched_tick-") >= 1 end)

    {:ok, %{rows: rows}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        """
        SELECT workflow_uuid, count(*) FROM dbos.operation_outputs
        WHERE workflow_uuid LIKE 'sched-sched_tick-%'
        GROUP BY workflow_uuid HAVING count(*) > 1
        """,
        []
      )

    assert rows == []

    {:ok, %{rows: [[completed_count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(DISTINCT workflow_uuid) FROM dbos.workflow_status WHERE workflow_uuid LIKE 'sched-sched_tick-%' AND status = 'SUCCESS'",
        []
      )

    assert completed_count >= 1
  end

  test "automatic_backfill fires missed ticks recorded since last_fired_at once a new process starts" do
    engine = start_engine([ScheduledWorkflows])
    config = Dbos.config(engine)

    wait_until(fn -> success_count(config, "sched-sched_backfill_tick-") >= 1 end)

    past_last_fired = System.os_time(:millisecond) - 5_000

    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_schedules SET last_fired_at = $1 WHERE schedule_name = 'sched_backfill_tick'",
      [Integer.to_string(past_last_fired)]
    )

    fresh_engine = start_engine([ScheduledWorkflows])
    fresh_config = Dbos.config(fresh_engine)

    wait_until(fn -> success_count(fresh_config, "sched-sched_backfill_tick-") >= 4 end)
  end

  test "a schedule without automatic_backfill does not retroactively fire ticks missed before this process started" do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: Dbos.TestConn, application_version: "v1"}

    far_past = System.os_time(:millisecond) - 60_000

    SystemDb.register_schedule(config, %{
      schedule_name: "no_backfill_far_past",
      workflow_name: "sched_tick",
      schedule: "* * * * * *",
      context: Dbos.Serialization.encode(nil),
      automatic_backfill: false,
      queue_name: nil
    })

    SystemDb.update_schedule_last_fired_at(config, "no_backfill_far_past", far_past)

    start_supervised!(
      {Dbos.Supervisor,
       [
         name: name,
         db: {Dbos.DB.Postgrex, Dbos.TestConn},
         executor_id: "exec-#{System.unique_integer([:positive])}",
         workflows: [ScheduledWorkflows],
         migrations: :skip,
         scheduler_poll_interval_ms: 50
       ]},
      id: name
    )

    Dbos.Recovery.await_boot_recovery(name)
    Process.sleep(300)

    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        Dbos.TestConn,
        "SELECT count(*) FROM dbos.workflow_status WHERE workflow_uuid LIKE 'sched-no_backfill_far_past-%'",
        []
      )

    assert count < 10
  end
end
