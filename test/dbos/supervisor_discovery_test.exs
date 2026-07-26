defmodule Dbos.SupervisorDiscoveryTest do
  @moduledoc """
  Tests `Dbos.Supervisor`'s `otp_app:` workflow discovery: modules exporting
  `__dbos_workflows__/0` are found without being listed in `workflows:`, schedules come through
  with them, `workflows:` stays additive and deduplicates against discovery, and an engine given
  neither option raises at boot.
  """

  use Dbos.Case, async: false

  alias Dbos.Registry
  alias Dbos.SystemDb

  defp start_engine(extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    name
  end

  test "otp_app: alone discovers a workflow module's workflows and schedules" do
    engine = start_engine(otp_app: :dbos)

    assert {:ok, {Dbos.DiscoveryFixture, :discovered_add__dbos_workflow_body__, 2}} =
             Registry.lookup(engine, "discovery_fixture_add")

    config = Dbos.config(engine)
    schedule_names = config |> SystemDb.list_active_schedules() |> Enum.map(& &1.schedule_name)
    assert "discovery_fixture_tick" in schedule_names
  end

  test "workflows: is additive on top of otp_app: discovery and deduplicates" do
    engine = start_engine(otp_app: :dbos, workflows: [Dbos.DiscoveryFixture])

    assert {:ok, {Dbos.DiscoveryFixture, :discovered_add__dbos_workflow_body__, 2}} =
             Registry.lookup(engine, "discovery_fixture_add")
  end

  test "neither otp_app: nor workflows: raises a clear error naming both" do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    error =
      assert_raise ArgumentError, fn ->
        Dbos.Supervisor.start_link(
          name: name,
          db: {Dbos.DB.Postgrex, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          migrations: :skip
        )
      end

    assert error.message =~ "otp_app"
    assert error.message =~ "workflows"
  end
end
