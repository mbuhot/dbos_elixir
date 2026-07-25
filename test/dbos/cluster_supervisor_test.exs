defmodule Dbos.ClusterSupervisorTest do
  @moduledoc """
  Tests that `Dbos.Supervisor`'s `cluster:` option is genuinely opt-in for `Dbos.Cluster` and
  `Dbos.Cluster.NodeWatcher` (no `:pg` group unless explicitly enabled), while
  `Dbos.Cluster.OrphanSweep` starts by default, independent of `cluster:` entirely, since it needs
  only the system database.
  """

  use Dbos.Case, async: false

  alias Dbos.Cluster

  defp start_engine(extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  test "with cluster disabled (the default), no Dbos.Cluster process starts, but the orphan sweep does" do
    name = start_engine()

    assert Process.whereis(Cluster.process_name(name)) == nil
    assert Process.whereis(Dbos.Cluster.NodeWatcher.process_name(name)) == nil
    assert Process.whereis(Dbos.Cluster.OrphanSweep.process_name(name)) != nil
  end

  test "with cluster enabled, Dbos.Cluster and Dbos.Cluster.NodeWatcher also start" do
    name = start_engine(cluster: [enabled: true])

    assert Process.whereis(Cluster.process_name(name)) != nil
    assert Process.whereis(Dbos.Cluster.NodeWatcher.process_name(name)) != nil
    assert Process.whereis(Dbos.Cluster.OrphanSweep.process_name(name)) != nil
  end

  test "the orphan sweep can be disabled independently of cluster" do
    name = start_engine(orphan_sweep: [enabled: false])

    assert Process.whereis(Dbos.Cluster.OrphanSweep.process_name(name)) == nil
  end
end
