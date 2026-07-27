defmodule Dbos.CompensationPrimitivesTest do
  use Dbos.Case, async: false

  alias Dbos.Compensation
  alias Dbos.Recovery

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))
  end

  defmodule Sagas do
    @moduledoc false
    use Dbos

    def retract(value) do
      Ledger.record({:retracted, value})
      :retracted
    end

    def restore(key, value) do
      Ledger.record({:restored, key, value})
      :restored
    end

    defworkflow announce(_id), name: "primitive.announce" do
      Dbos.set_event("state", "shipped", compensate: &Sagas.retract/1)
      :announced
    end

    defworkflow announce_with_args(id), name: "primitive.announce_with_args" do
      Dbos.set_event("state", "shipped",
        compensate: {Sagas, :restore, ["state", :__checkpoint__]}
      )

      id
    end

    defworkflow notify(target), name: "primitive.notify" do
      Dbos.send_message(target, "orders", "shipped", compensate: &Sagas.retract/1)
      :notified
    end

    defworkflow publish(id), name: "primitive.publish" do
      Dbos.write_stream("events", "shipped", compensate: &Sagas.retract/1)
      id
    end

    defworkflow uncompensated(id), name: "primitive.uncompensated" do
      Dbos.set_event("state", "shipped")
      id
    end
  end

  setup do
    start_supervised!(%{id: Ledger, start: {Ledger, :start, []}})

    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, Dbos.TestConn},
       executor_id: "exec-#{System.unique_integer([:positive])}",
       workflows: [Sagas],
       lease_sweep: [enabled: false],
       migrations: :skip},
      id: name
    )

    Recovery.await_boot_recovery(name)
    Dbos.put_engine(name)

    {:ok, engine: name}
  end

  defp unwind!(workflow_id) do
    {:ok, handle} = Dbos.unwind(workflow_id)
    Dbos.await(handle, timeout_ms: 10_000)
  end

  test "set_event's compensation is recorded and run with the value it wrote" do
    {:ok, handle} = Sagas.announce("o1")
    assert {:ok, :announced} = Dbos.await(handle)

    assert {:ok, 1} = unwind!(handle.workflow_id)
    assert Ledger.entries() == [{:retracted, "shipped"}]
  end

  test "an explicit MFA puts the checkpointed value in the marked slot" do
    {:ok, handle} = Sagas.announce_with_args("o2")
    assert {:ok, "o2"} = Dbos.await(handle)

    assert {:ok, 1} = unwind!(handle.workflow_id)
    assert Ledger.entries() == [{:restored, "state", "shipped"}]
  end

  test "send_message's compensation is recorded" do
    {:ok, target} = Sagas.uncompensated("o3")
    assert {:ok, "o3"} = Dbos.await(target)

    {:ok, handle} = Sagas.notify(target.workflow_id)
    assert {:ok, :notified} = Dbos.await(handle)

    assert {:ok, 1} = unwind!(handle.workflow_id)
    assert Ledger.entries() == [{:retracted, :ok}]
  end

  test "write_stream's compensation is recorded with the item it appended" do
    {:ok, handle} = Sagas.publish("o4")
    assert {:ok, "o4"} = Dbos.await(handle)

    assert {:ok, 1} = unwind!(handle.workflow_id)
    assert Ledger.entries() == [{:retracted, "shipped"}]
  end

  test "a primitive declaring no compensation records none" do
    {:ok, handle} = Sagas.uncompensated("o5")
    assert {:ok, "o5"} = Dbos.await(handle)

    assert {:ok, 0} = unwind!(handle.workflow_id)
    assert Ledger.entries() == []
  end

  describe "record!/1" do
    test "accepts a remote capture, undoing with the checkpointed value" do
      assert Compensation.record!(&Sagas.retract/1) == %{
               undo: {Sagas, :retract, [:__checkpoint__]}
             }
    end

    test "accepts an MFA with the checkpoint marker anywhere in the arguments" do
      assert Compensation.record!({Sagas, :restore, ["state", :__checkpoint__]}) == %{
               undo: {Sagas, :restore, ["state", :__checkpoint__]}
             }
    end

    test "refuses an anonymous function, which no unwind could read back" do
      error = assert_raise ArgumentError, fn -> Compensation.record!(fn value -> value end) end
      assert Exception.message(error) =~ "capture of a named function"
    end

    test "refuses a target that is not exported" do
      error =
        assert_raise ArgumentError, fn -> Compensation.record!({Sagas, :no_such_undo, [1]}) end

      assert Exception.message(error) =~ "is not exported"
    end

    test "refuses anything that is neither a capture nor an MFA" do
      error = assert_raise ArgumentError, fn -> Compensation.record!(:retract) end
      assert Exception.message(error) =~ "must be &Module.fun/1 or {module, function, args}"
    end
  end
end
