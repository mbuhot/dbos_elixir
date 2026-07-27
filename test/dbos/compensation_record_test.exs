defmodule Dbos.CompensationRecordTest do
  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.Serialization

  defmodule Sagas do
    @moduledoc false
    use Dbos

    defworkflow reserve(product_id, quantity), name: "saga.reserve" do
      reserve_stock(product_id, quantity)
    end

    defworkflow charge(order_id), name: "saga.charge" do
      charge_card(order_id)
    end

    defworkflow plain(product_id), name: "saga.plain" do
      look_up(product_id)
    end

    defworkflow folded(product_id), name: "saga.folded" do
      outer(product_id)
    end

    defstep reserve_stock(product_id, quantity),
      compensate: &release_stock(product_id, quantity, &1) do
      "reservation-#{product_id}-#{quantity}"
    end

    defstep(release_stock(_product_id, _quantity, _reservation_id), do: :released)

    defstep charge_card(order_id), compensate: &refund/1 do
      "charge-#{order_id}"
    end

    defstep(refund(_charge_id), do: :refunded)

    defstep(look_up(product_id), do: product_id)

    defstep outer(product_id) do
      reserve_stock(product_id, 1)
    end
  end

  setup do
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
    :ok
  end

  defp compensations(workflow_id) do
    %{rows: rows} =
      Postgrex.query!(
        Dbos.TestConn,
        """
        SELECT function_name, ex_compensation FROM "dbos".operation_outputs
        WHERE workflow_uuid = $1 ORDER BY function_id
        """,
        [workflow_id]
      )

    Enum.map(rows, fn
      [name, nil] -> {name, nil}
      [name, encoded] -> {name, Serialization.decode(encoded)}
    end)
  end

  test "a compensable step records its undo with the checkpointed value substituted" do
    {:ok, handle} = Sagas.reserve("widget", 3)
    assert {:ok, "reservation-widget-3"} = Dbos.await(handle)

    assert [{"reserve_stock/2", record}] = compensations(handle.workflow_id)

    assert record == %{
             undo: {Sagas, :release_stock, ["widget", 3, "reservation-widget-3"]}
           }
  end

  test "the &fun/1 shorthand records the checkpointed value as the only argument" do
    {:ok, handle} = Sagas.charge("order-9")
    assert {:ok, "charge-order-9"} = Dbos.await(handle)

    assert [{"charge_card/1", record}] = compensations(handle.workflow_id)
    assert record == %{undo: {Sagas, :refund, ["charge-order-9"]}}
  end

  test "a step declaring no compensation records none" do
    {:ok, handle} = Sagas.plain("widget")
    assert {:ok, "widget"} = Dbos.await(handle)

    assert [{"look_up/1", nil}] = compensations(handle.workflow_id)
  end

  test "a compensable step folded into an enclosing step records nothing of its own" do
    {:ok, handle} = Sagas.folded("widget")
    assert {:ok, "reservation-widget-1"} = Dbos.await(handle)

    assert [{"outer/1", nil}] = compensations(handle.workflow_id)
  end

  describe "the compile-time contract" do
    defp compile_error(body) do
      module = Module.concat(__MODULE__, :"Fixture#{System.unique_integer([:positive])}")

      source = """
      defmodule #{inspect(module)} do
        use Dbos
      #{body}
      end
      """

      assert_raise CompileError, fn -> Code.compile_string(source, "test/fixture.ex") end
    end

    test "rejects a compensate: that is not a capture" do
      error =
        compile_error("""
          defstep pay(amount), compensate: :refund do
            amount
          end
        """)

      assert error.description =~ "must be a capture of a step in this module"
    end

    test "rejects a compensate: naming something that is not a step in this module" do
      error =
        compile_error("""
          defstep pay(amount), compensate: &refund(amount, &1) do
            amount
          end
        """)

      assert error.description =~ "names refund/2"
      assert error.description =~ "not a defstep or deftransaction"
    end

    test "rejects a compensate: whose target arity does not match the bound arguments" do
      error =
        compile_error("""
          defstep pay(amount), compensate: &refund(amount, &1) do
            amount
          end

          defstep refund(_amount), do: :refunded
        """)

      assert error.description =~ "names refund/2"
    end

    test "rejects &1 buried inside a larger argument" do
      error =
        compile_error("""
          defstep pay(amount), compensate: &refund(Map.get(&1, :id)) do
            amount
          end

          defstep refund(_id), do: :refunded
        """)

      assert error.description =~ "only use &1 as a whole argument"
    end

    test "rejects a compensate: with no &1 at all" do
      error =
        compile_error("""
          defstep pay(amount), compensate: &refund(amount) do
            amount
          end

          defstep refund(_amount), do: :refunded
        """)

      assert error.description =~ "must use &1 exactly once"
    end

    test "accepts a deftransaction as the compensating target" do
      module = Module.concat(__MODULE__, :"TxFixture#{System.unique_integer([:positive])}")

      source = """
      defmodule #{inspect(module)} do
        use Dbos, repo: Dbos.TestRepo

        defstep pay(amount), compensate: &refund(amount, &1) do
          amount
        end

        deftransaction refund(_amount, _receipt) do
          :refunded
        end
      end
      """

      assert [_ | _] = Code.compile_string(source, "test/fixture.ex")
    end
  end
end
