defmodule Dbos.RuntimeTest do
  use Dbos.Case, async: false

  alias Dbos.Runtime
  alias Dbos.SystemDb

  defmodule CardDeclinedError do
    defexception [:amount]

    @impl true
    def message(%__MODULE__{amount: amount}), do: "card declined for #{amount}"
  end

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  defp start_workflow(config, workflow_id) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "TestWorkflow"
    })

    workflow_id
  end

  test "a sequence of run_step calls allocates function ids in execution order", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-sequence")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      assert Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end) == %{
               reserved: true
             }

      assert Runtime.run_step("charge_card/2", [], fn -> %{charged: true} end) == %{
               charged: true
             }

      assert Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end) == %{
               shipped: true
             }
    end)

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, & &1.function_id) == [0, 1, 2]

    assert Enum.map(steps, & &1.function_name) == [
             "reserve_stock/1",
             "charge_card/2",
             "ship_order/1"
           ]

    assert Enum.map(steps, & &1.output) == [
             %{reserved: true},
             %{charged: true},
             %{shipped: true}
           ]
  end

  test "replay returns every checkpointed value without running any step body", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-replay")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      Runtime.run_step("charge_card/2", [], fn -> %{charged: true} end)
      Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
    end)

    results =
      Runtime.with_context([config: config, workflow_id: workflow_id, replay: true], fn ->
        [
          Runtime.run_step("reserve_stock/1", [], fn -> raise "must not run" end),
          Runtime.run_step("charge_card/2", [], fn -> raise "must not run" end),
          Runtime.run_step("ship_order/1", [], fn -> raise "must not run" end)
        ]
      end)

    assert results == [%{reserved: true}, %{charged: true}, %{shipped: true}]
  end

  test "a mismatched recorded function_name raises UnexpectedStepError naming both", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-mismatch")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
    end)

    error =
      assert_raise Dbos.UnexpectedStepError, fn ->
        Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
          Runtime.run_step("charge_card/2", [], fn -> %{charged: true} end)
        end)
      end

    assert error.expected == "charge_card/2"
    assert error.recorded == "reserve_stock/1"
  end

  test "a cancelled workflow raises WorkflowCancelledError on the next run_step", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-cancelled")

    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = $1",
      [workflow_id]
    )

    assert_raise Dbos.WorkflowCancelledError, fn ->
      Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
        Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      end)
    end
  end

  test "a run_step against a non-existent workflow raises NonExistentWorkflowError", %{
    config: config
  } do
    assert_raise Dbos.NonExistentWorkflowError, fn ->
      Runtime.with_context([config: config, workflow_id: "does-not-exist"], fn ->
        Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      end)
    end
  end

  test "a step that raises a custom exception records the failure, replayed with the same struct",
       %{config: config} do
    workflow_id = start_workflow(config, "wf-custom-exception")

    assert_raise CardDeclinedError, fn ->
      Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
        Runtime.run_step("charge_card/1", [], fn ->
          raise CardDeclinedError, amount: 4999
        end)
      end)
    end

    try do
      Runtime.with_context([config: config, workflow_id: workflow_id, replay: true], fn ->
        Runtime.run_step("charge_card/1", [], fn -> raise "must not run" end)
      end)

      flunk("expected the replay to reraise CardDeclinedError")
    rescue
      exception ->
        assert exception == %CardDeclinedError{amount: 4999}
    end
  end

  test "a step that fails transiently and succeeds on retry N records exactly one success row",
       %{config: config} do
    workflow_id = start_workflow(config, "wf-retry-success")
    counter = :counters.new(1, [])

    result =
      Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
        Runtime.run_step(
          "flaky_step/0",
          [max_retries: 3, base_interval_ms: 1],
          fn ->
            attempt = :counters.get(counter, 1)
            :counters.add(counter, 1, 1)

            if attempt < 2 do
              raise "transient failure"
            else
              %{ok: true}
            end
          end
        )
      end)

    assert result == %{ok: true}

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0]
    assert Enum.map(steps, & &1.output) == [%{ok: true}]
  end

  test "a step that exhausts its retries records the wrapped MaxStepRetriesExceededError", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-retry-exhausted")

    assert_raise Dbos.MaxStepRetriesExceededError, fn ->
      Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
        Runtime.run_step(
          "always_fails/0",
          [max_retries: 2, base_interval_ms: 1],
          fn -> raise "always fails" end
        )
      end)
    end

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0]

    assert [
             %Dbos.StepInfo{
               output: nil,
               error: %{kind: :error, value: %Dbos.MaxStepRetriesExceededError{}}
             }
           ] =
             steps
  end

  test "run_step outside any workflow context returns the body's value and writes no row", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-passthrough")

    result = Runtime.run_step("adhoc/0", [], fn -> %{ran: true} end)

    assert result == %{ran: true}
    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
    assert steps == []
  end

  test "in_workflow?/0 reflects whether a context is active" do
    refute Runtime.in_workflow?()
  end
end
