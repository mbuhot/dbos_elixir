defmodule Dbos.SystemDbTest do
  use Dbos.Case, async: false

  alias Dbos.SystemDb

  defmodule Money do
    defstruct [:currency, :at]
  end

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  test "enqueued inputs round-trip with atoms and structs intact", %{config: config} do
    inputs = ["ord_1", 4999, %Money{currency: :aud, at: ~U[2026-07-25 00:00:00Z]}]

    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "Checkout.process_order",
        queue_name: "default",
        inputs: inputs
      })

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)

    assert status.inputs == inputs
    assert status.status == :enqueued
    assert status.name == "Checkout.process_order"
    assert status.queue_name == "default"
  end

  test "get_workflow_status returns not_found for an unknown workflow id", %{config: config} do
    assert SystemDb.get_workflow_status(config, "does-not-exist") == {:error, :not_found}
  end

  test "enqueueing the same workflow id twice does not duplicate and does not error", %{
    config: config
  } do
    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{
        workflow_id: "wf-dup",
        name: "Checkout.process_order",
        queue_name: "default",
        inputs: [1]
      })

    {:ok, ^workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{
        workflow_id: "wf-dup",
        name: "Checkout.process_order",
        queue_name: "default",
        inputs: [1]
      })

    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE workflow_uuid = $1",
        [
          "wf-dup"
        ]
      )

    assert count == 1
  end

  test "list_workflows filters by status, name, and queue_name, ordered by created_at", %{
    config: config
  } do
    {:ok, wf_a} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "Checkout.process_order",
        queue_name: "orders",
        inputs: [1]
      })

    Process.sleep(2)

    {:ok, wf_b} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "Checkout.process_order",
        queue_name: "orders",
        inputs: [2]
      })

    {:ok, _wf_c} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "Refund.process",
        queue_name: "refunds",
        inputs: [3]
      })

    {:ok, results} =
      SystemDb.list_workflows(config,
        status: :enqueued,
        name: "Checkout.process_order",
        queue_name: "orders",
        sort: :asc
      )

    assert Enum.map(results, & &1.workflow_uuid) == [wf_a, wf_b]
  end

  test "list_workflows respects limit and offset", %{config: config} do
    {:ok, wf_a} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

    Process.sleep(2)

    {:ok, wf_b} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [2]})

    Process.sleep(2)

    {:ok, _wf_c} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [3]})

    {:ok, results} = SystemDb.list_workflows(config, sort: :asc, limit: 2)
    assert Enum.map(results, & &1.workflow_uuid) == [wf_a, wf_b]

    {:ok, results} = SystemDb.list_workflows(config, sort: :asc, limit: 1, offset: 1)
    assert Enum.map(results, & &1.workflow_uuid) == [wf_b]
  end

  test "get_workflow_steps returns steps ordered by function_id with decoded outputs", %{
    config: config
  } do
    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

    insert_operation_output(config, workflow_id, 1, "charge_card/2", %{charged: true})
    insert_operation_output(config, workflow_id, 0, "reserve_stock/1", %{reserved: true})

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, & &1.function_id) == [0, 1]
    assert Enum.map(steps, & &1.function_name) == ["reserve_stock/1", "charge_card/2"]
    assert Enum.map(steps, & &1.output) == [%{reserved: true}, %{charged: true}]
  end

  test "get_workflow_result returns :pending for an enqueued workflow", %{config: config} do
    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

    assert SystemDb.get_workflow_result(config, workflow_id) == :pending
  end

  test "get_workflow_result returns the decoded output for a successful workflow", %{
    config: config
  } do
    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

    mark_success(config, workflow_id, %{total: 4999})

    assert SystemDb.get_workflow_result(config, workflow_id) == {:ok, %{total: 4999}}
  end

  test "get_workflow_result returns the decoded error for a failed workflow", %{config: config} do
    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

    mark_error(config, workflow_id, %RuntimeError{message: "card declined"})

    assert SystemDb.get_workflow_result(config, workflow_id) ==
             {:error, %RuntimeError{message: "card declined"}}
  end

  defp insert_operation_output(config, workflow_id, function_id, function_name, output) do
    Dbos.DB.Postgrex.query!(
      config.conn,
      "INSERT INTO dbos.operation_outputs (workflow_uuid, function_id, function_name, output, serialization) VALUES ($1, $2, $3, $4, $5)",
      [
        workflow_id,
        function_id,
        function_name,
        Dbos.Serialization.encode(output),
        Dbos.Serialization.format_name()
      ]
    )
  end

  defp mark_success(config, workflow_id, output) do
    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET status = 'SUCCESS', output = $2 WHERE workflow_uuid = $1",
      [workflow_id, Dbos.Serialization.encode(output)]
    )
  end

  defp mark_error(config, workflow_id, error) do
    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET status = 'ERROR', error = $2 WHERE workflow_uuid = $1",
      [workflow_id, Dbos.Serialization.encode(error)]
    )
  end
end
