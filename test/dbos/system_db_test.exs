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

  test "list_workflows filters by a list of statuses, returning exactly the union", %{
    config: config
  } do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-pending",
      status: :pending,
      name: "W"
    })

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-enqueued",
      status: :enqueued,
      name: "W",
      queue_name: "q"
    })

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-success",
      status: :success,
      name: "W"
    })

    {:ok, results} = SystemDb.list_workflows(config, status: [:pending, :enqueued], sort: :asc)

    assert Enum.map(results, & &1.workflow_uuid) |> Enum.sort() == [
             "wf-enqueued",
             "wf-pending"
           ]
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

  describe "insert_workflow_status/3" do
    test "inserts a PENDING row and returns the RETURNING shape", %{config: config} do
      result =
        SystemDb.insert_workflow_status(config, %{
          workflow_id: "wf-insert-1",
          status: :pending,
          name: "Checkout.process_order",
          inputs: ["ord_1"],
          owner_xid: "owner-1"
        })

      assert result.attempts == 1
      assert result.status == :pending
      assert result.name == "Checkout.process_order"
      assert result.owner_xid == "owner-1"

      {:ok, status} = SystemDb.get_workflow_status(config, "wf-insert-1")
      assert status.status == :pending
      assert status.inputs == ["ord_1"]
    end

    test "an ENQUEUED insert starts recovery_attempts at 0", %{config: config} do
      result =
        SystemDb.insert_workflow_status(config, %{
          workflow_id: "wf-insert-2",
          status: :enqueued,
          name: "W",
          queue_name: "q",
          owner_xid: "owner-2"
        })

      assert result.attempts == 0
    end

    test "a re-entrant call without increment_attempts does not bump recovery_attempts or touch owner_xid",
         %{config: config} do
      SystemDb.insert_workflow_status(config, %{
        workflow_id: "wf-reentrant",
        status: :pending,
        name: "W",
        owner_xid: "owner-first"
      })

      result =
        SystemDb.insert_workflow_status(config, %{
          workflow_id: "wf-reentrant",
          status: :pending,
          name: "W",
          owner_xid: "owner-second"
        })

      assert result.attempts == 1
      assert result.owner_xid == "owner-first"
    end

    test "increment_attempts drives recovery_attempts up until MAX_RECOVERY_ATTEMPTS_EXCEEDED, owner_xid unchanged",
         %{config: config} do
      first =
        SystemDb.insert_workflow_status(config, %{
          workflow_id: "wf-dlq",
          status: :pending,
          name: "W",
          owner_xid: "owner-original"
        })

      assert first.owner_xid == "owner-original"

      second =
        SystemDb.insert_workflow_status(
          config,
          %{workflow_id: "wf-dlq", status: :pending, name: "W", owner_xid: "owner-retry-1"},
          increment_attempts: true,
          max_retries: 2
        )

      assert second.attempts == 2
      assert second.owner_xid == "owner-original"

      third =
        SystemDb.insert_workflow_status(
          config,
          %{workflow_id: "wf-dlq", status: :pending, name: "W", owner_xid: "owner-retry-2"},
          increment_attempts: true,
          max_retries: 2
        )

      assert third.attempts == 3
      assert third.owner_xid == "owner-original"

      assert_raise Dbos.MaxRecoveryAttemptsExceededError, fn ->
        SystemDb.insert_workflow_status(
          config,
          %{workflow_id: "wf-dlq", status: :pending, name: "W", owner_xid: "owner-retry-3"},
          increment_attempts: true,
          max_retries: 2
        )
      end

      {:ok, status} = SystemDb.get_workflow_status(config, "wf-dlq")
      assert status.status == :max_recovery_attempts_exceeded
      assert status.deduplication_id == nil
      assert status.started_at_epoch_ms == nil
      assert status.queue_name == nil
      assert status.owner_xid == "owner-original"
    end

    test "an ENQUEUED re-entrant call never bumps recovery_attempts even with increment_attempts",
         %{config: config} do
      SystemDb.insert_workflow_status(config, %{
        workflow_id: "wf-enqueued-reentrant",
        status: :enqueued,
        name: "W",
        queue_name: "q",
        owner_xid: "owner-1"
      })

      result =
        SystemDb.insert_workflow_status(
          config,
          %{
            workflow_id: "wf-enqueued-reentrant",
            status: :enqueued,
            name: "W",
            queue_name: "q",
            owner_xid: "owner-2"
          },
          increment_attempts: true
        )

      assert result.attempts == 0
    end
  end

  describe "check_operation_execution/4" do
    test "returns :none when the step has not run yet", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      Dbos.DB.Postgrex.query!(
        config.conn,
        "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1",
        [workflow_id]
      )

      assert SystemDb.check_operation_execution(config, workflow_id, 0, "reserve_stock/1") ==
               :none
    end

    test "returns {:replay, output} for an already-recorded step", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      insert_operation_output(config, workflow_id, 0, "reserve_stock/1", %{reserved: true})

      assert SystemDb.check_operation_execution(config, workflow_id, 0, "reserve_stock/1") ==
               {:replay, %{reserved: true}}
    end

    test "returns {:replay_failure, decoded_failure} for a step recorded with an error", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      failure = Dbos.Serialization.encode_failure(:error, %RuntimeError{message: "boom"}, [])

      Dbos.DB.Postgrex.query!(
        config.conn,
        "INSERT INTO dbos.operation_outputs (workflow_uuid, function_id, function_name, error, serialization) VALUES ($1, $2, $3, $4, $5)",
        [workflow_id, 0, "reserve_stock/1", failure, Dbos.Serialization.format_name()]
      )

      assert {:replay_failure, %{kind: :error, value: %RuntimeError{message: "boom"}}} =
               SystemDb.check_operation_execution(config, workflow_id, 0, "reserve_stock/1")
    end

    test "raises UnexpectedStepError when the recorded function_name differs", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      insert_operation_output(config, workflow_id, 0, "reserve_stock/1", %{reserved: true})

      assert_raise Dbos.UnexpectedStepError, fn ->
        SystemDb.check_operation_execution(config, workflow_id, 0, "charge_card/2")
      end
    end

    test "raises WorkflowCancelledError when the workflow is cancelled", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      Dbos.DB.Postgrex.query!(
        config.conn,
        "UPDATE dbos.workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = $1",
        [workflow_id]
      )

      assert_raise Dbos.WorkflowCancelledError, fn ->
        SystemDb.check_operation_execution(config, workflow_id, 0, "reserve_stock/1")
      end
    end

    test "raises NonExistentWorkflowError when the workflow does not exist", %{config: config} do
      assert_raise Dbos.NonExistentWorkflowError, fn ->
        SystemDb.check_operation_execution(config, "does-not-exist", 0, "reserve_stock/1")
      end
    end
  end

  describe "record_operation_result/3" do
    test "inserts a row and stamps workflow_status.executor_id on the winning insert", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      assert :ok =
               SystemDb.record_operation_result(config, %{
                 workflow_id: workflow_id,
                 function_id: 0,
                 function_name: "reserve_stock/1",
                 output: Dbos.Serialization.encode(%{reserved: true}),
                 started_at: 1000,
                 completed_at: 1010
               })

      {:ok, [step]} = SystemDb.get_workflow_steps(config, workflow_id)
      assert step.function_name == "reserve_stock/1"
      assert step.output == %{reserved: true}

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.executor_id == "exec-1"
    end

    test "an identical retried write is an idempotent no-op", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      attrs = %{
        workflow_id: workflow_id,
        function_id: 0,
        function_name: "reserve_stock/1",
        output: Dbos.Serialization.encode(%{reserved: true}),
        started_at: 1000,
        completed_at: 1010
      }

      assert :ok = SystemDb.record_operation_result(config, attrs)
      assert :ok = SystemDb.record_operation_result(config, attrs)

      {:ok, [step]} = SystemDb.get_workflow_steps(config, workflow_id)
      assert step.output == %{reserved: true}
    end

    test "a retried write for a different function_name raises UnexpectedStepError", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      attrs = %{
        workflow_id: workflow_id,
        function_id: 0,
        function_name: "reserve_stock/1",
        output: Dbos.Serialization.encode(%{reserved: true}),
        started_at: 1000,
        completed_at: 1010
      }

      SystemDb.record_operation_result(config, attrs)

      assert_raise Dbos.UnexpectedStepError, fn ->
        SystemDb.record_operation_result(config, %{attrs | function_name: "charge_card/2"})
      end
    end
  end

  describe "clear_queue_assignment/2" do
    test "moves a PENDING queued workflow back to ENQUEUED and nulls started_at", %{
      config: config
    } do
      workflow_id = "wf-clear-queue"

      SystemDb.insert_workflow_status(config, %{
        workflow_id: workflow_id,
        status: :pending,
        name: "W",
        queue_name: "orders"
      })

      assert SystemDb.clear_queue_assignment(config, workflow_id) == :cleared

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :enqueued
      assert status.started_at_epoch_ms == nil
      assert status.queue_name == "orders"
    end

    test "returns :not_cleared when the workflow is not PENDING with a queue_name", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      assert SystemDb.clear_queue_assignment(config, workflow_id) == :not_cleared
    end
  end

  describe "check_child_workflow/3" do
    test "returns :none when no child has been recorded for that step", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      assert SystemDb.check_child_workflow(config, workflow_id, 0, "Refund.process") == :none
    end

    test "returns {:existing, child_id} when the recorded step matches the child's name", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      SystemDb.record_operation_result(config, %{
        workflow_id: workflow_id,
        function_id: 0,
        function_name: "Refund.process",
        child_workflow_id: "#{workflow_id}-0",
        started_at: 1000,
        completed_at: 1000
      })

      assert SystemDb.check_child_workflow(config, workflow_id, 0, "Refund.process") ==
               {:existing, "#{workflow_id}-0"}
    end

    test "raises UnexpectedStepError when the recorded step is a different workflow name", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      SystemDb.record_operation_result(config, %{
        workflow_id: workflow_id,
        function_id: 0,
        function_name: "Refund.process",
        child_workflow_id: "#{workflow_id}-0",
        started_at: 1000,
        completed_at: 1000
      })

      assert_raise Dbos.UnexpectedStepError, fn ->
        SystemDb.check_child_workflow(config, workflow_id, 0, "Other.workflow")
      end
    end
  end

  describe "update_workflow_outcome/3" do
    test "SUCCESS records the output and clears deduplication_id", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [1],
          deduplication_id: "dedup-1"
        })

      assert :ok =
               SystemDb.update_workflow_outcome(config, workflow_id, %{
                 status: :success,
                 output: Dbos.Serialization.encode(%{total: 4999})
               })

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :success
      assert status.output == %{total: 4999}
      assert status.deduplication_id == nil
      assert status.completed_at != nil
    end

    test "ERROR records the serialized error", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      failure = Dbos.Serialization.encode_failure(:error, %RuntimeError{message: "declined"}, [])

      assert :ok =
               SystemDb.update_workflow_outcome(config, workflow_id, %{
                 status: :error,
                 error: failure
               })

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :error

      assert status.error == %{
               kind: :error,
               value: %RuntimeError{message: "declined"},
               stacktrace: []
             }
    end

    test "refuses to overwrite a terminal SUCCESS and is a silent no-op", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      mark_success(config, workflow_id, %{total: 1})

      assert :ok =
               SystemDb.update_workflow_outcome(config, workflow_id, %{
                 status: :error,
                 error: Dbos.Serialization.encode_failure(:error, %RuntimeError{message: "x"}, [])
               })

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :success
    end

    test "raises WorkflowCancelledError when the row was cancelled underneath it", %{
      config: config
    } do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      Dbos.DB.Postgrex.query!(
        config.conn,
        "UPDATE dbos.workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = $1",
        [workflow_id]
      )

      assert_raise Dbos.WorkflowCancelledError, fn ->
        SystemDb.update_workflow_outcome(config, workflow_id, %{status: :success, output: nil})
      end
    end
  end
end
