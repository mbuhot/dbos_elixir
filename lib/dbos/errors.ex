defmodule Dbos.WorkflowCancelledError do
  @moduledoc "Raised when an operation runs against a workflow whose status is `:cancelled`."

  defexception [:workflow_id]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id}) do
    "workflow #{workflow_id} was cancelled; stop running steps against it"
  end
end

defmodule Dbos.UnexpectedStepError do
  @moduledoc """
  Raised when a checkpointed step's recorded `function_name` does not match the step now
  expected at that id, a determinism violation between the original run and this replay.
  """

  defexception [:workflow_id, :function_id, :expected, :recorded]

  @impl true
  def message(%__MODULE__{
        workflow_id: workflow_id,
        function_id: function_id,
        expected: expected,
        recorded: recorded
      }) do
    "workflow #{workflow_id} step #{function_id} expected #{inspect(expected)} but the " <>
      "recorded step is #{inspect(recorded)}; make sure the workflow body is deterministic across replays"
  end
end

defmodule Dbos.NonExistentWorkflowError do
  @moduledoc "Raised when an operation references a workflow id with no `workflow_status` row."

  defexception [:workflow_id]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id}) do
    "workflow #{workflow_id} does not exist; start it before running steps against it"
  end
end

defmodule Dbos.MaxRecoveryAttemptsExceededError do
  @moduledoc """
  Raised when a workflow's recovery attempts exceed the configured maximum; the row has been
  moved to `MAX_RECOVERY_ATTEMPTS_EXCEEDED` and will not run again without an explicit resume.
  """

  defexception [:workflow_id, :attempts]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, attempts: attempts}) do
    "workflow #{workflow_id} exceeded its maximum recovery attempts (#{attempts}); " <>
      "resume it explicitly before it will run again"
  end
end

defmodule Dbos.NotStartedError do
  @moduledoc "Raised when a call requires the DBOS engine and it has not been started."

  defexception []

  @impl true
  def message(_error) do
    "the DBOS engine has not been started; start it before calling this function"
  end
end

defmodule Dbos.NotInWorkflowError do
  @moduledoc "Raised when a call requires an active workflow context and none is present."

  defexception []

  @impl true
  def message(_error) do
    "this call must run inside a workflow context; wrap it with Dbos.Runtime.with_context/2"
  end
end

defmodule Dbos.InvalidQueueOptionError do
  @moduledoc "Raised when a `Dbos.Queue` configuration is invalid."

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}), do: "invalid queue configuration: #{reason}"
end

defmodule Dbos.QueueDeduplicatedError do
  @moduledoc "Raised when an enqueue's deduplication id already has a live holder on the same queue."

  defexception [:workflow_id, :queue_name, :deduplication_id]

  @impl true
  def message(%__MODULE__{
        workflow_id: workflow_id,
        queue_name: queue_name,
        deduplication_id: deduplication_id
      }) do
    "workflow #{workflow_id} was not enqueued: deduplication id #{inspect(deduplication_id)} " <>
      "is already held on queue #{inspect(queue_name)}"
  end
end

defmodule Dbos.ConcurrentCheckpointConflictError do
  @moduledoc """
  Raised when a step checkpoint write races another write for the same `(workflow_uuid,
  function_id)` and neither is a byte-for-byte match.
  """

  defexception [:workflow_id, :function_id, :reason]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, function_id: function_id, reason: reason}) do
    "workflow #{workflow_id} step #{function_id}: #{reason}"
  end
end

defmodule Dbos.RecvConflictError do
  @moduledoc "Raised when a `recv` registers as the receiver for a `(workflow_id, topic)` pair already held by another in-progress `recv`."

  defexception [:workflow_id, :topic]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, topic: topic}) do
    "workflow #{workflow_id} is already receiving on topic #{inspect(topic)}"
  end
end

defmodule Dbos.RecvTimeoutError do
  @moduledoc "Raised when `recv`'s durable timeout elapses with no message consumed."

  defexception [:workflow_id, :topic]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, topic: topic}) do
    "workflow #{workflow_id} timed out waiting for a message on topic #{inspect(topic)}"
  end
end

defmodule Dbos.StreamClosedError do
  @moduledoc "Raised when `write_stream` is called against a stream that already recorded its close sentinel."

  defexception [:workflow_id, :key]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, key: key}) do
    "stream #{inspect(key)} on workflow #{workflow_id} is already closed"
  end
end

defmodule Dbos.NestedTransactionError do
  @moduledoc "Raised when `Dbos.transaction/3` is called from inside another `Dbos.transaction/3`'s body."

  defexception [:workflow_id]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id}) do
    "workflow #{workflow_id}: cannot call Dbos.transaction/3 within a transaction"
  end
end

defmodule Dbos.StepInTransactionError do
  @moduledoc """
  Raised when `Dbos.Runtime.run_step/3` (or any durable operation built on it — `send_message`,
  `recv_message`, `set_event`, etc.) is called from inside a `Dbos.transaction/3` body.
  """

  defexception [:workflow_id, :function_name]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, function_name: function_name}) do
    "workflow #{workflow_id}: cannot call step #{inspect(function_name)} from within a Dbos.transaction/3 body"
  end
end

defmodule Dbos.PatchInStepError do
  @moduledoc """
  Raised when `Dbos.patch/1` or `Dbos.deprecate_patch/1` is called from inside a step or a
  `Dbos.transaction/3` body. A patch check decides whether to consume a step id, which only the
  workflow body allocates.
  """

  defexception [:workflow_id, :patch_name, :enclosing]

  @impl true
  def message(%__MODULE__{
        workflow_id: workflow_id,
        patch_name: patch_name,
        enclosing: enclosing
      }) do
    "workflow #{workflow_id}: cannot check patch #{inspect(patch_name)} from within a " <>
      "#{enclosing} body; call it from the workflow body"
  end
end

defmodule Dbos.NotSupportedError do
  @moduledoc "Raised when a call requires functionality the engine does not implement."

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}), do: reason
end

defmodule Dbos.Waits.Parked do
  @moduledoc """
  Raised at a durable wait site to unwind a workflow process that is parking instead of staying
  resident. Caught by `Dbos.WorkflowProcess`, which records no outcome — the workflow's row is
  left `PENDING`, exactly as `Dbos.Waits` left it, for the eventual wake to redispatch.
  """

  defexception [:workflow_id]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id}) do
    "workflow #{workflow_id} parked its durable wait; its process is exiting"
  end
end

defmodule Dbos.TestingModeWaitError do
  @moduledoc """
  Raised when a durable wait (`recv_message/2`, `get_event/4`) has nothing pending under an
  `:inline`/`:manual` testing-mode engine — one with no background process able to wake it
  later.
  """

  defexception [:workflow_id, :operation, :topic_or_key]

  @impl true
  def message(%__MODULE__{
        workflow_id: workflow_id,
        operation: operation,
        topic_or_key: topic_or_key
      }) do
    "workflow #{workflow_id}: #{operation} has nothing pending for #{inspect(topic_or_key)} " <>
      "under a testing-mode engine; send the message or set the event before running the " <>
      "workflow, or enqueue it and drain with Dbos.Testing under :manual mode"
  end
end

defmodule Dbos.MaxStepRetriesExceededError do
  @moduledoc "Raised when a step exhausts its configured retry budget, wrapping the last underlying failure."

  defexception [:workflow_id, :function_name, :max_retries, :cause]

  @impl true
  def message(%__MODULE__{
        workflow_id: workflow_id,
        function_name: function_name,
        max_retries: max_retries,
        cause: cause
      }) do
    "workflow #{workflow_id} step #{function_name} exceeded #{max_retries} retries; " <>
      "last error: #{Exception.format_banner(:error, cause)}"
  end
end
