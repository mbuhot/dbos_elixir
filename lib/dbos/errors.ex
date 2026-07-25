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
  @moduledoc "Raised when a `Dbos.Queue` configuration is invalid, per `notes/queues.md` §1."

  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}), do: "invalid queue configuration: #{reason}"
end

defmodule Dbos.QueueDeduplicatedError do
  @moduledoc """
  Raised when an enqueue's deduplication id already has a live holder on the same queue, per
  `notes/queues.md` §10.
  """

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
  function_id)` and neither is a byte-for-byte match, per `notes/engine-core.md` §3.
  """

  defexception [:workflow_id, :function_id, :reason]

  @impl true
  def message(%__MODULE__{workflow_id: workflow_id, function_id: function_id, reason: reason}) do
    "workflow #{workflow_id} step #{function_id}: #{reason}"
  end
end

defmodule Dbos.MaxStepRetriesExceededError do
  @moduledoc """
  Raised when a step exhausts its configured retry budget. Mirrors upstream's
  `MaxStepRetriesExceededError`, wrapping the last underlying failure.
  """

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
