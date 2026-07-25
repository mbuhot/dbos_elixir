# The RETURNING shape of Dbos.SystemDb.insert_workflow_status/3: what the row looked like
# immediately after the upsert, including columns the caller's own insert may have lost a race on.
defmodule Dbos.InsertWorkflowResult do
  @moduledoc false

  alias Dbos.Status

  defstruct [
    :attempts,
    :status,
    :name,
    :queue_name,
    :queue_partition_key,
    :workflow_timeout_ms,
    :workflow_deadline_epoch_ms,
    :owner_xid
  ]

  @doc "Builds a `#{inspect(__MODULE__)}` from the `RETURNING` row, with `status` as a `Dbos.Status` atom."
  def from_row([
        attempts,
        status,
        name,
        queue_name,
        queue_partition_key,
        timeout_ms,
        deadline_ms,
        owner_xid
      ]) do
    %__MODULE__{
      attempts: attempts,
      status: Status.from_string(status),
      name: name,
      queue_name: queue_name,
      queue_partition_key: queue_partition_key,
      workflow_timeout_ms: timeout_ms,
      workflow_deadline_epoch_ms: deadline_ms,
      owner_xid: owner_xid
    }
  end
end
