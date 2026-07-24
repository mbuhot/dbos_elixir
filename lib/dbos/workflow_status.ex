defmodule Dbos.WorkflowStatus do
  @moduledoc """
  Mirrors one `dbos.workflow_status` row, with `status` as a `Dbos.Status` atom and
  `inputs`/`output`/`error` decoded from their stored serialization.
  """

  alias Dbos.Serialization
  alias Dbos.Status

  @columns ~w(
    workflow_uuid status name authenticated_user assumed_role authenticated_roles
    request output error executor_id created_at updated_at application_version
    application_id class_name config_name recovery_attempts queue_name
    workflow_timeout_ms workflow_deadline_epoch_ms inputs started_at_epoch_ms
    deduplication_id priority queue_partition_key forked_from owner_xid
    parent_workflow_id serialization delay_until_epoch_ms was_forked_from
    rate_limited completed_at attributes schedule_name debounce_deadline_epoch_ms
    is_debounced
  )a

  defstruct @columns

  @doc "The `workflow_status` columns, in the order `columns/0`, `from_row/1`, and every SELECT built against this table agree on."
  def columns, do: @columns

  @doc "Builds a `#{inspect(__MODULE__)}` from a row shaped like `columns/0`."
  def from_row(row) do
    @columns
    |> Enum.zip(row)
    |> Map.new()
    |> Map.update!(:status, &Status.from_string/1)
    |> Map.update!(:inputs, &decode_or_nil/1)
    |> Map.update!(:output, &decode_or_nil/1)
    |> Map.update!(:error, &decode_or_nil/1)
    |> then(&struct!(__MODULE__, &1))
  end

  defp decode_or_nil(nil), do: nil
  defp decode_or_nil(binary), do: Serialization.decode(binary)
end
