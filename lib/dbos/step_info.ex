defmodule Dbos.StepInfo do
  @moduledoc """
  Mirrors one `dbos.operation_outputs` row, with `output`/`error` decoded from their stored
  serialization.
  """

  alias Dbos.Serialization

  @columns ~w(
    workflow_uuid function_id function_name output error child_workflow_id
    started_at_epoch_ms completed_at_epoch_ms serialization
  )a

  defstruct @columns

  @doc "The `operation_outputs` columns, in the order `columns/0`, `from_row/1`, and every SELECT built against this table agree on."
  def columns, do: @columns

  @doc "Builds a `#{inspect(__MODULE__)}` from a row shaped like `columns/0`."
  def from_row(row) do
    @columns
    |> Enum.zip(row)
    |> Map.new()
    |> Map.update!(:output, &decode_or_nil/1)
    |> Map.update!(:error, &decode_or_nil/1)
    |> then(&struct!(__MODULE__, &1))
  end

  defp decode_or_nil(nil), do: nil
  defp decode_or_nil(binary), do: Serialization.decode(binary)
end
