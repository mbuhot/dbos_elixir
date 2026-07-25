defmodule Dbos.StepNames do
  @moduledoc "The reserved `function_name` strings for built-in durable operations, so no call site spells one by hand."

  @doc "The `function_name` for awaiting a workflow's result from within another workflow."
  def get_result, do: "DBOS.getResult"

  @doc "The `function_name` for `Send` calls made from within a workflow."
  def send_message, do: "DBOS.send"

  @doc "The `function_name` for the `Recv` step."
  def recv, do: "DBOS.recv"

  @doc "The `function_name` shared by the standalone `Sleep` step and the internal recv/getEvent timeout step."
  def sleep, do: "DBOS.sleep"

  @doc "The `function_name` for `SetEvent`."
  def set_event, do: "DBOS.setEvent"

  @doc "The `function_name` for `GetEvent`."
  def get_event, do: "DBOS.getEvent"

  @doc "The `function_name` for `WriteStream`."
  def write_stream, do: "DBOS.writeStream"

  @doc "The `function_name` for `CloseStream`."
  def close_stream, do: "DBOS.closeStream"

  @doc "The `function_name` for a `Patch`/`DeprecatePatch` checkpoint at the given patch name."
  def patch(patch_name), do: "DBOS.patch-" <> patch_name
end
