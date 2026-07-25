defmodule Dbos.WorkflowHandle do
  @moduledoc "A reference to a started workflow: which engine it runs under, and its id."

  defstruct [:engine, :workflow_id]

  @type t :: %__MODULE__{engine: atom, workflow_id: String.t()}
end
