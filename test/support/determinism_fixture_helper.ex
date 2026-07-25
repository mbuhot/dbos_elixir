defmodule Dbos.DeterminismFixtureHelper do
  @moduledoc "A plain module with no `defstep`, used by `Dbos.DeterminismTest` to exercise the undeclared-cross-module-call warning."

  def side_effect(x), do: x
end
