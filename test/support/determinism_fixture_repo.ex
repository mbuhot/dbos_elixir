defmodule Dbos.DeterminismFixtureRepo do
  @moduledoc "Stands in for an application's repo in determinism fixtures, which only compile."

  def insert!(record), do: record
end
