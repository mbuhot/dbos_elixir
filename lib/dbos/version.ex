# Computes a deterministic application version from a set of workflow modules' compiled code, used
# as the default application_version when neither DBOS__APPVERSION nor an explicit config value is
# given.
defmodule Dbos.Version do
  @moduledoc false

  @doc "A short hex digest over `modules`' compiled code, stable regardless of list order."
  def compute(modules) do
    modules
    |> Enum.map(&module_digest/1)
    |> Enum.sort()
    |> Enum.join()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> String.slice(0, 16)
  end

  defp module_digest(module) do
    Code.ensure_loaded!(module)

    module.module_info(:md5)
    |> Base.encode16(case: :lower)
  end
end
