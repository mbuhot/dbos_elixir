defmodule Dbos.Version do
  @moduledoc """
  Computes a deterministic application version from a set of workflow modules' compiled code,
  used as the default `application_version` when neither `DBOS__APPVERSION` nor an explicit
  config value is given.
  """

  @doc "A short hex digest derived from `modules`' BEAM code chunks, excluding the `Docs` chunk, stable regardless of list order."
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
    {^module, binary, _filename} = :code.get_object_code(module)
    chunk_ids = chunk_ids(binary)

    {:ok, {^module, chunks}} = :beam_lib.chunks(binary, chunk_ids)

    chunks
    |> Enum.sort()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp chunk_ids(binary) do
    binary
    |> :beam_lib.info()
    |> Keyword.fetch!(:chunks)
    |> Enum.map(fn {chunk_id, _pos, _size} -> chunk_id end)
    |> Enum.reject(&(&1 == "Docs"))
  end
end
