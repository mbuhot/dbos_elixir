# Encodes and decodes Elixir terms for storage in the dbos schema's TEXT serialization columns.
#
# Terms are encoded with the External Term Format (:erlang.term_to_binary/1) and then
# base64-encoded, since the underlying columns are TEXT, not BYTEA, and raw ETF bytes are not
# guaranteed to be valid UTF-8. The format name stored alongside the value is "erl_etf".
#
# Decoding does not pass :safe. That option refuses to create atoms the decoding VM has never
# seen, and a recorded failure is full of them: the exception's own module, the anonymous-function
# names in its stacktrace, and whatever atoms the exception carries in its fields. A VM that has
# not itself raised that failure cannot name them, so :safe cannot read back what this module
# wrote — the very rows the engine exists to replay. Preloading modules does not help; a stacktrace
# entry's function name is an atom no module defines.
#
# What that costs is the atom table, which is never garbage collected, so a row is trusted not to
# have been crafted to exhaust it. That trust is already unavoidable: the system database holds the
# arguments a workflow replays with and the {module, function, args} an unwind applies
# (Dbos.Compensation), so anything able to write to it can already choose what this node runs.
# Pids, ports and references are still refused after decoding — a dead pid read back as live is a
# correctness problem rather than a security one.
defmodule Dbos.Serialization do
  @moduledoc false

  @format_name "erl_etf"

  @doc "The format name stored in the `serialization` column for this encoder."
  def format_name, do: @format_name

  @doc "Encodes a term to a base64 ETF binary suitable for a `TEXT` column."
  def encode(term) do
    term
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  @doc "Decodes a binary produced by `encode/1`, raising if it embeds a pid, port, or reference."
  def decode(binary) when is_binary(binary) do
    term =
      binary
      |> Base.decode64!()
      |> :erlang.binary_to_term()

    if contains_unsafe?(term) do
      raise ArgumentError, "refusing to decode a term containing a pid, port, or reference"
    end

    term
  end

  @doc "Decodes a binary, checking the stored format name first."
  def decode(binary, @format_name), do: {:ok, decode(binary)}
  def decode(_binary, other), do: {:error, {:unsupported_serialization, other}}

  @doc """
  Encodes a step or workflow failure: which kind it was raised as (`:error`, `:throw`, or
  `:exit`), the value (the exception struct itself for `:error`, so its type survives), and
  the stacktrace captured at the point of failure.
  """
  def encode_failure(kind, value, stacktrace) when kind in [:error, :throw, :exit] do
    encode(%{kind: kind, value: value, stacktrace: stacktrace})
  end

  @doc "Reproduces a failure captured by `encode_failure/3` and decoded by `decode/1`, re-raising it with its original stacktrace attached."
  def reraise_failure(%{kind: kind, value: value, stacktrace: stacktrace}) do
    :erlang.raise(kind, value, stacktrace)
  end

  defp contains_unsafe?(term) when is_pid(term) or is_port(term) or is_reference(term), do: true

  defp contains_unsafe?(term) when is_tuple(term) do
    term |> Tuple.to_list() |> contains_unsafe?()
  end

  defp contains_unsafe?(term) when is_list(term) do
    Enum.any?(term, &contains_unsafe?/1)
  end

  defp contains_unsafe?(term) when is_map(term) do
    term
    |> Map.to_list()
    |> Enum.any?(fn {key, value} -> contains_unsafe?(key) or contains_unsafe?(value) end)
  end

  defp contains_unsafe?(_term), do: false
end
