defmodule S3Mirror.ObjectStore do
  @moduledoc """
  A bucket-shaped key/value blob store: list, read, write, and existence-check by key.
  `S3Mirror.ObjectStore.Local` implements this over a local directory tree;
  `S3Mirror.ObjectStore.S3` implements it over a real S3 bucket.
  """

  @type store :: term
  @type key :: String.t()

  @doc "Every key under `prefix` (empty string for the whole bucket), sorted."
  @callback list_keys(store, prefix :: String.t()) :: {:ok, [key]} | {:error, term}

  @doc "Reads one object's bytes."
  @callback read(store, key) :: {:ok, binary} | {:error, term}

  @doc "Writes one object's bytes, creating or overwriting it."
  @callback write(store, key, binary) :: :ok | {:error, term}

  @doc "Whether `key` already exists in the store."
  @callback exists?(store, key) :: boolean
end
