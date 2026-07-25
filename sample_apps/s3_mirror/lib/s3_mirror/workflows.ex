defmodule S3Mirror.Workflows do
  @moduledoc """
  Mirrors every object under a prefix from one `S3Mirror.ObjectStore` to another: one durable
  `copy_object` workflow per key, driven from a queue (`"s3_mirror_copies"`, concurrency- and
  rate-limited on `S3Mirror.Application`), progress reported live over a `Dbos` stream.

  Both `mirror_bucket` and `copy_object` are idempotent under a restart. `mirror_bucket` itself
  is a durable workflow: a crash mid-run leaves every already-`Dbos.await`ed key's outcome
  checkpointed, and replay resumes only the keys not yet reached. Independently, each
  `copy_object` is keyed by a deterministic workflow id derived from the destination and key
  (`workflow_id/3`), so re-enqueueing the same object — whether from a replayed `mirror_bucket`
  or a fresh `mirror_bucket` call over the same bucket — collapses onto the one row already
  there rather than copying twice. `copy_object` also skips any key the destination already has,
  independent of `Dbos`'s own bookkeeping, the same way a real S3 sync skips existing objects.
  """

  use Dbos

  @queue_name "s3_mirror_copies"

  @doc "The deterministic `copy_object` workflow id for `(dest_mod, dest_store, key)`."
  def workflow_id(dest_mod, dest_store, key) do
    digest =
      :sha256
      |> :crypto.hash(:erlang.term_to_binary({dest_mod, dest_store, key}))
      |> Base.encode16(case: :lower)
      |> String.slice(0, 16)

    "mirror-#{digest}"
  end

  defworkflow mirror_bucket(source_mod, source_store, dest_mod, dest_store, prefix \\ ""),
    name: "mirror_bucket" do
    keys = list_source_keys(source_mod, source_store, prefix)

    handles =
      Enum.map(keys, fn key ->
        {:ok, handle} =
          Dbos.enqueue("copy_object", [source_mod, source_store, dest_mod, dest_store, key],
            queue_name: @queue_name,
            workflow_id: workflow_id(dest_mod, dest_store, key)
          )

        {key, handle}
      end)

    outcomes =
      Enum.map(handles, fn {key, handle} ->
        {:ok, outcome} = Dbos.await(handle)
        Dbos.write_stream("progress", %{key: key, outcome: outcome})
        {key, outcome}
      end)

    Dbos.close_stream("progress")
    summarize(outcomes)
  end

  defworkflow copy_object(source_mod, source_store, dest_mod, dest_store, key), name: "copy_object" do
    if already_copied?(dest_mod, dest_store, key) do
      :skipped
    else
      data = read_source(source_mod, source_store, key)
      copy_into_dest(dest_mod, dest_store, key, data)
      :copied
    end
  end

  defstep list_source_keys(source_mod, source_store, prefix) do
    {:ok, keys} = source_mod.list_keys(source_store, prefix)
    keys
  end

  defstep already_copied?(dest_mod, dest_store, key) do
    dest_mod.exists?(dest_store, key)
  end

  defstep read_source(source_mod, source_store, key) do
    {:ok, data} = source_mod.read(source_store, key)
    data
  end

  defstep copy_into_dest(dest_mod, dest_store, key, data) do
    :ok = dest_mod.write(dest_store, key, data)
    :ok
  end

  defp summarize(outcomes) do
    Enum.reduce(outcomes, %{copied: 0, skipped: 0}, fn {_key, outcome}, acc ->
      Map.update!(acc, outcome, &(&1 + 1))
    end)
  end

  @doc """
  Every key under `prefix`, paired with its `copy_object` status: `:copied`, `:skipped`, or
  `:remaining` (not yet finished). Reads `Dbos` workflow status directly — no polling of either
  object store — so this can be called while `mirror_bucket` is still running.
  """
  def progress(source_mod, source_store, dest_mod, dest_store, prefix, opts \\ []) do
    {:ok, keys} = source_mod.list_keys(source_store, prefix)

    Enum.map(keys, fn key ->
      {key, key_status(dest_mod, dest_store, key, opts)}
    end)
  end

  defp key_status(dest_mod, dest_store, key, opts) do
    case Dbos.status(workflow_id(dest_mod, dest_store, key), opts) do
      {:ok, %Dbos.WorkflowStatus{status: :success, output: outcome}} -> outcome
      _other -> :remaining
    end
  end

  @doc "The queue `mirror_bucket` enqueues every `copy_object` onto."
  def queue_name, do: @queue_name
end
