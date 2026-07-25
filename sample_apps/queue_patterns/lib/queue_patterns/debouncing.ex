defmodule QueuePatterns.Debouncing do
  @moduledoc """
  Debouncing: collapsing a burst of updates for the same key into one run.

  **Problem**: deduplication rejects a repeat outright; some workloads instead want the classic
  "wait until the user stops typing" behavior — a document gets edited several times in quick
  succession, and only the last edit should trigger a reindex, once things settle down.

  **Solution**: `Dbos.Debouncer.debounce/4` either starts a fresh, delayed workflow keyed by
  `:debounce_key`, or "bounces" one still waiting out its delay — replacing its inputs with this
  call's and pushing its start time out by `:period_ms` from now. `:deadline_ms`, fixed at the
  first call, caps how far later bounces can push the delay.

  **Observe**: call `debounce_reindex/3` for the same `doc_id` several times within `period_ms` of
  each other — only one `reindex_document/2` workflow ever runs, and it runs with the *last*
  call's `revision`, once `period_ms` has passed since the last bounce.
  """

  use Dbos

  defworkflow reindex_document(doc_id, revision), name: "reindex_document" do
    do_reindex(doc_id, revision)
  end

  defstep do_reindex(doc_id, revision) do
    %{doc_id: doc_id, revision: revision, indexed_at: System.os_time(:millisecond)}
  end

  @doc "Debounces a reindex for `doc_id`, collapsing repeats within `opts[:period_ms]` (default `2_000`) of each other."
  def debounce_reindex(doc_id, revision, opts \\ []) do
    period_ms = Keyword.get(opts, :period_ms, 2_000)
    deadline_ms = Keyword.get(opts, :deadline_ms)

    Dbos.Debouncer.debounce(Dbos.config(), "reindex_document", [doc_id, revision],
      queue_name: "debounce_queue",
      debounce_key: doc_id,
      period_ms: period_ms,
      deadline_ms: deadline_ms
    )
  end
end
