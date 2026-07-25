defmodule Dbos.Debouncer do
  @moduledoc """
  Collapses rapid, repeated enqueues of the same logical unit of work into one delayed workflow,
  per `notes/queues.md` §8. Each call either starts a fresh `DELAYED` workflow keyed by
  `opts[:debounce_key]`, or "bounces" an existing one still waiting out its delay: replacing its
  inputs and pushing `delay_until_epoch_ms` forward by `opts[:period_ms]`, capped at an optional
  absolute `opts[:deadline_ms]` from the first call.

  The reference's `debouncer.go` classifies a failed bounce (`bounceReturn`/`bounceEnqueue`/
  `bounceRaise`/`bounceRetry`) using internals this port's `notes/` does not fully transcribe; the
  three outcomes below are inferred from the documented columns and SQL, not copied verbatim —
  flagged in `DECISIONS.md`.
  """

  alias Dbos.Config
  alias Dbos.SystemDb

  @max_retry_attempts 20

  @doc """
  Debounces workflow `name_or_capture` with `args`. `opts`: `:queue_name` (required),
  `:debounce_key` (required), `:period_ms` (required — how far each bounce pushes
  `delay_until_epoch_ms`), `:deadline_ms` (optional — an absolute ceiling on the total delay,
  fixed at the first call and never extended by later bounces).

  Returns `{:ok, workflow_id}` — the same id across every bounce while the key is held. Raises
  `Dbos.QueueDeduplicatedError` if the key is currently held by a plain (non-debounced) dedup
  enqueue, or by a debounced workflow under a different name.
  """
  def debounce(%Config{} = config, name, args, opts) do
    queue_name = Keyword.fetch!(opts, :queue_name)
    debounce_key = Keyword.fetch!(opts, :debounce_key)
    period_ms = Keyword.fetch!(opts, :period_ms)
    deadline_ms = Keyword.get(opts, :deadline_ms)

    attempt(
      config,
      name,
      args,
      queue_name,
      debounce_key,
      period_ms,
      deadline_ms,
      @max_retry_attempts
    )
  end

  defp attempt(_config, _name, _args, queue_name, debounce_key, _period_ms, _deadline_ms, 0) do
    raise Dbos.QueueDeduplicatedError,
      workflow_id: nil,
      queue_name: queue_name,
      deduplication_id: debounce_key
  end

  defp attempt(
         config,
         name,
         args,
         queue_name,
         debounce_key,
         period_ms,
         deadline_ms,
         attempts_left
       ) do
    now = System.os_time(:millisecond)
    requested_delay_until = now + period_ms

    case SystemDb.bounce_debounced_workflow(
           config,
           name,
           queue_name,
           debounce_key,
           args,
           requested_delay_until
         ) do
      {:bounced, workflow_id} ->
        {:ok, workflow_id}

      :not_found ->
        handle_bounce_miss(
          config,
          name,
          args,
          queue_name,
          debounce_key,
          period_ms,
          deadline_ms,
          attempts_left
        )
    end
  end

  defp handle_bounce_miss(
         config,
         name,
         args,
         queue_name,
         debounce_key,
         period_ms,
         deadline_ms,
         attempts_left
       ) do
    case SystemDb.get_debounce_holder(config, queue_name, debounce_key) do
      :none ->
        insert_fresh(config, name, args, queue_name, debounce_key, period_ms, deadline_ms)

      {:holder, _workflow_id, true, ^name} ->
        attempt(
          config,
          name,
          args,
          queue_name,
          debounce_key,
          period_ms,
          deadline_ms,
          attempts_left - 1
        )

      {:holder, workflow_id, _is_debounced, _name} ->
        raise Dbos.QueueDeduplicatedError,
          workflow_id: workflow_id,
          queue_name: queue_name,
          deduplication_id: debounce_key
    end
  end

  defp insert_fresh(config, name, args, queue_name, debounce_key, period_ms, deadline_ms) do
    now = System.os_time(:millisecond)

    SystemDb.insert_debounced_workflow(config, %{
      name: name,
      queue_name: queue_name,
      inputs: args,
      debounce_key: debounce_key,
      delay_until_epoch_ms: now + period_ms,
      debounce_deadline_epoch_ms: deadline_ms && now + deadline_ms
    })
  end
end
