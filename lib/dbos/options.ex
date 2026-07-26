defmodule Dbos.Options do
  @moduledoc """
  The option keys workflow dispatch accepts, and the validation `Dbos.start/3`, `Dbos.enqueue/3`
  and a `defworkflow`'s generated options dispatcher share.

  `start_options/0` and `queue_options/0` are the single definition every one of those call sites
  checks against, so an option is dispatchable exactly when it is listed here.
  """

  alias Dbos.InvalidWorkflowOptionError

  @start_options [
    :engine,
    :workflow_id,
    :deduplication_id,
    :priority,
    :application_version,
    :timeout_ms
  ]

  @queue_options [:queue_name, :delay_ms, :partition_key]

  @doc "The options `Dbos.start/3` accepts."
  def start_options, do: @start_options

  @doc "The options that only apply to a queued workflow."
  def queue_options, do: @queue_options

  @doc "The options `Dbos.enqueue/3` accepts."
  def enqueue_options, do: @start_options ++ @queue_options

  @doc "Validates `opts` for `Dbos.start/3`, raising `Dbos.InvalidWorkflowOptionError`."
  def validate_start!(name, opts) do
    keyword_list!(name, opts)
    Enum.each(opts, fn {key, _value} -> check_start_key!(name, key) end)
    :ok
  end

  @doc "Validates `opts` for `Dbos.enqueue/3`, raising `Dbos.InvalidWorkflowOptionError`."
  def validate_enqueue!(name, opts) do
    keyword_list!(name, opts)
    Enum.each(opts, fn {key, _value} -> check_dispatch_key!(name, key) end)
    reject_dedup_with_partition!(name, opts)
    :ok
  end

  @doc """
  Validates `opts` for a `defworkflow`'s generated options dispatcher, which routes to
  `Dbos.enqueue/3` under a `:queue_name` and `Dbos.start/3` without one. Raises
  `Dbos.InvalidWorkflowOptionError`.
  """
  def validate_dispatch!(name, opts) do
    keyword_list!(name, opts)
    Enum.each(opts, fn {key, _value} -> check_dispatch_key!(name, key) end)
    reject_queue_only_without_queue!(name, opts)
    reject_dedup_with_partition!(name, opts)
    :ok
  end

  defp keyword_list!(name, opts) do
    unless Keyword.keyword?(opts) do
      raise InvalidWorkflowOptionError,
        workflow_name: name,
        option: opts,
        reason: "is not a keyword list; pass options as `key: value` pairs"
    end
  end

  defp check_start_key!(name, key) do
    cond do
      key in @start_options ->
        :ok

      key in @queue_options ->
        raise InvalidWorkflowOptionError,
          workflow_name: name,
          option: key,
          reason:
            "only applies to a queued workflow; call Dbos.enqueue/3 with a queue_name: to use it"

      true ->
        raise unknown_option(name, key, @start_options)
    end
  end

  defp check_dispatch_key!(name, key) do
    if key in @start_options or key in @queue_options do
      :ok
    else
      raise unknown_option(name, key, enqueue_options())
    end
  end

  defp unknown_option(name, key, allowed) do
    %InvalidWorkflowOptionError{
      workflow_name: name,
      option: key,
      reason:
        "is not a workflow option; correct the spelling or remove it. Available: " <>
          Enum.map_join(allowed, ", ", &inspect/1)
    }
  end

  defp reject_queue_only_without_queue!(name, opts) do
    unless Keyword.has_key?(opts, :queue_name) do
      Enum.each(@queue_options -- [:queue_name], fn key ->
        if Keyword.has_key?(opts, key) do
          raise InvalidWorkflowOptionError,
            workflow_name: name,
            option: key,
            reason: "only applies to a queued workflow; add a queue_name: to enqueue it"
        end
      end)
    end
  end

  defp reject_dedup_with_partition!(name, opts) do
    if Keyword.has_key?(opts, :deduplication_id) and Keyword.has_key?(opts, :partition_key) do
      raise InvalidWorkflowOptionError,
        workflow_name: name,
        option: :partition_key,
        reason: "cannot be combined with :deduplication_id; a queued workflow takes one of them"
    end
  end
end
