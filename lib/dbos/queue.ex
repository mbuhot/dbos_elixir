defmodule Dbos.Queue do
  @moduledoc """
  A queue's declared configuration. Queues are declared on `Dbos.Supervisor`'s `:queues` option
  and persisted to the `queues` table on registration.
  """

  alias Dbos.InvalidQueueOptionError

  @internal_queue_name "_dbos_internal_queue"
  @default_base_polling_interval_ms 1_000
  @default_max_polling_interval_ms 120_000

  defstruct [
    :name,
    :worker_concurrency,
    :global_concurrency,
    :rate_limit,
    priority_enabled: false,
    partition_queue: false,
    base_polling_interval_ms: @default_base_polling_interval_ms,
    max_polling_interval_ms: @default_max_polling_interval_ms
  ]

  @type rate_limit :: %{limit: pos_integer, period_ms: pos_integer}

  @type t :: %__MODULE__{
          name: String.t(),
          worker_concurrency: pos_integer | nil,
          global_concurrency: pos_integer | nil,
          rate_limit: rate_limit | nil,
          priority_enabled: boolean,
          partition_queue: boolean,
          base_polling_interval_ms: pos_integer,
          max_polling_interval_ms: pos_integer
        }

  @doc "The reserved name of the always-present internal queue."
  def internal_queue_name, do: @internal_queue_name

  @doc """
  Builds a queue configuration. `opts`: `:worker_concurrency`, `:global_concurrency`,
  `:rate_limit` (`%{limit:, period_ms:}`), `:priority_enabled` (default `false`),
  `:partition_queue` (default `false`), `:base_polling_interval_ms` (default `1_000`). Raises
  `Dbos.InvalidQueueOptionError` on invalid combinations.
  """
  def new(name, opts \\ []) when is_binary(name) do
    queue = %__MODULE__{
      name: name,
      worker_concurrency: Keyword.get(opts, :worker_concurrency),
      global_concurrency: Keyword.get(opts, :global_concurrency),
      rate_limit: Keyword.get(opts, :rate_limit),
      priority_enabled: Keyword.get(opts, :priority_enabled, false),
      partition_queue: Keyword.get(opts, :partition_queue, false),
      base_polling_interval_ms:
        Keyword.get(opts, :base_polling_interval_ms, @default_base_polling_interval_ms)
    }

    validate!(queue)
    queue
  end

  @doc "Validates a queue configuration, raising `Dbos.InvalidQueueOptionError` on the first violation."
  def validate!(%__MODULE__{} = queue) do
    cond do
      queue.name == @internal_queue_name ->
        raise InvalidQueueOptionError,
          reason: "the name #{inspect(@internal_queue_name)} is reserved for the internal queue"

      exceeds_global_concurrency?(queue) ->
        raise InvalidQueueOptionError,
          reason: "worker_concurrency must be less than or equal to global_concurrency"

      queue.base_polling_interval_ms <= 0 ->
        raise InvalidQueueOptionError, reason: "base_polling_interval_ms must be positive"

      invalid_rate_limit?(queue.rate_limit) ->
        raise InvalidQueueOptionError,
          reason: "rate_limit's limit and period_ms must both be positive"

      true ->
        :ok
    end
  end

  defp exceeds_global_concurrency?(%__MODULE__{
         worker_concurrency: worker,
         global_concurrency: global
       })
       when is_integer(worker) and is_integer(global),
       do: worker > global

  defp exceeds_global_concurrency?(_queue), do: false

  defp invalid_rate_limit?(nil), do: false

  defp invalid_rate_limit?(%{limit: limit, period_ms: period_ms}),
    do: limit <= 0 or period_ms <= 0
end
