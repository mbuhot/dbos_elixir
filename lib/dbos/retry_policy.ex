defmodule Dbos.RetryPolicy do
  @moduledoc """
  A step's retry budget and backoff schedule. See `notes/steps-retry.md` for the defaults and
  the delay formula.
  """

  defstruct max_retries: 0,
            base_interval_ms: 100,
            backoff_factor: 2.0,
            max_interval_ms: 5000

  @type t :: %__MODULE__{
          max_retries: non_neg_integer,
          base_interval_ms: non_neg_integer,
          backoff_factor: float,
          max_interval_ms: non_neg_integer
        }

  @doc "Builds a policy from `opts`, applying defaults for any field not given."
  def new(opts \\ []) do
    %__MODULE__{
      max_retries: Keyword.get(opts, :max_retries, 0),
      base_interval_ms: Keyword.get(opts, :base_interval_ms, 100),
      backoff_factor: Keyword.get(opts, :backoff_factor, 2.0),
      max_interval_ms: Keyword.get(opts, :max_interval_ms, 5000)
    }
  end

  @doc "The backoff delay in milliseconds for 1-indexed attempt `attempt`."
  def delay_ms(%__MODULE__{} = policy, attempt) do
    delay = policy.base_interval_ms * :math.pow(policy.backoff_factor, attempt - 1)
    delay |> min(policy.max_interval_ms) |> round()
  end

  @doc "Whether `runs` completed runs still leave retry budget under `policy`."
  def retry?(%__MODULE__{} = policy, runs), do: runs <= policy.max_retries
end
