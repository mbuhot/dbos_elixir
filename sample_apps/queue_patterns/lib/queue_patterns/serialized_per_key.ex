defmodule QueuePatterns.SerializedPerKey do
  @moduledoc """
  Serialized per-key execution: one workflow at a time per key.

  **Problem**: concurrent updates to the same logical entity (an account balance, a row a
  read-modify-write step touches) need to run one at a time to stay correct, while unrelated
  entities should still run in parallel for throughput.

  **Solution**: a partitioned queue with `worker_concurrency: 1` — each distinct `:partition_key`
  gets its own independent concurrency slot capped at one, so at most one workflow runs per key at
  a time, and different keys run concurrently with each other.

  **Observe**: enqueue several updates for the same `account_id` with
  `partition_key: account_id` — their `started_at_epoch_ms`/`completed_at` never overlap. Updates
  for a different `account_id` can run at the same time as those.
  """

  use Dbos

  defworkflow update_account(account_id, amount), name: "update_account" do
    apply_update(account_id, amount)
  end

  defstep apply_update(account_id, amount) do
    %{account_id: account_id, amount: amount, applied_at: System.os_time(:millisecond)}
  end
end
