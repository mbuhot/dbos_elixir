defmodule QueuePatterns.TenantFairness do
  @moduledoc """
  Per-tenant fairness with a global cap: the two-queue composition.

  **Problem**: a partitioned queue gives every distinct `:partition_key` (here, `tenant_id`) its
  own independent concurrency and rate-limit allowance — exactly what stops one noisy tenant from
  starving another's slice. But that per-partition isolation is also what makes a queue-wide cap
  awkward on a single partitioned queue: there is no one place a *global* count is computed, only
  per-partition ones.

  **Solution**: two queues. `"tenant_jobs"` is partitioned with `worker_concurrency: 1` — each
  tenant gets one job running at a time, independent of every other tenant. `route_job/2` is what
  runs on that per-tenant slot; it does no real work itself, it only re-enqueues the actual job
  onto `"global_jobs"` — a second, non-partitioned queue capping the combined total across every
  tenant (`global_concurrency: 3` here) — and awaits it.

  **Observe**: enqueue jobs for several tenants at once, each keyed by
  `partition_key: tenant_id` on `"tenant_jobs"`. Each tenant's own jobs run one at a time, but the
  jobs of different tenants can run concurrently with each other, capped in total by
  `"global_jobs"`'s `global_concurrency` regardless of how many tenants are submitting.
  """

  use Dbos

  defworkflow route_job(tenant_id, job_id), name: "route_job" do
    {:ok, handle} = Dbos.enqueue(&do_job/2, [tenant_id, job_id], queue_name: "global_jobs")

    case Dbos.await(handle) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  defworkflow do_job(tenant_id, job_id), name: "do_job" do
    run_job(tenant_id, job_id)
  end

  defstep run_job(tenant_id, job_id) do
    %{tenant_id: tenant_id, job_id: job_id, ran_at: System.os_time(:millisecond)}
  end
end
