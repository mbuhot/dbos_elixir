defmodule QueuePatterns.TenantFairnessTest do
  use ExUnit.Case, async: false

  alias QueuePatterns.TenantFairness

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, QueuePatterns.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [TenantFairness],
       queues: [
         Dbos.Queue.new("tenant_jobs",
           partition_queue: true,
           worker_concurrency: 1,
           base_polling_interval_ms: 20
         ),
         Dbos.Queue.new("global_jobs", global_concurrency: 3, base_polling_interval_ms: 20)
       ],
       migrations: :skip},
      id: name
    )

    Dbos.Recovery.await_boot_recovery(name)
    {:ok, engine: name}
  end

  test "each tenant's jobs route through their own partition onto the shared global queue", %{
    engine: engine
  } do
    jobs = for tenant <- ["tenant-a", "tenant-b"], job_number <- 1..3, do: {tenant, job_number}

    handles =
      Enum.map(jobs, fn {tenant_id, job_number} ->
        job_id = "#{tenant_id}-job-#{job_number}"

        {:ok, handle} =
          Dbos.enqueue(&TenantFairness.route_job/2, [tenant_id, job_id],
            queue_name: "tenant_jobs",
            partition_key: tenant_id,
            engine: engine
          )

        {job_id, handle}
      end)

    results =
      Enum.map(handles, fn {job_id, handle} ->
        assert {:ok, result} = Dbos.await(handle, timeout_ms: 10_000)
        {job_id, result}
      end)

    for {job_id, result} <- results do
      assert result.job_id == job_id
      assert result.tenant_id in ["tenant-a", "tenant-b"]
    end

    assert length(results) == 6
  end
end
