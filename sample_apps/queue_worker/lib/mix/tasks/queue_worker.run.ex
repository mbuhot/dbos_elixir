defmodule Mix.Tasks.QueueWorker.Run do
  @moduledoc """
  Enqueues a batch of tasks, kills one in-flight worker process partway through the batch, and
  confirms every task still completes exactly once.

  Usage: `mix queue_worker.run [count]` — `count` defaults to 20.
  """

  use Mix.Task

  alias QueueWorker.Producer
  alias QueueWorker.Tasks

  @shortdoc "Runs the queue-worker kill-a-worker demo"

  @impl true
  def run(args) do
    Mix.Task.run("app.start")

    count = args |> List.first() |> parse_count()
    batch_id = "batch-#{System.unique_integer([:positive])}"

    Mix.shell().info("Enqueueing #{count} tasks under #{inspect(batch_id)}...")
    handles = Producer.enqueue_batch(batch_id, count)

    kill_one_worker(handles)

    Mix.shell().info("Recovering this executor's pending workflows (a restarted worker does this at boot)...")
    Dbos.Recovery.recover_pending(Dbos)

    Mix.shell().info("Awaiting completion (checkpointed tasks are not re-run)...")
    results = Producer.await_all(handles)

    verify(batch_id, count, results)
  end

  defp parse_count(nil), do: 20
  defp parse_count(str), do: String.to_integer(str)

  defp kill_one_worker(handles, attempts \\ 300)

  defp kill_one_worker(_handles, 0) do
    Mix.shell().info("No in-flight worker found to kill — batch finished before we caught one")
  end

  defp kill_one_worker(handles, attempts) do
    handles
    |> Enum.find_value(fn handle ->
      case Dbos.WorkflowSup.whereis(Dbos, handle.workflow_id) do
        {:ok, pid} -> {handle, pid}
        :error -> nil
      end
    end)
    |> case do
      {handle, pid} ->
        Mix.shell().info("Killing worker process for #{handle.workflow_id} (#{inspect(pid)})")
        Process.exit(pid, :kill)

      nil ->
        Process.sleep(10)
        kill_one_worker(handles, attempts - 1)
    end
  end

  defp verify(batch_id, count, results) do
    %{rows: [[success_count]]} =
      Postgrex.query!(
        QueueWorker.Repo,
        "SELECT count(*) FROM \"dbos\".workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{batch_id}-%"]
      )

    claim_counts = for task_number <- 1..count, do: Tasks.execution_count(:claim_task, batch_id, task_number)
    work_counts = for task_number <- 1..count, do: Tasks.execution_count(:do_work, batch_id, task_number)

    Mix.shell().info("#{success_count}/#{count} tasks SUCCESS in dbos.workflow_status")
    Mix.shell().info("Collected #{length(results)} results")
    Mix.shell().info("Per-task claim_task attempts: #{inspect(claim_counts)}")
    Mix.shell().info("Per-task do_work attempts: #{inspect(work_counts)}")

    if success_count == count and length(results) == count do
      Mix.shell().info(
        "Exactly-once outcome confirmed: every task reached SUCCESS despite the kill, none lost."
      )
    else
      Mix.shell().error(
        "Mismatch: expected #{count} successes and #{count} results, saw " <>
          "#{success_count} successes and #{length(results)} results"
      )
    end
  end
end
