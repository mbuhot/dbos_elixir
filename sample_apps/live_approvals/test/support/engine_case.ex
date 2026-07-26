defmodule LiveApprovals.EngineCase do
  @moduledoc """
  Starts a per-test `Dbos` engine in `:manual` testing mode, on the sandboxed connection the test
  already owns. `:manual` starts none of the engine's background processes, so nothing races the
  sandbox and every workflow runs synchronously, inside the test.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import LiveApprovals.EngineCase

      alias LiveApprovals.Approvals
      alias LiveApprovals.Repo
      alias LiveApprovals.Reviews
      alias LiveApprovals.Reviews.ReviewWorkflow
    end
  end

  setup tags do
    LiveApprovals.DataCase.setup_sandbox(tags)
    {:ok, engine: start_engine!()}
  end

  @doc "Starts an isolated engine, makes it the application default, and returns its name."
  def start_engine! do
    engine = Module.concat(LiveApprovals.TestEngine, :"E#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: engine,
       db: {Dbos.DB.Ecto, LiveApprovals.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       otp_app: :live_approvals,
       queues: [Dbos.Queue.new(LiveApprovals.Reviews.ReviewWorkflow.queue_name())],
       migrations: :skip,
       testing: :manual},
      id: engine
    )

    Application.put_env(:live_approvals, :dbos_engine, engine)
    on_exit(fn -> Application.delete_env(:live_approvals, :dbos_engine) end)

    engine
  end

  @doc "Blocks until `request_id`'s review has a decision waiting for it in the engine."
  def await_decision_delivered(engine, request_id) do
    config = Dbos.config(engine)
    topic = LiveApprovals.Reviews.decision_topic()

    wait_until(fn -> Dbos.SystemDb.notification_pending?(config, request_id, topic) end)
  end

  @doc "Marks `request_id`'s review `PENDING` again, standing in for a node that died mid-wait."
  def simulate_lost_process(engine, request_id) do
    config = Dbos.config(engine)

    Dbos.DB.Ecto.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1",
      [request_id]
    )

    :ok
  end

  @doc "Polls `fun` until it is truthy, failing the test if it never is."
  def wait_until(fun, attempts \\ 200)

  def wait_until(_fun, 0), do: ExUnit.Assertions.flunk("condition was never met")

  def wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  @doc "Starts the inbound PubSub-to-Dbos bridge bound to `engine`, sharing the test's connection."
  def start_inbound_bridge!(engine) do
    pid =
      start_supervised!(
        {LiveApprovals.Bridges.Inbound,
         engine: engine, pubsub: LiveApprovals.PubSub, name: :"inbound_#{engine}"}
      )

    Ecto.Adapters.SQL.Sandbox.allow(LiveApprovals.Repo, self(), pid)
    pid
  end

  @doc "Submission attributes for a claim above the policy ceiling."
  def large_claim(attrs \\ %{}) do
    Map.merge(
      %{
        "id" => "exp-#{System.unique_integer([:positive])}",
        "title" => "Team offsite dinner",
        "amount_cents" => "45000",
        "submitter" => "dana"
      },
      attrs
    )
  end

  @doc "Submission attributes for a claim the policy engine clears on its own."
  def small_claim(attrs \\ %{}) do
    large_claim(Map.merge(%{"title" => "Taxi to airport", "amount_cents" => "2500"}, attrs))
  end

  @doc "Runs every review currently queued, synchronously, and returns how many ran."
  def drain_reviews(engine) do
    Dbos.Testing.drain_queue(LiveApprovals.Reviews.ReviewWorkflow.queue_name(), engine: engine)
  end
end
