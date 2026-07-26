defmodule LiveApprovals.Reviews do
  @moduledoc """
  The seam between the expense-claim data layer and the durable engine. This is the only module
  that both writes claims and talks to `Dbos`; `LiveApprovals.Approvals` stays unaware that
  workflows exist, and `LiveApprovals.Reviews.ReviewWorkflow` only ever calls back into the data
  layer.

  A claim's id is also its workflow id, so a request id is all anything needs to address the
  running review.
  """

  alias LiveApprovals.Approvals
  alias LiveApprovals.Reviews.ReviewWorkflow

  @decision_topic "decision"

  @doc "The `Dbos` message topic a parked review is listening on."
  def decision_topic, do: @decision_topic

  @doc "Inserts a claim and enqueues its durable review, returning the claim."
  def submit(attrs, opts \\ []) do
    attrs = Map.put_new_lazy(attrs, "id", &new_request_id/0)

    with {:ok, request} <- Approvals.submit_request(attrs) do
      {:ok, _handle} =
        Dbos.enqueue(&ReviewWorkflow.review/1, [request.id],
          queue_name: ReviewWorkflow.queue_name(),
          workflow_id: request.id,
          engine: engine(opts)
        )

      {:ok, request}
    end
  end

  @doc "Durably hands `decision` to the review parked on `request_id`."
  def deliver_decision(request_id, decision, decided_by, opts \\ []) do
    Dbos.send_message(
      request_id,
      @decision_topic,
      %{decision: decision, decided_by: decided_by},
      engine: engine(opts)
    )
  end

  @doc "The engine-recorded status of `request_id`'s review."
  def review_status(request_id, opts \\ []) do
    Dbos.status(request_id, engine: engine(opts))
  end

  @doc "The engine reviews run on: `opts[:engine]`, then application config, then `Dbos`."
  def engine(opts \\ []) do
    Keyword.get_lazy(opts, :engine, fn ->
      Application.get_env(:live_approvals, :dbos_engine, Dbos)
    end)
  end

  defp new_request_id do
    "exp-" <> (8 |> :crypto.strong_rand_bytes() |> Base.url_encode64(padding: false))
  end
end
