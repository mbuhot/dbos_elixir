defmodule LiveApprovals.Reviews.ReviewWorkflow do
  @moduledoc """
  The durable review of one expense claim. The workflow id is the request id, so anything holding
  a request id can address the running review.

  Small claims clear the policy engine on their own. Anything above the policy ceiling parks in
  `Dbos.recv_message/3` on the `"decision"` topic until a human answers — a wait that outlives the
  workflow process and the node it started on.

  Every stage transition runs through `LiveApprovals.Approvals.record_stage/3` inside a durable
  step, which is what puts the workflow's progress onto `Phoenix.PubSub`.
  """

  use Dbos, repo: LiveApprovals.Repo

  alias LiveApprovals.Approvals

  @queue_name "reviews"
  @policy_ceiling_cents 100_00
  @decision_timeout_ms :timer.hours(72)

  @doc "The queue reviews are dispatched on."
  def queue_name, do: @queue_name

  @doc "The claim amount at or below which the policy engine decides on its own."
  def policy_ceiling_cents, do: @policy_ceiling_cents

  @doc "How long a claim waits on a human before it is rejected unanswered."
  def decision_timeout_ms, do: @decision_timeout_ms

  defworkflow review(request_id), name: "review_expense" do
    announce(request_id, :validating)
    amount_cents = load_amount(request_id)
    announce(request_id, :policy_check)

    outcome =
      case classify(amount_cents) do
        :within_policy ->
          %{decision: :approved, decided_by: "policy-engine"}

        :needs_human ->
          announce(request_id, :awaiting_decision)
          collect_decision()
      end

    settle(request_id, outcome.decision, outcome.decided_by)
    Map.put(outcome, :request_id, request_id)
  end

  defp classify(amount_cents) when amount_cents <= @policy_ceiling_cents, do: :within_policy
  defp classify(_amount_cents), do: :needs_human

  defp collect_decision do
    Dbos.recv_message("decision", @decision_timeout_ms)
  rescue
    Dbos.RecvTimeoutError -> %{decision: :rejected, decided_by: "timed out"}
  end

  @doc "Announces `stage` on `request_id`, writing the durable timeline entry and broadcasting it."
  defstep announce(request_id, stage) do
    {:ok, _event} = Approvals.record_stage(request_id, stage)
    :ok
  end

  @doc "Reads the claim amount once, so every later branch is taken against a recorded value."
  defstep load_amount(request_id) do
    Approvals.get_request!(request_id).amount_cents
  end

  @doc "Writes the final decision onto the claim and announces the terminal stage."
  defstep settle(request_id, decision, decided_by) do
    {:ok, _request} = Approvals.record_outcome(request_id, decision, decided_by)
    :ok
  end
end
