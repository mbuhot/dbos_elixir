defmodule LiveApprovals.Approvals do
  @moduledoc """
  The expense-claim data layer. Every write publishes a notification on `Phoenix.PubSub` before
  it returns, the way an Ash resource with a PubSub notifier does, and nothing in here knows that
  a durable workflow exists.

  Three topics carry those notifications:

  | Topic | Payload | Read by |
  |---|---|---|
  | `"approvals"` | `{:request_submitted, request}`, `{:stage_changed, request_id, event}`, `{:decision_recorded, request_id, decision, decided_by}` | the request list and the approver queue |
  | `"approvals:<id>"` | `{:stage_changed, request_id, event}`, `{:decision_recorded, request_id, decision, decided_by}` | one request's live timeline |
  | `"approvals:decisions"` | `{:decision_recorded, request_id, decision, decided_by}` | `LiveApprovals.Bridges.Inbound` |
  """

  import Ecto.Query

  alias LiveApprovals.Approvals.ExpenseRequest
  alias LiveApprovals.Approvals.RequestEvent
  alias LiveApprovals.Repo

  @pubsub LiveApprovals.PubSub
  @firehose_topic "approvals"
  @decisions_topic "approvals:decisions"

  @doc "The topic carrying every request's notifications."
  def firehose_topic, do: @firehose_topic

  @doc "The topic the inbound bridge listens on."
  def decisions_topic, do: @decisions_topic

  @doc "The topic carrying one request's notifications."
  def request_topic(request_id), do: @firehose_topic <> ":" <> request_id

  @doc "Subscribes the caller to every request's notifications."
  def subscribe_all, do: Phoenix.PubSub.subscribe(@pubsub, @firehose_topic)

  @doc "Subscribes the caller to one request's notifications."
  def subscribe(request_id), do: Phoenix.PubSub.subscribe(@pubsub, request_topic(request_id))

  @doc "Subscribes the caller to decisions recorded by human approvers."
  def subscribe_decisions, do: Phoenix.PubSub.subscribe(@pubsub, @decisions_topic)

  @doc "Inserts a claim, opens its timeline at the `:submitted` stage, and announces both."
  def submit_request(attrs) do
    with {:ok, request} <- attrs |> ExpenseRequest.submission_changeset() |> Repo.insert() do
      publish(@firehose_topic, {:request_submitted, request})
      {:ok, _event} = record_stage(request.id, :submitted)
      {:ok, request}
    end
  end

  @doc "Builds the changeset backing the submission form."
  def submission_changeset(attrs \\ %{}), do: ExpenseRequest.submission_changeset(attrs)

  @doc """
  Moves `request_id` to `stage`, appends the matching timeline entry, and announces both.
  Announcing a stage a request already reached leaves the timeline unchanged and re-announces
  the same entry.
  """
  def record_stage(request_id, stage, detail \\ nil) do
    {:ok, event} =
      Repo.transaction(fn ->
        request_id
        |> get_request!()
        |> ExpenseRequest.stage_changeset(stage)
        |> Repo.update!()

        upsert_event(request_id, stage, detail)
      end)

    publish(request_topic(request_id), {:stage_changed, request_id, event})
    publish(@firehose_topic, {:stage_changed, request_id, event})
    {:ok, event}
  end

  @doc """
  Records a human approver's `decision` on `request_id` and announces it on the decisions topic
  as well as the request and firehose topics.
  """
  def record_decision(request_id, decision, decided_by) do
    with {:ok, request} <- write_decision(request_id, decision, decided_by) do
      publish(@decisions_topic, {:decision_recorded, request_id, decision, decided_by})
      {:ok, request}
    end
  end

  @doc """
  Records the review's settled `decision` on `request_id` and moves it to the matching terminal
  stage. Announced on the request and firehose topics only, so settling never looks like a fresh
  human decision.
  """
  def record_outcome(request_id, decision, decided_by) do
    with {:ok, request} <- write_decision(request_id, decision, decided_by),
         {:ok, _event} <- record_stage(request_id, decision, "decided by #{decided_by}") do
      {:ok, request}
    end
  end

  defp write_decision(request_id, decision, decided_by) do
    with {:ok, request} <-
           request_id
           |> get_request!()
           |> ExpenseRequest.decision_changeset(decision, decided_by)
           |> Repo.update() do
      message = {:decision_recorded, request_id, decision, decided_by}

      publish(request_topic(request_id), message)
      publish(@firehose_topic, message)
      {:ok, request}
    end
  end

  @doc "Every claim, newest first."
  def list_requests do
    Repo.all(from request in ExpenseRequest, order_by: [desc: request.inserted_at])
  end

  @doc "Every claim currently parked on a human approver, oldest first."
  def list_awaiting_decision do
    Repo.all(
      from request in ExpenseRequest,
        where: request.stage == :awaiting_decision,
        order_by: [asc: request.inserted_at]
    )
  end

  @doc "One claim by id, raising if it is missing."
  def get_request!(request_id), do: Repo.get!(ExpenseRequest, request_id)

  @doc "One claim by id."
  def get_request(request_id), do: Repo.get(ExpenseRequest, request_id)

  @doc "`request_id`'s durable timeline, oldest first."
  def list_events(request_id) do
    Repo.all(
      from event in RequestEvent,
        where: event.request_id == ^request_id,
        order_by: [asc: event.id]
    )
  end

  defp upsert_event(request_id, stage, detail) do
    %RequestEvent{request_id: request_id, stage: stage, detail: detail}
    |> Repo.insert(on_conflict: :nothing, conflict_target: [:request_id, :stage])

    Repo.one!(
      from event in RequestEvent,
        where: event.request_id == ^request_id and event.stage == ^stage
    )
  end

  defp publish(topic, message), do: Phoenix.PubSub.broadcast(@pubsub, topic, message)
end
