defmodule LiveApprovals.ReviewsTest do
  use LiveApprovals.EngineCase, async: false

  setup %{engine: engine} do
    start_inbound_bridge!(engine)
    :ok
  end

  test "a claim inside the policy ceiling is settled without an approver seeing it", %{
    engine: engine
  } do
    {:ok, request} = Reviews.submit(small_claim())

    assert drain_reviews(engine) == 1

    settled = Approvals.get_request!(request.id)
    assert settled.stage == :approved
    assert settled.decision == :approved
    assert settled.decided_by == "policy-engine"
    assert Approvals.list_awaiting_decision() == []
  end

  test "a claim above the policy ceiling is settled by the approver who answered it", %{
    engine: engine
  } do
    {:ok, request} = Reviews.submit(large_claim())

    {:ok, _decided} = Approvals.record_decision(request.id, :approved, "sam")
    await_decision_delivered(engine, request.id)

    assert drain_reviews(engine) == 1

    settled = Approvals.get_request!(request.id)
    assert settled.stage == :approved
    assert settled.decided_by == "sam"
  end

  test "a rejection from an approver settles the claim as rejected", %{engine: engine} do
    {:ok, request} = Reviews.submit(large_claim())

    {:ok, _decided} = Approvals.record_decision(request.id, :rejected, "sam")
    await_decision_delivered(engine, request.id)

    assert drain_reviews(engine) == 1
    assert Approvals.get_request!(request.id).stage == :rejected
  end

  test "an answer given before the review has ever run is still waiting for it", %{engine: engine} do
    {:ok, request} = Reviews.submit(large_claim())

    assert {:ok, %Dbos.WorkflowStatus{status: :enqueued}} =
             Reviews.review_status(request.id, engine: engine)

    {:ok, _decided} = Approvals.record_decision(request.id, :approved, "sam")
    await_decision_delivered(engine, request.id)

    assert {:ok, %Dbos.WorkflowStatus{status: :enqueued}} =
             Reviews.review_status(request.id, engine: engine)

    assert Approvals.get_request!(request.id).stage == :submitted
  end

  test "an answer that arrives while the review is not running finishes it when it comes back", %{
    engine: engine
  } do
    {:ok, request} = Reviews.submit(large_claim())

    assert drain_reviews(engine) == 1
    assert Approvals.get_request!(request.id).stage == :awaiting_decision

    {:ok, _decided} = Approvals.record_decision(request.id, :approved, "sam")
    await_decision_delivered(engine, request.id)

    simulate_lost_process(engine, request.id)
    assert Dbos.Testing.recover_pending(engine: engine) == 1
    assert drain_reviews(engine) == 1

    settled = Approvals.get_request!(request.id)
    assert settled.stage == :approved
    assert settled.decided_by == "sam"

    assert {:ok, %Dbos.WorkflowStatus{status: :success}} =
             Reviews.review_status(request.id, engine: engine)
  end

  test "replaying a settled review announces none of its stages a second time", %{engine: engine} do
    {:ok, request} = Reviews.submit(small_claim())
    assert drain_reviews(engine) == 1

    Approvals.subscribe(request.id)
    simulate_lost_process(engine, request.id)
    assert Dbos.Testing.recover_pending(engine: engine) == 1
    assert drain_reviews(engine) == 1

    refute_received {:stage_changed, _request_id, _event}
    refute_received {:decision_recorded, _request_id, _decision, _decided_by}

    stages = Enum.map(Approvals.list_events(request.id), & &1.stage)
    assert stages == [:submitted, :validating, :policy_check, :approved]
  end
end
