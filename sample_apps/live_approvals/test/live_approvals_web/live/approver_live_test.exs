defmodule LiveApprovalsWeb.ApproverLiveTest do
  use LiveApprovalsWeb.ConnCase, async: false

  setup :start_review_engine

  setup %{engine: engine} do
    start_inbound_bridge!(engine)
    :ok
  end

  defp park_on_approver(engine) do
    {:ok, request} = Reviews.submit(large_claim())
    assert drain_reviews(engine) == 1
    assert Approvals.get_request!(request.id).stage == :awaiting_decision
    request
  end

  defp resume_review(engine, request) do
    await_decision_delivered(engine, request.id)
    simulate_lost_process(engine, request.id)
    assert Dbos.Testing.recover_pending(engine: engine) == 1
    assert drain_reviews(engine) == 1
  end

  test "the approver queue lists the claims parked on a human", %{conn: conn, engine: engine} do
    request = park_on_approver(engine)

    {:ok, _live_view, html} = live(conn, ~p"/approvals")

    assert html =~ request.title
    assert html =~ "$450.00"
    assert html =~ "dana"
  end

  test "approving a parked claim settles it under the approver's name", %{
    conn: conn,
    engine: engine
  } do
    request = park_on_approver(engine)

    {:ok, queue, _html} = live(conn, ~p"/approvals")
    queue |> element("button", "Approve") |> render_click()

    resume_review(engine, request)

    settled = Approvals.get_request!(request.id)
    assert settled.stage == :approved
    assert settled.decided_by == "approver"
  end

  test "rejecting a parked claim settles it as rejected", %{conn: conn, engine: engine} do
    request = park_on_approver(engine)

    {:ok, queue, _html} = live(conn, ~p"/approvals")
    queue |> element("button", "Reject") |> render_click()

    resume_review(engine, request)

    assert Approvals.get_request!(request.id).stage == :rejected
  end

  test "a decision taken in the approver session reaches the submitter's open claim page", %{
    conn: conn,
    engine: engine
  } do
    request = park_on_approver(engine)

    {:ok, claim_page, html} = live(conn, ~p"/requests/#{request}")
    assert html =~ "Awaiting decision: reached"
    assert html =~ "No decision recorded yet."

    {:ok, queue, _queue_html} = live(conn, ~p"/approvals")
    queue |> element("button", "Approve") |> render_click()

    assert render(claim_page) =~ "Decision: Approved by approver"

    resume_review(engine, request)

    assert render(claim_page) =~ "Approved: reached (decided by approver)"
  end

  test "a claim the approver has answered leaves the queue", %{conn: conn, engine: engine} do
    request = park_on_approver(engine)

    {:ok, queue, _html} = live(conn, ~p"/approvals")
    queue |> element("button", "Approve") |> render_click()

    resume_review(engine, request)

    refute render(queue) =~ request.title
    assert render(queue) =~ "Nothing is waiting on an approver."
  end
end
