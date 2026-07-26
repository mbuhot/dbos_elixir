defmodule LiveApprovalsWeb.RequestLiveTest do
  use LiveApprovalsWeb.ConnCase, async: false

  setup :start_review_engine

  test "the claim page fills in each stage as the review reaches it, with no reload", %{
    conn: conn,
    engine: engine
  } do
    {:ok, request} = Reviews.submit(small_claim())

    {:ok, live_view, html} = live(conn, ~p"/requests/#{request}")

    assert html =~ "Submitted: reached"
    assert html =~ "Validating: pending"
    assert html =~ "Policy check: pending"
    assert html =~ "No decision recorded yet."

    assert drain_reviews(engine) == 1

    settled = render(live_view)
    assert settled =~ "Validating: reached"
    assert settled =~ "Policy check: reached"
    assert settled =~ "Approved: reached (decided by policy-engine)"
    assert settled =~ "Decision: Approved by policy-engine"
  end

  test "a claim submitted from the list appears there and moves to its settled stage", %{
    conn: conn,
    engine: engine
  } do
    {:ok, live_view, _html} = live(conn, ~p"/requests")

    live_view
    |> form("#claim-form",
      expense_request: %{title: "Taxi to airport", amount_cents: "2500", submitter: "dana"}
    )
    |> render_submit()

    submitted = render(live_view)
    assert submitted =~ "Taxi to airport"
    assert submitted =~ "Submitted"

    assert drain_reviews(engine) == 1

    settled = render(live_view)
    assert settled =~ "Taxi to airport"
    assert settled =~ "Approved"
  end

  test "a claim page opened after the review finished shows the whole timeline", %{
    conn: conn,
    engine: engine
  } do
    {:ok, request} = Reviews.submit(small_claim())
    assert drain_reviews(engine) == 1

    {:ok, _live_view, html} = live(conn, ~p"/requests/#{request}")

    assert html =~ "Submitted: reached"
    assert html =~ "Validating: reached"
    assert html =~ "Policy check: reached"
    assert html =~ "Approved: reached (decided by policy-engine)"
  end
end
