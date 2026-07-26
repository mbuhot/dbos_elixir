defmodule LiveApprovalsWeb.ApproverLive.Index do
  @moduledoc """
  The approver's view, meant to be opened in a second browser session. Approving or rejecting
  writes through `LiveApprovals.Approvals.record_decision/3` and nothing else — this page never
  touches `Dbos`. `LiveApprovals.Bridges.Inbound` picks the notification up and hands it to the
  parked review.
  """

  use LiveApprovalsWeb, :live_view

  alias LiveApprovals.Approvals

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: Approvals.subscribe_all()

    {:ok,
     socket
     |> assign(:page_title, "Approver queue")
     |> assign(:approver, "approver")
     |> assign(:queue, Approvals.list_awaiting_decision())}
  end

  @impl true
  def handle_event("set-approver", %{"approver" => approver}, socket) do
    {:noreply, assign(socket, :approver, approver)}
  end

  @impl true
  def handle_event("decide", %{"id" => request_id, "decision" => decision}, socket) do
    {:ok, _request} =
      Approvals.record_decision(
        request_id,
        String.to_existing_atom(decision),
        socket.assigns.approver
      )

    {:noreply, put_flash(socket, :info, "Decision sent to the review.")}
  end

  @impl true
  def handle_info({:stage_changed, _request_id, _event}, socket) do
    {:noreply, assign(socket, :queue, Approvals.list_awaiting_decision())}
  end

  @impl true
  def handle_info({:decision_recorded, _request_id, _decision, _decided_by}, socket) do
    {:noreply, socket}
  end

  @impl true
  def handle_info({:request_submitted, _request}, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <.nav_bar current={:approvals} />

      <h1 class="text-2xl font-semibold">Approver queue</h1>
      <p class="text-sm opacity-70">
        Each claim here is a review durably parked on a decision. Answering one resumes it.
      </p>

      <form id="approver-form" phx-change="set-approver">
        <input
          type="text"
          name="approver"
          value={@approver}
          aria-label="Approver name"
          class="input input-bordered"
        />
      </form>

      <ul class="space-y-3">
        <li :for={request <- @queue} class="flex items-center gap-3">
          <span class="flex-1">
            {request.title} — {money(request.amount_cents)} from {request.submitter}
          </span>
          <button
            type="button"
            phx-click="decide"
            phx-value-id={request.id}
            phx-value-decision="approved"
            class="btn btn-primary btn-sm"
          >
            Approve
          </button>
          <button
            type="button"
            phx-click="decide"
            phx-value-id={request.id}
            phx-value-decision="rejected"
            class="btn btn-sm"
          >
            Reject
          </button>
        </li>
      </ul>

      <p :if={@queue == []}>Nothing is waiting on an approver.</p>
    </Layouts.app>
    """
  end
end
