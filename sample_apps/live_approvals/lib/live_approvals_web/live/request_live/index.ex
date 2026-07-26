defmodule LiveApprovalsWeb.RequestLive.Index do
  @moduledoc """
  The submitter's view: file a claim, then watch every claim's stage move on its own. The list is
  read once on mount and thereafter only ever changed by a `Phoenix.PubSub` notification.
  """

  use LiveApprovalsWeb, :live_view

  alias LiveApprovals.Approvals
  alias LiveApprovals.Reviews
  alias LiveApprovals.Reviews.ReviewWorkflow

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket), do: Approvals.subscribe_all()

    {:ok,
     socket
     |> assign(:page_title, "Expense claims")
     |> assign(:ceiling, ReviewWorkflow.policy_ceiling_cents())
     |> assign(:form, to_form(Approvals.submission_changeset()))
     |> assign(:requests, Approvals.list_requests())}
  end

  @impl true
  def handle_event("validate", %{"expense_request" => params}, socket) do
    changeset = Map.put(Approvals.submission_changeset(params), :action, :validate)
    {:noreply, assign(socket, :form, to_form(changeset))}
  end

  @impl true
  def handle_event("submit", %{"expense_request" => params}, socket) do
    case Reviews.submit(params) do
      {:ok, _request} ->
        {:noreply,
         socket
         |> put_flash(:info, "Claim submitted for review.")
         |> assign(:form, to_form(Approvals.submission_changeset()))}

      {:error, changeset} ->
        {:noreply, assign(socket, :form, to_form(changeset))}
    end
  end

  @impl true
  def handle_info({:request_submitted, request}, socket) do
    {:noreply, assign(socket, :requests, [request | socket.assigns.requests])}
  end

  @impl true
  def handle_info({:stage_changed, request_id, _event}, socket) do
    {:noreply, refresh(socket, request_id)}
  end

  @impl true
  def handle_info({:decision_recorded, request_id, _decision, _decided_by}, socket) do
    {:noreply, refresh(socket, request_id)}
  end

  defp refresh(socket, request_id) do
    case Approvals.get_request(request_id) do
      nil -> socket
      request -> assign(socket, :requests, replace(socket.assigns.requests, request))
    end
  end

  defp replace(requests, request) do
    Enum.map(requests, fn existing ->
      if existing.id == request.id, do: request, else: existing
    end)
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <.nav_bar current={:requests} />

      <h1 class="text-2xl font-semibold">Expense claims</h1>
      <p class="text-sm opacity-70">
        Claims of {money(@ceiling)} or less clear the policy engine on their own. Anything larger
        parks on the approver queue until a human answers.
      </p>

      <.form
        for={@form}
        id="claim-form"
        phx-change="validate"
        phx-submit="submit"
        class="grid gap-2 sm:grid-cols-4"
      >
        <div class="sm:col-span-2">
          <.input field={@form[:title]} type="text" label="Title" />
        </div>
        <.input field={@form[:amount_cents]} type="number" label="Amount in cents" />
        <.input field={@form[:submitter]} type="text" label="Submitter" />
        <button type="submit" class="btn btn-primary sm:col-span-4">Submit claim</button>
      </.form>

      <table class="table">
        <thead>
          <tr>
            <th>Claim</th>
            <th>Amount</th>
            <th>Submitter</th>
            <th>Stage</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={request <- @requests}>
            <td><.link navigate={~p"/requests/#{request}"}>{request.title}</.link></td>
            <td>{money(request.amount_cents)}</td>
            <td>{request.submitter}</td>
            <td>{stage_label(request.stage)}</td>
          </tr>
          <tr :if={@requests == []}>
            <td colspan="4">No claims yet.</td>
          </tr>
        </tbody>
      </table>
    </Layouts.app>
    """
  end
end
