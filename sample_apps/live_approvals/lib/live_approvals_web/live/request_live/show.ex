defmodule LiveApprovalsWeb.RequestLive.Show do
  @moduledoc """
  One claim's live timeline. Mount reads the durable timeline so a viewer arriving late sees
  everything that already happened; from then on the page changes only when a `Phoenix.PubSub`
  notification arrives. There is no timer and no re-query loop.
  """

  use LiveApprovalsWeb, :live_view

  alias LiveApprovals.Approvals
  alias LiveApprovals.Approvals.ExpenseRequest

  @impl true
  def mount(%{"id" => request_id}, _session, socket) do
    if connected?(socket), do: Approvals.subscribe(request_id)

    request = Approvals.get_request!(request_id)
    reached = request_id |> Approvals.list_events() |> Map.new(&{&1.stage, &1})

    {:ok,
     socket
     |> assign(:page_title, request.title)
     |> assign(:request, request)
     |> assign(:reached, reached)}
  end

  @impl true
  def handle_info({:stage_changed, _request_id, event}, socket) do
    {:noreply,
     socket
     |> assign(:reached, Map.put(socket.assigns.reached, event.stage, event))
     |> assign(:request, %{socket.assigns.request | stage: event.stage})}
  end

  @impl true
  def handle_info({:decision_recorded, _request_id, decision, decided_by}, socket) do
    request = %{socket.assigns.request | decision: decision, decided_by: decided_by}
    {:noreply, assign(socket, :request, request)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <Layouts.app flash={@flash}>
      <.nav_bar current={:requests} />

      <h1 class="text-2xl font-semibold">{@request.title}</h1>
      <p class="text-sm opacity-70">
        {money(@request.amount_cents)} claimed by {@request.submitter}
      </p>

      <h2 class="text-lg font-semibold">Review timeline</h2>
      <ol class="space-y-1">
        <li :for={stage <- timeline_stages(@request, @reached)}>
          {stage_label(stage)}: {timeline_state(@reached, stage)}{detail_suffix(@reached, stage)}
        </li>
      </ol>

      <p :if={@request.decision}>
        Decision: {stage_label(@request.decision)} by {@request.decided_by}
      </p>
      <p :if={is_nil(@request.decision)}>No decision recorded yet.</p>
    </Layouts.app>
    """
  end

  defp timeline_stages(request, reached) do
    Enum.filter(ExpenseRequest.stages(), &visible?(&1, request, reached))
  end

  defp visible?(stage, request, _reached) when stage in [:approved, :rejected] do
    request.decision in [nil, stage]
  end

  defp visible?(:awaiting_decision, request, reached) do
    Map.has_key?(reached, :awaiting_decision) or is_nil(request.decision)
  end

  defp visible?(_stage, _request, _reached), do: true

  defp timeline_state(reached, stage) do
    if Map.has_key?(reached, stage), do: "reached", else: "pending"
  end

  defp detail_suffix(reached, stage) do
    case Map.fetch(reached, stage) do
      {:ok, %{detail: detail}} when is_binary(detail) -> " (#{detail})"
      _other -> ""
    end
  end
end
