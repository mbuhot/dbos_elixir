defmodule LiveApprovalsWeb.ApprovalComponents do
  @moduledoc "Presentation shared by the three live views: navigation, money, and stage names."

  use Phoenix.Component

  use Phoenix.VerifiedRoutes,
    endpoint: LiveApprovalsWeb.Endpoint,
    router: LiveApprovalsWeb.Router,
    statics: LiveApprovalsWeb.static_paths()

  @stage_labels %{
    submitted: "Submitted",
    validating: "Validating",
    policy_check: "Policy check",
    awaiting_decision: "Awaiting decision",
    approved: "Approved",
    rejected: "Rejected"
  }

  @doc "The human name for a review stage."
  def stage_label(stage), do: Map.fetch!(@stage_labels, stage)

  @doc "Cents rendered as dollars."
  def money(cents) do
    "$" <> :erlang.float_to_binary(cents / 100, decimals: 2)
  end

  attr :current, :atom, required: true

  @doc "Links between the submitter view and the approver queue."
  def nav_bar(assigns) do
    ~H"""
    <nav class="flex gap-4 text-sm">
      <.link navigate={~p"/requests"} class={link_class(@current == :requests)}>Claims</.link>
      <.link navigate={~p"/approvals"} class={link_class(@current == :approvals)}>
        Approver queue
      </.link>
    </nav>
    """
  end

  defp link_class(true), do: "font-semibold underline"
  defp link_class(false), do: "opacity-70"
end
