defmodule AgentInbox.Approvals do
  @moduledoc """
  A human-in-the-loop approval workflow: publishes a pending request for a human to see, then
  waits durably for their decision — hours or days, if that's how long it takes.

  The wait is genuinely long-lived. `Dbos.recv_message/2` blocked for longer than the engine's
  `park_exit_threshold_ms` (a `Dbos.Supervisor` option, default one minute) gives up its BEAM
  process entirely, leaving one ETS row and a timer behind (`Dbos.Waits`). A timer firing, or the
  human's answer arriving, redispatches the workflow, replayed from its checkpoints back to the
  wait site. Thousands of pending approvals, each waiting days, cost a few dozen bytes apiece —
  not a supervised process each.

  `request_approval/5` requests human approval for `subject`/`details`, waiting up to
  `timeout_ms` for a response. Returns `{:ok, {:approved, note}}`, `{:ok, {:rejected, reason}}`,
  or `:expired`. `pre_wait_ms` durably sleeps before waiting for the response — real callers pass
  `0`; a test passes something larger to create a deterministic window in which to send the
  response *before* this workflow reaches its wait, proving that race is resolved correctly.

  `pre_wait_ms` takes no default: workflows started via `Dbos.start/3` by their registered name
  (as opposed to calling the generated dispatcher function directly) are invoked with exactly the
  argument list given — Elixir's own default-argument handling is a callsite-expansion feature of
  the dispatcher, and does not run on that path.
  """

  use Dbos
  require Logger

  @doc "This workflow's registered name — the value to filter `Dbos.Client.list/2` on to find pending approvals."
  def workflow_name, do: "request_approval"

  @doc "The topic a human's decision is delivered on."
  def response_topic, do: "approval_response"

  defworkflow request_approval(request_id, subject, details, timeout_ms, pre_wait_ms),
    name: "request_approval" do
    Dbos.set_event("request", %{request_id: request_id, subject: subject, details: details})
    Dbos.set_event("state", :awaiting_response)

    if pre_wait_ms > 0 do
      Dbos.sleep(pre_wait_ms)
    end

    outcome =
      try do
        {:ok, Dbos.recv_message(response_topic(), timeout_ms)}
      rescue
        Dbos.RecvTimeoutError -> :expired
      end

    case outcome do
      :expired -> escalate(request_id, subject)
      _ok -> :ok
    end

    Dbos.set_event("state", outcome_state(outcome))
    outcome
  end

  @doc "Logs an expired approval as escalated. A real app would page someone or open a ticket here."
  defstep escalate(request_id, subject) do
    Logger.warning(
      "agent_inbox: approval #{inspect(request_id)} (#{inspect(subject)}) expired with no " <>
        "response; escalating"
    )

    :escalated
  end

  defp outcome_state({:ok, {:approved, _note}}), do: :approved
  defp outcome_state({:ok, {:rejected, _reason}}), do: :rejected
  defp outcome_state(:expired), do: :expired
end
