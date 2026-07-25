defmodule Mix.Tasks.AgentInbox.Answer do
  @shortdoc "Answers a pending human approval request"

  @moduledoc """
  Approves or rejects a pending `request_approval` workflow.

      mix agent_inbox.answer WORKFLOW_ID approve ["a note for the record"]
      mix agent_inbox.answer WORKFLOW_ID reject ["a reason"]

  Delivers the decision as a durable message on the `"approval_response"` topic. The workflow
  picks it up whether it is actively running, has parked (`Dbos.Waits`), or has not yet reached
  its wait at all — a response sent before the workflow calls `Dbos.recv_message/2` is still
  delivered, since that call checks for an already-arrived message before it starts waiting.
  """

  use Mix.Task

  @impl Mix.Task
  def run([workflow_id, decision | rest]) when decision in ["approve", "reject"] do
    Mix.Task.run("app.start")
    config = AgentInbox.Cli.config()
    note = List.first(rest)

    message =
      case decision do
        "approve" -> {:approved, note}
        "reject" -> {:rejected, note}
      end

    Dbos.send_message(workflow_id, AgentInbox.Approvals.response_topic(), message,
      engine: config.name
    )

    Mix.shell().info("Sent #{decision} for #{workflow_id}.")
  end

  def run(_args) do
    Mix.shell().error("usage: mix agent_inbox.answer WORKFLOW_ID approve|reject [\"note\"]")
  end
end
