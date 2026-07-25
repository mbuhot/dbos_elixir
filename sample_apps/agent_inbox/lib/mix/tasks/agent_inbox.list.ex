defmodule Mix.Tasks.AgentInbox.List do
  @shortdoc "Lists pending human approval requests"

  @moduledoc """
  Lists every `request_approval` workflow currently waiting on a human decision.

      mix agent_inbox.list

  Each entry shows the workflow id (pass it to `mix agent_inbox.answer`), the subject and
  details published when the request was raised, and how long it has been waiting.
  """

  use Mix.Task

  alias Dbos.Client
  alias Dbos.SystemDb

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start")
    config = AgentInbox.Cli.config()

    case Client.list(config, name: AgentInbox.Approvals.workflow_name(), status: :pending) do
      {:ok, []} -> Mix.shell().info("No pending approvals.")
      {:ok, workflows} -> Enum.each(workflows, &print_pending(config, &1))
    end
  end

  defp print_pending(config, workflow) do
    request =
      case SystemDb.get_event_value(config, workflow.workflow_uuid, "request") do
        {:ok, value} -> value
        :none -> %{}
      end

    Mix.shell().info("""

    #{workflow.workflow_uuid}
      subject: #{inspect(Map.get(request, :subject))}
      details: #{inspect(Map.get(request, :details))}
      waiting since: #{format_epoch_ms(workflow.created_at)}\
    """)
  end

  defp format_epoch_ms(nil), do: "unknown"

  defp format_epoch_ms(epoch_ms) do
    epoch_ms
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_string()
  end
end
