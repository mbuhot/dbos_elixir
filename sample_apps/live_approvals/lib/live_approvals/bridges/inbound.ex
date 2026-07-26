defmodule LiveApprovals.Bridges.Inbound do
  @moduledoc """
  Phoenix.PubSub → Dbos. Subscribes to `LiveApprovals.Approvals.decisions_topic/0` and turns each
  `{:decision_recorded, request_id, decision, decided_by}` notification into a durable
  `Dbos.send_message/4` addressed at the review parked on that request id.

  The notification is ordinary, in-memory PubSub; the message this writes is a row in the engine's
  `notifications` table. That row is what survives the approver's browser session, the LiveView
  process, this bridge, and the node — the parked review consumes it whenever and wherever it next
  runs.

  Start one per engine:

      {LiveApprovals.Bridges.Inbound, engine: Dbos, pubsub: LiveApprovals.PubSub}
  """

  use GenServer

  require Logger

  alias LiveApprovals.Approvals
  alias LiveApprovals.Reviews

  def start_link(opts) do
    state = %{
      engine: Keyword.get(opts, :engine, Dbos),
      pubsub: Keyword.get(opts, :pubsub, LiveApprovals.PubSub)
    }

    GenServer.start_link(__MODULE__, state, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(state) do
    :ok = Phoenix.PubSub.subscribe(state.pubsub, Approvals.decisions_topic())
    {:ok, state}
  end

  @impl true
  def handle_info({:decision_recorded, request_id, decision, decided_by}, state) do
    deliver(state.engine, request_id, decision, decided_by)
    {:noreply, state}
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  defp deliver(engine, request_id, decision, decided_by) do
    Reviews.deliver_decision(request_id, decision, decided_by, engine: engine)
  rescue
    error ->
      Logger.warning(
        "inbound bridge could not deliver #{inspect(decision)} for #{request_id}: #{Exception.message(error)}"
      )
  end
end
