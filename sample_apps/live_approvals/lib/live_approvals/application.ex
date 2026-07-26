defmodule LiveApprovals.Application do
  @moduledoc """
  Boot order matters here: the repo comes up first, then `Phoenix.PubSub`, then the `Dbos` engine
  sharing the repo's pool, then the inbound bridge that needs both PubSub and a running engine,
  and finally the endpoint.
  """

  use Application

  alias LiveApprovals.Reviews.ReviewWorkflow

  @impl true
  def start(_type, _args) do
    children =
      [
        LiveApprovalsWeb.Telemetry,
        LiveApprovals.Repo,
        {DNSCluster, query: Application.get_env(:live_approvals, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: LiveApprovals.PubSub}
      ] ++ engine_children() ++ [LiveApprovalsWeb.Endpoint]

    opts = [strategy: :one_for_one, name: LiveApprovals.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def config_change(changed, _new, removed) do
    LiveApprovalsWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp engine_children do
    if Application.get_env(:live_approvals, :start_engine, true) do
      [
        {Dbos.Supervisor,
         name: Dbos,
         db: {Dbos.DB.Ecto, LiveApprovals.Repo},
         otp_app: :live_approvals,
         queues: [Dbos.Queue.new(ReviewWorkflow.queue_name(), worker_concurrency: 5)],
         migrations: :verify},
        {LiveApprovals.Bridges.Inbound, engine: Dbos, pubsub: LiveApprovals.PubSub}
      ]
    else
      []
    end
  end
end
