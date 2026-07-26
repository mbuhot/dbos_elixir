defmodule LiveApprovals do
  @moduledoc """
  An expense-approval app wiring Phoenix, LiveView, Ecto, `Phoenix.PubSub` and `Dbos` together.

  Two bridges connect the durable engine to the live UI: `LiveApprovals.Reviews.ReviewWorkflow`
  announces its progress through `LiveApprovals.Approvals`, which broadcasts on PubSub, and
  `LiveApprovals.Bridges.Inbound` turns an approver's PubSub notification back into a durable
  message for the review waiting on it.
  """
end
