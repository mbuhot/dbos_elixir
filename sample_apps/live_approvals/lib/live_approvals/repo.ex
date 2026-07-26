defmodule LiveApprovals.Repo do
  use Ecto.Repo,
    otp_app: :live_approvals,
    adapter: Ecto.Adapters.Postgres
end
