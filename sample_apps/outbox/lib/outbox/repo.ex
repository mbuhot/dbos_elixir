defmodule Outbox.Repo do
  @moduledoc "The order service's own Ecto repo — the same Postgres database `Dbos` checkpoints into."

  use Ecto.Repo, otp_app: :outbox, adapter: Ecto.Adapters.Postgres
end
