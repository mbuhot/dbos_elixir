defmodule WidgetStore.Repo do
  @moduledoc "The storefront's own Ecto repo — the same Postgres database `Dbos` checkpoints into."

  use Ecto.Repo, otp_app: :widget_store, adapter: Ecto.Adapters.Postgres
end
