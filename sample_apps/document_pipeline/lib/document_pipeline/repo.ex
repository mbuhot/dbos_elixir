defmodule DocumentPipeline.Repo do
  @moduledoc "Ecto repo backing both this app's `chunks` table and the Dbos engine's own checkpoints."
  use Ecto.Repo, otp_app: :document_pipeline, adapter: Ecto.Adapters.Postgres
end
