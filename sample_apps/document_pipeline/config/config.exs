import Config

config :document_pipeline, ecto_repos: [DocumentPipeline.Repo]

database_url =
  case config_env() do
    :test ->
      System.get_env("DOCUMENT_PIPELINE_TEST_DATABASE_URL") ||
        "postgres://localhost/document_pipeline_test"

    _ ->
      System.get_env("DOCUMENT_PIPELINE_DATABASE_URL") ||
        "postgres://localhost/document_pipeline_dev"
  end

config :document_pipeline, DocumentPipeline.Repo,
  url: database_url,
  log: config_env() != :test

config :document_pipeline,
  embedder: DocumentPipeline.Embedder.Stub,
  embed_delay_ms: if(config_env() == :test, do: 100, else: 0)
