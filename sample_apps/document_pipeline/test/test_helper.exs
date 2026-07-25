database_url =
  System.get_env("DOCUMENT_PIPELINE_TEST_DATABASE_URL") ||
    "postgres://localhost/document_pipeline_test"

%URI{path: "/" <> database_name} = URI.parse(database_url)

{_output, 0} = System.cmd("dropdb", ["--if-exists", database_name])
{_output, 0} = System.cmd("createdb", [database_name])

{:ok, _pid} = DocumentPipeline.Repo.start_link()

Ecto.Migrator.run(
  DocumentPipeline.Repo,
  Path.join(__DIR__, "../priv/repo/migrations"),
  :up,
  all: true
)

ExUnit.start()
