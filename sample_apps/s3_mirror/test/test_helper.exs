database = System.get_env("S3_MIRROR_DATABASE") || "s3_mirror_test"
System.cmd("createdb", [database], stderr_to_stdout: true)

{:ok, _pid} = Postgrex.start_link(name: S3Mirror.TestConn, database: database)

config = %Dbos.Config{name: :s3_mirror_bootstrap, db: Dbos.DB.Postgrex, conn: S3Mirror.TestConn}

try do
  Dbos.Migrator.verify!(config)
rescue
  _error -> Dbos.Migrator.create!(config)
end

ExUnit.start()
