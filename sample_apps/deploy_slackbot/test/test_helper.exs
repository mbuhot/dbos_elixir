database = System.get_env("DEPLOY_SLACKBOT_DATABASE") || "deploy_slackbot_test"
System.cmd("createdb", [database], stderr_to_stdout: true)

{:ok, _pid} = Postgrex.start_link(name: DeploySlackbot.TestConn, database: database)

config = %Dbos.Config{
  name: :deploy_slackbot_bootstrap,
  db: Dbos.DB.Postgrex,
  conn: DeploySlackbot.TestConn
}

try do
  Dbos.Migrator.verify!(config)
rescue
  _error -> Dbos.Migrator.create!(config)
end

ExUnit.start()
