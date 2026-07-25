database = System.get_env("AGENT_INBOX_TEST_DATABASE") || "agent_inbox_test"

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])

{:ok, _pid} = Postgrex.start_link(name: AgentInbox.TestConn, database: database)

ExUnit.start()
