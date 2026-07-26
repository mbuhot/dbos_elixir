import Config

config :agent_inbox, AgentInbox.Repo,
  database: System.get_env("AGENT_INBOX_DATABASE", "agent_inbox_#{config_env()}"),
  hostname: System.get_env("PGHOST", "localhost"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  pool_size: 10

config :agent_inbox, ecto_repos: [AgentInbox.Repo]
