import Config

config :queue_patterns, QueuePatterns.Repo,
  database: System.get_env("QUEUE_PATTERNS_DATABASE", "queue_patterns_#{config_env()}"),
  hostname: System.get_env("PGHOST", "localhost"),
  port: System.get_env("PGPORT", "5432") |> String.to_integer(),
  username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
  password: System.get_env("PGPASSWORD"),
  pool_size: 10

config :queue_patterns, ecto_repos: [QueuePatterns.Repo]
