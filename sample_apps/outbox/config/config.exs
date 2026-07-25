import Config

config :outbox, Outbox.Repo,
  database: System.get_env("OUTBOX_DATABASE", "outbox_#{config_env()}"),
  hostname: System.get_env("PGHOST", "localhost"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  pool_size: 10

config :outbox, ecto_repos: [Outbox.Repo]

config :postgrex, :json_library, Jason
