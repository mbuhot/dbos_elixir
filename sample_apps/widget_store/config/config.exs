import Config

config :widget_store, WidgetStore.Repo,
  database: System.get_env("WIDGET_STORE_DATABASE", "widget_store_#{config_env()}"),
  hostname: System.get_env("PGHOST", "localhost"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  pool_size: 10

config :widget_store, ecto_repos: [WidgetStore.Repo]
