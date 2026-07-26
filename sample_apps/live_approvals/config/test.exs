import Config

config :live_approvals, start_engine: false

config :live_approvals, LiveApprovals.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "live_approvals_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2

config :live_approvals, LiveApprovalsWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "hQY10qjlzkPvBWOvm2DFdtciqyXxw+pKb6tzOXZ66d5uvIgut5+tDOiUGJoKz334",
  server: false

config :logger, level: :warning

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  enable_expensive_runtime_checks: true

config :phoenix,
  sort_verified_routes_query_params: true
