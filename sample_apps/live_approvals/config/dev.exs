import Config

config :live_approvals, LiveApprovals.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "live_approvals_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :live_approvals, LiveApprovalsWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}],
  check_origin: false,
  code_reloader: true,
  debug_errors: true,
  secret_key_base: "RV1D8D4sDu+ovp0wEs1mhemlxywemHORt+4b1U8LcpIWykqhj0IuH0Atm2ZPss+B",
  watchers: [
    esbuild: {Esbuild, :install_and_run, [:live_approvals, ~w(--sourcemap=inline --watch)]},
    tailwind: {Tailwind, :install_and_run, [:live_approvals, ~w(--watch)]}
  ]

config :logger, :default_formatter, format: "[$level] $message\n"

config :phoenix, :stacktrace_depth, 20

config :phoenix, :plug_init_mode, :runtime

config :phoenix_live_view,
  debug_heex_annotations: true,
  debug_attributes: true,
  enable_expensive_runtime_checks: true
