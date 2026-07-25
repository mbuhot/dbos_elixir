defmodule Dbos.Config do
  @moduledoc """
  Configuration a `Dbos.SystemDb` call needs to reach the system database: which adapter and
  connection to use, which schema the `dbos` tables live under, and this executor's identity.
  """

  @enforce_keys [:db, :conn]
  defstruct name: Dbos,
            db: nil,
            conn: nil,
            schema: "dbos",
            executor_id: nil,
            application_version: nil,
            max_recovery_attempts: 3,
            cluster_mode: :disabled,
            cluster_group: Dbos.Cluster.Group,
            reclaim_batch_size: 50,
            orphan_sweep_interval_ms: 300_000,
            orphan_sweep_threshold_ms: 300_000,
            notifications: :listen,
            notifications_conn_opts: nil,
            scheduler_poll_interval_ms: 30_000,
            park_exit_threshold_ms: 60_000,
            park_replay_ceiling: 500

  @type cluster_mode :: :disabled | :cluster_only | :cluster_and_orphan_sweep

  @type t :: %__MODULE__{
          name: atom,
          db: module,
          conn: term,
          schema: String.t(),
          executor_id: String.t() | nil,
          application_version: String.t() | nil,
          max_recovery_attempts: non_neg_integer,
          cluster_mode: cluster_mode,
          cluster_group: atom,
          reclaim_batch_size: pos_integer,
          orphan_sweep_interval_ms: pos_integer,
          orphan_sweep_threshold_ms: pos_integer,
          notifications: :listen | :poll,
          notifications_conn_opts: keyword | nil,
          scheduler_poll_interval_ms: pos_integer,
          park_exit_threshold_ms: pos_integer | :infinity,
          park_replay_ceiling: pos_integer
        }
end
