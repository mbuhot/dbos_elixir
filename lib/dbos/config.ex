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
            cluster_enabled: false,
            cluster_group: Dbos.Cluster.Group,
            reclaim_batch_size: 50,
            orphan_sweep_enabled: false,
            orphan_sweep_interval_ms: 300_000,
            orphan_sweep_threshold_ms: 300_000,
            notifications: :listen,
            notifications_conn_opts: nil,
            scheduler_poll_interval_ms: 30_000

  @type t :: %__MODULE__{
          name: atom,
          db: module,
          conn: term,
          schema: String.t(),
          executor_id: String.t() | nil,
          application_version: String.t() | nil,
          max_recovery_attempts: non_neg_integer,
          cluster_enabled: boolean,
          cluster_group: atom,
          reclaim_batch_size: pos_integer,
          orphan_sweep_enabled: boolean,
          orphan_sweep_interval_ms: pos_integer,
          orphan_sweep_threshold_ms: pos_integer,
          notifications: :listen | :poll,
          notifications_conn_opts: keyword | nil,
          scheduler_poll_interval_ms: pos_integer
        }
end
