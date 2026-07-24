defmodule Dbos.Case do
  @moduledoc """
  Test case giving each test a Postgrex connection to `dbos_test`, with the `dbos` tables
  truncated between tests.
  """

  use ExUnit.CaseTemplate

  @tables ~w(
    workflow_status
    operation_outputs
    notifications
    workflow_events
    workflow_events_history
    streams
    event_dispatch_kv
    application_versions
    workflow_schedules
    queues
  )

  using do
    quote do
      import Dbos.Case
    end
  end

  setup do
    conn = Dbos.TestConn
    truncate_tables(conn)
    {:ok, conn: conn}
  end

  def truncate_tables(conn) do
    tables = Enum.map_join(@tables, ", ", &"dbos.#{&1}")
    Postgrex.query!(conn, "TRUNCATE TABLE #{tables} CASCADE", [])
  end
end
