defmodule LiveApprovalsWeb.ConnCase do
  @moduledoc """
  This module defines the test case to be used by
  tests that require setting up a connection.

  Such tests rely on `Phoenix.ConnTest` and also
  import other functionality to make it easier
  to build common data structures and query the data layer.

  Finally, if the test case interacts with the database,
  we enable the SQL sandbox, so changes done to the database
  are reverted at the end of every test. If you are using
  PostgreSQL, you can even run database tests asynchronously
  by setting `use LiveApprovalsWeb.ConnCase, async: true`, although
  this option is not recommended for other databases.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      @endpoint LiveApprovalsWeb.Endpoint

      use LiveApprovalsWeb, :verified_routes

      import Plug.Conn
      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
      import LiveApprovals.EngineCase
      import LiveApprovalsWeb.ConnCase

      alias LiveApprovals.Approvals
      alias LiveApprovals.Reviews
      alias LiveApprovals.Reviews.ReviewWorkflow
    end
  end

  setup tags do
    LiveApprovals.DataCase.setup_sandbox(tags)
    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  @doc "Setup hook adding a per-test `Dbos` engine to the context."
  def start_review_engine(_context) do
    {:ok, engine: LiveApprovals.EngineCase.start_engine!()}
  end
end
