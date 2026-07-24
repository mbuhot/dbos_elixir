defmodule Dbos.ClientTest do
  use Dbos.Case, async: false

  alias Dbos.Client

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn}
    {:ok, config: config}
  end

  test "enqueue generates a UUIDv4 workflow id when none is given", %{config: config} do
    {:ok, workflow_id} = Client.enqueue(config, "Checkout.process_order", "default", [1])

    assert workflow_id =~
             ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
  end

  test "status, list, steps, and result delegate to SystemDb", %{config: config} do
    {:ok, workflow_id} = Client.enqueue(config, "Checkout.process_order", "default", [1])

    {:ok, status} = Client.status(config, workflow_id)
    assert status.workflow_uuid == workflow_id

    {:ok, [listed]} = Client.list(config, name: "Checkout.process_order")
    assert listed.workflow_uuid == workflow_id

    {:ok, steps} = Client.steps(config, workflow_id)
    assert steps == []

    assert Client.result(config, workflow_id) == :pending
  end
end
