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

  # The path a dev console hits after a restart: a failure recorded by a VM that had raised it,
  # read back by one that never has. Every atom in the stored term is compared as a string here,
  # since naming one as a literal would have the compiler intern it and hide the bug.
  describe "reading back a failure whose atoms this VM has never seen" do
    @absent_atom_failure "g3QAAAADdwV2YWx1ZXQAAAADdwdtZXNzYWdlbQAAAARib29tdwpfX3N0cnVjdF9fdxpFbGl4aXIuRGJvcy5BYnNlbnRSb3dFcnJvcncJY2hhbmdlc2V0dAAAAAF3Fi1yZWNvcmRfcXVvdGUvMi1mdW4tMC1sAAAAA3cPZWxpeGlyX2VybF9wYXNzdxBub19wYXJlbnNfcmVtb3RldwR2YXJzancKc3RhY2t0cmFjZWwAAAABaAR3G0VsaXhpci5EYm9zLkFic2VudFJvd01vZHVsZXclLXRyYW5zYWN0aW9uIChvdmVycmlkYWJsZSAxKS8yLWZ1bi0wLWECbAAAAAJoAncEZmlsZWsADWxpYi9hYnNlbnQuZXhoAncEbGluZWEBamp3BGtpbmR3BWVycm9y"

    setup %{config: config} do
      {:ok, workflow_id} = Client.enqueue(config, "Checkout.fails", "default", [1])

      Postgrex.query!(
        config.conn,
        ~s(UPDATE "dbos".workflow_status SET status = 'ERROR', error = $2 WHERE workflow_uuid = $1),
        [workflow_id, @absent_atom_failure]
      )

      {:ok, workflow_id: workflow_id}
    end

    test "status/2 decodes it", %{config: config, workflow_id: workflow_id} do
      {:ok, status} = Client.status(config, workflow_id)
      assert Atom.to_string(status.error.value.__struct__) == "Elixir.Dbos.AbsentRowError"
    end

    test "list/2 decodes it", %{config: config} do
      {:ok, [listed]} = Client.list(config, name: "Checkout.fails")
      assert Atom.to_string(listed.error.value.__struct__) == "Elixir.Dbos.AbsentRowError"
    end

    test "result/2 returns it as the workflow's error", %{
      config: config,
      workflow_id: workflow_id
    } do
      assert {:error, %{kind: :error, value: exception}} = Client.result(config, workflow_id)
      assert Atom.to_string(exception.__struct__) == "Elixir.Dbos.AbsentRowError"
    end
  end
end
