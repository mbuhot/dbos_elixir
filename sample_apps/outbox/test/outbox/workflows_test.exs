defmodule Outbox.WorkflowsTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Outbox.ExternalSystem
  alias Outbox.Order
  alias Outbox.OutboxEvent
  alias Outbox.Repo

  setup do
    Repo.delete_all(OutboxEvent)
    Repo.delete_all(Order)

    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Ecto, Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Outbox.Workflows],
       migrations: :create_if_absent}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)
    ExternalSystem.reset!()

    :ok
  end

  test "placing an order never commits the order without its outbox event" do
    {:ok, handle} =
      Outbox.Workflows.place_order("alice", "widget", 3)

    {:ok, order_id} = Dbos.await(handle)

    order = Repo.get!(Order, order_id)
    assert %Order{customer: "alice", item: "widget", quantity: 3} = order

    events = Repo.all(from e in OutboxEvent, where: e.order_id == ^order_id)
    assert [%OutboxEvent{event_type: "order_placed", status: "pending"}] = events
  end

  test "a publish failure is retried within the same drain, not dropped" do
    {:ok, place_handle} = Outbox.Workflows.place_order("bob", "gadget", 1)
    {:ok, order_id} = Dbos.await(place_handle)

    ExternalSystem.fail_next("order_placed")

    {:ok, drain_handle} =
      Outbox.Workflows.drain_outbox(System.os_time(:millisecond), nil)

    {:ok, drained_count} = Dbos.await(drain_handle)
    assert drained_count == 1

    assert ExternalSystem.attempts("order_placed") == 2

    event = Repo.get_by!(OutboxEvent, order_id: order_id)
    assert event.status == "sent"
  end

  test "draining twice never re-sends an already-sent event" do
    {:ok, place_handle} = Outbox.Workflows.place_order("carol", "gizmo", 2)
    {:ok, order_id} = Dbos.await(place_handle)

    {:ok, first_drain} =
      Outbox.Workflows.drain_outbox(System.os_time(:millisecond), nil)

    {:ok, 1} = Dbos.await(first_drain)
    assert ExternalSystem.attempts("order_placed") == 1

    {:ok, second_drain} =
      Outbox.Workflows.drain_outbox(System.os_time(:millisecond), nil)

    {:ok, 0} = Dbos.await(second_drain)
    assert ExternalSystem.attempts("order_placed") == 1

    event = Repo.get_by!(OutboxEvent, order_id: order_id)
    assert event.status == "sent"
  end
end
