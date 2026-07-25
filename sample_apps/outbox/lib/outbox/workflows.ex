defmodule Outbox.Workflows do
  @moduledoc """
  The transactional outbox pattern: `place_order/3` writes the `orders` row and its
  `outbox_events` row in one Postgres transaction, so an order is never committed without the
  event that should follow it, and vice versa. `drain_outbox/2` is a separately scheduled
  workflow that finds every `"pending"` event, asks `Outbox.ExternalSystem` to publish it, and
  only then marks it `"sent"`.

  The delivery guarantee this gives the external system is **at-least-once**: if the process
  dies after `publish/2` succeeds but before `mark_event_sent/1` commits, the next drain finds
  the row still `"pending"` and publishes it again. The consumer on the other end of
  `Outbox.ExternalSystem` is responsible for idempotency (a unique event id, an upsert, a
  dedup table) — this pattern guarantees the event is never dropped, not that it arrives
  exactly once.
  """

  use Dbos, repo: Outbox.Repo

  import Ecto.Query

  alias Outbox.ExternalSystem
  alias Outbox.Order
  alias Outbox.OutboxEvent
  alias Outbox.Repo

  defworkflow place_order(customer, item, quantity), name: "place_order" do
    insert_order_with_outbox(customer, item, quantity)
  end

  defworkflow drain_outbox(_scheduled_time_ms, _context),
    name: "drain_outbox",
    schedule: [cron: "*/2 * * * * *"] do
    pending_events = list_pending_outbox_events()

    Enum.each(pending_events, fn event ->
      publish_event(event)
      mark_event_sent(event.id)
    end)

    length(pending_events)
  end

  @doc "Inserts the order and its `\"order_placed\"` outbox row in one transaction; returns the order's id."
  deftransaction insert_order_with_outbox(customer, item, quantity) do
    order =
      %Order{customer: customer, item: item, quantity: quantity}
      |> Repo.insert!()

    %OutboxEvent{
      order_id: order.id,
      event_type: "order_placed",
      payload: %{"customer" => customer, "item" => item, "quantity" => quantity},
      status: "pending"
    }
    |> Repo.insert!()

    order.id
  end

  @doc "Every currently pending outbox event, oldest first."
  defstep list_pending_outbox_events() do
    from(e in OutboxEvent,
      where: e.status == "pending",
      order_by: e.id,
      select: %{id: e.id, event_type: e.event_type, payload: e.payload}
    )
    |> Repo.all()
  end

  @doc "Publishes one outbox event to the external system, retrying on a transient failure."
  defstep publish_event(event), max_retries: 5, base_interval_ms: 200 do
    ExternalSystem.publish(event.event_type, event.payload)
  end

  @doc "Marks `event_id`'s outbox row `\"sent\"`."
  deftransaction mark_event_sent(event_id) do
    OutboxEvent
    |> Repo.get!(event_id)
    |> Ecto.Changeset.change(status: "sent")
    |> Repo.update!()
  end
end
