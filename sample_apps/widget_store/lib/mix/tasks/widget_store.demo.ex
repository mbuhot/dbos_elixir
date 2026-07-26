defmodule Mix.Tasks.WidgetStore.Demo do
  @moduledoc """
  Drives the fault-tolerant checkout end to end, in two halves that simulate a crash between
  them.

      mix widget_store.demo start    # seeds a product, starts a checkout, then hard-crashes
      mix widget_store.demo confirm  # restarts (recovery resumes the checkout), then pays

  Run `start`, watch it print that inventory was reserved and the charge was initiated, then
  watch the BEAM go down via `System.halt/1` — a real crash, not a graceful shutdown. Run
  `confirm` next: `Dbos.Recovery` replays the checkout from its last checkpoint before this task
  does anything else, `reserve_and_create_order` and `charge_customer` return their recorded
  outputs instead of running again, and only then does the payment confirmation arrive and let
  the workflow finish.
  """

  use Mix.Task

  @order_id "demo-order-1"
  @product_id "widget-1"
  @quantity 2

  @impl Mix.Task
  def run(["start"]) do
    boot()
    seed_product()

    {:ok, _handle} =
      WidgetStore.Checkout.checkout(@order_id, @product_id, @quantity, workflow_id: @order_id)

    Process.sleep(1_000)

    IO.puts("""

    Inventory reserved and the order recorded; the payment gateway was asked to charge the
    customer. The checkout workflow is now durably waiting for a payment confirmation that has
    not arrived yet.

    Crashing the BEAM now with System.halt/1 to simulate the process dying mid-checkout...
    """)

    System.halt(1)
  end

  def run(["confirm"]) do
    boot()

    IO.puts("Waiting for Dbos.Recovery to finish replaying every PENDING workflow...")
    Dbos.Recovery.await_boot_recovery(Dbos)

    IO.puts("Recovery finished. Sending the payment confirmation the checkout is waiting on...")
    Dbos.send_message(@order_id, "payment", :paid)

    {:ok, result} = Dbos.await(%Dbos.WorkflowHandle{engine: Dbos, workflow_id: @order_id})
    IO.inspect(result, label: "checkout result")
  end

  defp boot do
    Mix.Task.run("app.start")
    Ecto.Migrator.run(WidgetStore.Repo, migrations_path(), :up, all: true)
  end

  defp migrations_path, do: Application.app_dir(:widget_store, "priv/repo/migrations")

  defp seed_product do
    %WidgetStore.Product{product_id: @product_id, name: "Widget", inventory: 10}
    |> WidgetStore.Repo.insert!(on_conflict: :nothing, conflict_target: :product_id)
  end
end
