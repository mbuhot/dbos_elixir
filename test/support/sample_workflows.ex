defmodule Dbos.SampleWorkflows do
  @moduledoc "Plain functions used as workflow bodies in tests, so they resolve through `Function.info/1`."

  def sleep_forever(_arg) do
    receive do
      :stop -> :stopped
    end
  end

  def add(a, b), do: a + b

  def boom(_arg), do: raise("boom")

  def crash_self(_arg), do: Process.exit(self(), :kill)

  def three_steps(order_id) do
    Dbos.Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: order_id} end)
    Dbos.Runtime.run_step("charge_card/1", [], fn -> %{charged: order_id} end)
    Dbos.Runtime.run_step("ship_order/1", [], fn -> %{shipped: order_id} end)
  end

  def raises_declined(_order_id) do
    Dbos.Runtime.run_step("charge_card/1", [], fn ->
      raise Dbos.SampleWorkflows.CardDeclinedError, amount: 4999
    end)
  end

  def blocking_workflow(_arg) do
    Dbos.Runtime.run_step("count_once/0", [], fn -> bump_counter() end)

    Dbos.Runtime.run_step("wait_for_go/0", [], fn ->
      receive do
        :go -> :done
      end
    end)
  end

  def spawn_child(_arg) do
    {:ok, handle} = Dbos.start("add/2", [1, 2])
    {:ok, result} = Dbos.await(handle)
    result
  end

  def receiver(topic, timeout_ms), do: Dbos.recv_message(topic, timeout_ms)

  def sleeper(ms) do
    Dbos.sleep(ms)
    :woke
  end

  def cancellable_recv(topic, timeout_ms), do: Dbos.recv_message(topic, timeout_ms)

  def cancellable_sleep(ms) do
    Dbos.sleep(ms)
    :woke
  end

  def multi_step_with_gate(table) do
    Dbos.Runtime.run_step("first_step/0", [], fn -> :ok end)

    Dbos.Runtime.run_step("wait_for_gate/0", [], fn ->
      receive do
        :go -> :ok
      end
    end)

    :ets.insert(table, {:gate_seen, Dbos.Runtime.current_workflow_id()})

    Dbos.Runtime.run_step("second_step/0", [], fn -> :ok end)
    Dbos.Runtime.run_step("third_step/0", [], fn -> :ok end)
  end

  def timeout_workflow(ms) do
    Dbos.sleep(ms)
    :never_gets_here
  end

  def parent_with_timeout_child(_arg) do
    {:ok, handle} = Dbos.start("child_deadline_probe/0", [])
    {:ok, deadline_epoch_ms} = Dbos.await(handle, timeout_ms: 30_000)
    deadline_epoch_ms
  end

  def child_deadline_probe do
    Dbos.Runtime.current_deadline_epoch_ms()
  end

  def transactional_insert(table, workflow_conn_mod, user_id) do
    Dbos.transaction("insert_user/2", [], fn conn ->
      workflow_conn_mod.query!(conn, "INSERT INTO #{table} (id) VALUES ($1)", [user_id])
      user_id
    end)
  end

  def transactional_insert_then_raise(table, workflow_conn_mod, user_id) do
    Dbos.transaction("insert_user_then_raise/2", [], fn conn ->
      workflow_conn_mod.query!(conn, "INSERT INTO #{table} (id) VALUES ($1)", [user_id])
      raise "boom"
    end)
  end

  def transaction_in_transaction do
    Dbos.transaction("outer_tx/0", [], fn _conn ->
      Dbos.transaction("inner_tx/0", [], fn _conn2 -> :ok end)
    end)
  end

  def step_in_transaction do
    Dbos.transaction("outer_tx/0", [], fn _conn ->
      Dbos.Runtime.run_step("inner_step/0", [], fn -> :ok end)
    end)
  end

  def transaction_in_step do
    Dbos.Runtime.run_step("outer_step/0", [], fn ->
      Dbos.transaction("inner_tx/0", [], fn _conn -> :ok end)
    end)
  end

  def transaction_isolation_probe(_table, workflow_conn_mod) do
    Dbos.transaction("isolation_probe/1", [isolation: :serializable], fn conn ->
      %{rows: [[level]]} = workflow_conn_mod.query!(conn, "SHOW transaction_isolation", [])
      level
    end)
  end

  def step_id_layout_workflow(target_workflow_id) do
    msg = Dbos.recv_message("topic", 5_000)
    event_value = Dbos.get_event(target_workflow_id, "key", 5_000)
    Dbos.set_event("status", :done)
    Dbos.write_stream("log", :entry)
    plain = Dbos.Runtime.run_step("plain_step/0", [], fn -> :ok end)
    {msg, event_value, plain}
  end

  @doc """
  Runs holding a concurrency slot on `table` (a pre-created public named ETS table with a
  `{:hold_ms, integer}` entry): bumps `:running`, records the high-water mark into `:max`,
  appends a `{{:event, seq}, workflow_id, monotonic_ms}` row, sleeps `:hold_ms`, then releases
  the slot. Used by queue concurrency/rate-limit/priority acceptance tests.
  """
  def instrumented(table) do
    running = :ets.update_counter(table, :running, {2, 1}, {:running, 0})
    bump_max(table, running)
    seq = :ets.update_counter(table, :seq, {2, 1}, {:seq, 0})

    :ets.insert(
      table,
      {{:event, seq}, Dbos.Runtime.current_workflow_id(), System.monotonic_time(:millisecond)}
    )

    hold_ms = :ets.lookup_element(table, :hold_ms, 2)
    Process.sleep(hold_ms)
    :ets.update_counter(table, :running, {2, -1}, {:running, 0})
    seq
  end

  defp bump_max(table, running) do
    case :ets.lookup(table, :max) do
      [{:max, current}] when current >= running -> :ok
      _ -> :ets.insert(table, {:max, running})
    end
  end

  defp bump_counter do
    key = {__MODULE__, :counter, Dbos.Runtime.current_workflow_id()}
    count = :persistent_term.get(key, 0) + 1
    :persistent_term.put(key, count)
    count
  end

  defmodule CardDeclinedError do
    defexception [:amount]

    @impl true
    def message(%__MODULE__{amount: amount}), do: "card declined for #{amount}"
  end
end
