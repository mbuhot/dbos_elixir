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

  @doc "Bumps `table`'s `:count` entry once per invocation, for asserting a workflow body ran exactly once."
  def counting_workflow(table) do
    :ets.update_counter(table, :count, {2, 1}, {:count, 0})
  end

  @doc """
  Starts a `counting_workflow/1` child then kills itself, but only the first time it runs: a
  replay after recovery finds the same child (already recorded) and awaits it instead, for
  crash-and-recover tests.
  """
  def spawn_child_and_die(table) do
    {:ok, handle} = Dbos.start("counting_child/1", [table])

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> Dbos.await(handle)
    end
  end

  @doc """
  Registers its own pid under a fresh `{:pid, tag}` slot in `table`, then checkpoints step 0
  directly (bypassing the check-first replay path) with `tag` as the output, and blocks on
  `:go`. Two concurrent executions of the same workflow id race to checkpoint the same step;
  the loser observes `Dbos.ConcurrentCheckpointConflictError` before ever reaching the receive.
  """
  def gated_racing_step(table) do
    tag = :ets.update_counter(table, :next_tag, 1, {:next_tag, -1})
    :ets.insert(table, {{:pid, tag}, self()})

    config = Dbos.Runtime.current_config()
    workflow_id = Dbos.Runtime.current_workflow_id()
    now = System.os_time(:millisecond)

    Dbos.SystemDb.record_operation_result(config, %{
      workflow_id: workflow_id,
      function_id: 0,
      function_name: "gated_racing_step/1",
      output: Dbos.Serialization.encode(tag),
      started_at: now,
      completed_at: now
    })

    receive do
      :go -> :ok
    end
  end

  def spawn_child_then_step(_arg) do
    {:ok, handle} = Dbos.start("add/2", [1, 2])
    {:ok, result} = Dbos.await(handle)
    extra = Dbos.Runtime.run_step("plain_step/0", [], fn -> :ok end)
    {result, extra}
  end

  def spawn_child_then_gated_step(table) do
    {:ok, handle} = Dbos.start("add/2", [1, 2])
    {:ok, result} = Dbos.await(handle)
    :ets.insert(table, {:got_result, result})

    Dbos.Runtime.run_step("wait_for_gate/0", [], fn ->
      receive do
        :go -> :ok
      end
    end)

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

  def event_waiter(target_workflow_id, key, timeout_ms),
    do: Dbos.get_event(target_workflow_id, key, timeout_ms)

  @doc """
  Cancels `target_workflow_id`, then dies, but only the first time it runs: a replay after
  recovery finds the cancel already checkpointed and does not cancel a second time, for
  crash-and-recover idempotency tests.
  """
  def cancel_and_die(table, target_workflow_id) do
    :ok = Dbos.cancel(target_workflow_id)

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> :done
    end
  end

  @doc """
  Resumes `target_workflow_id`, then dies, but only the first time it runs: a replay after
  recovery finds the resume already checkpointed and does not resume a second time, for
  crash-and-recover idempotency tests.
  """
  def resume_and_die(table, target_workflow_id) do
    :ok = Dbos.resume(target_workflow_id)

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> :done
    end
  end

  @doc "Blocks in a plain `receive`, so a cancellation only changes its durable status, not its live process."
  def blocks_forever(_arg) do
    receive do
      :stop -> :stopped
    end
  end

  @doc "Spawns a `blocks_forever/1` child, then itself blocks — one level of the descendant tree used by cancel_children tests."
  def spawns_blocking_child(_arg) do
    {:ok, _handle} = Dbos.start("blocks_forever/1", [nil])

    receive do
      :stop -> :stopped
    end
  end

  @doc "Spawns a `spawns_blocking_child/1` child (which itself spawns a `blocks_forever/1` grandchild), then blocks — a two-level descendant tree."
  def spawns_blocking_grandchild_tree(_arg) do
    {:ok, _handle} = Dbos.start("spawns_blocking_child/1", [nil])

    receive do
      :stop -> :stopped
    end
  end

  @doc """
  Enqueues a `counting_child/1` job onto the internal queue, then dies, but only the first time
  it runs: a replay after recovery finds the enqueue already checkpointed and does not enqueue a
  second one, for crash-and-recover idempotency tests.
  """
  def enqueue_and_die(table) do
    {:ok, _handle} =
      Dbos.enqueue("counting_child/1", [table], queue_name: Dbos.Queue.internal_queue_name())

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> :done
    end
  end

  @doc """
  Forks `target_workflow_id` from `start_step`, then dies, but only the first time it runs: a
  replay after recovery finds the fork already checkpointed and does not fork a second time, for
  crash-and-recover idempotency tests.
  """
  def fork_and_die(table, target_workflow_id, start_step) do
    {:ok, _fork_handle} = Dbos.fork(target_workflow_id, start_step)

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> :done
    end
  end

  @doc """
  Reads `target_workflow_id`'s status, records it into `table`, then dies, but only the first
  time it runs: a replay after recovery replays the checkpointed status rather than reading the
  (possibly since-changed) live row.
  """
  def status_reader_then_die(table, target_workflow_id) do
    status = Dbos.status(target_workflow_id)
    :ets.insert(table, {:status_result, status})

    case :ets.insert_new(table, {:crashed_once, true}) do
      true -> Process.exit(self(), :kill)
      false -> status
    end
  end

  @doc """
  Runs a standalone `Dbos.enqueue/3`, a `Dbos.fork/3` of `target_workflow_id`, a `Dbos.status/2`
  read of `target_workflow_id`, and a plain step, in that order, so a test can assert the exact
  `(function_id, function_name)` sequence all four produce.
  """
  def enqueue_fork_status_layout_workflow(target_workflow_id) do
    {:ok, _enqueue_handle} =
      Dbos.enqueue("add/2", [1, 2], queue_name: Dbos.Queue.internal_queue_name())

    {:ok, _fork_handle} = Dbos.fork(target_workflow_id, 0)
    _status = Dbos.status(target_workflow_id)
    Dbos.Runtime.run_step("plain_step/0", [], fn -> :ok end)
  end

  @doc """
  Runs `step_count` steps, each bumping `table`'s `:padding_runs` counter only when its body
  actually executes (never on replay), then durably sleeps for `ms`. Used both to prove a
  rehydrated workflow does not re-execute its completed steps, and that a workflow already deep
  into its run stays resident on a long wait instead of parking.
  """
  def counted_steps_then_sleep(table, step_count, ms) do
    Enum.each(1..step_count, fn i ->
      Dbos.Runtime.run_step("padding_step/1", [], fn ->
        :ets.update_counter(table, :padding_runs, {2, 1}, {:padding_runs, 0})
        i
      end)
    end)

    Dbos.sleep(ms)
    :woke
  end

  @doc "Uses `Dbos.step/2` for a one-off checkpointed step, without a `defstep`."
  def inline_step_workflow(order_id) do
    Dbos.step("one-off step", fn -> %{one_off: order_id} end)
  end

  @doc "Bumps `table`'s `:count` entry only when `Dbos.step/2`'s body actually runs, for replay tests."
  def counting_inline_step(table) do
    Dbos.step("counting step", fn ->
      :ets.update_counter(table, :count, {2, 1}, {:count, 0})
    end)
  end

  @doc "An inline step via `Dbos.step/3` that always raises, to exercise `max_retries`."
  def always_fails_inline_step(max_retries) do
    Dbos.step("flaky step", [max_retries: max_retries, base_interval_ms: 1], fn ->
      raise "boom"
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
