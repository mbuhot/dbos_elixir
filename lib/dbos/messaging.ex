defmodule Dbos.Messaging do
  @moduledoc """
  Messaging, event, and stream primitives: `send`/`recv`, `set_event`/`get_event`, and stream
  writes/reads, wired into `Dbos.Runtime`'s step-id/checkpoint protocol and `Dbos.Notifications`'
  wake mechanism. Backs `Dbos`'s public `send_message/4`, `recv_message/3`, `set_event/3`,
  `get_event/4`, `write_stream/3`, `close_stream/2`, and `read_stream/3`.
  """

  alias Dbos.Notifications
  alias Dbos.Runtime
  alias Dbos.Serialization
  alias Dbos.StepNames
  alias Dbos.SystemDb
  alias Dbos.Waits

  @doc "The sentinel topic a no-topic `send`/`recv` uses."
  def null_topic, do: SystemDb.null_topic()

  @doc """
  Sends `message` to `destination_id` on `topic` (`nil` normalizes to `null_topic/0`). A
  durable, checkpointed step (one id) inside a workflow; a direct write (no id) outside one.
  """
  def send_message(config, destination_id, topic, message) do
    topic = topic || null_topic()

    Runtime.run_step(StepNames.send_message(), [], fn ->
      SystemDb.send_notification(config, destination_id, topic, message)
      :ok
    end)
  end

  @doc """
  Receives a message on `topic` (`nil` normalizes to `null_topic/0`), blocking up to
  `timeout_ms`. Allocates two step ids up front — `DBOS.recv` then `DBOS.sleep` — so the layout
  is stable whether or not a wait actually happens. Registers as the exclusive receiver before
  rechecking the table, closing the race where a
  `send` lands between the check and the registration. Raises `Dbos.RecvConflictError` if
  another `recv` is already registered for this `(workflow_id, topic)`, or
  `Dbos.RecvTimeoutError` if the deadline passes with nothing consumed.
  """
  def recv_message(config, topic, timeout_ms) do
    topic = topic || null_topic()
    workflow_id = Runtime.current_workflow_id()
    step_id = Runtime.next_function_id()
    sleep_step_id = Runtime.next_function_id()

    case SystemDb.check_operation_execution(config, workflow_id, step_id, StepNames.recv()) do
      {:replay, output} ->
        output

      {:replay_failure, failure} ->
        Serialization.reraise_failure(failure)

      :none ->
        do_recv(config, workflow_id, topic, step_id, sleep_step_id, timeout_ms)
    end
  end

  defp do_recv(config, workflow_id, topic, step_id, _sleep_step_id, _timeout_ms)
       when config.testing in [:inline, :manual] do
    if SystemDb.notification_pending?(config, workflow_id, topic) do
      consume_recv(config, workflow_id, topic, step_id, false)
    else
      raise Dbos.TestingModeWaitError,
        workflow_id: workflow_id,
        operation: "recv_message",
        topic_or_key: topic
    end
  end

  defp do_recv(config, workflow_id, topic, step_id, sleep_step_id, timeout_ms) do
    engine = config.name

    case Notifications.subscribe_recv(engine, workflow_id, topic) do
      {:error, :conflict} ->
        raise Dbos.RecvConflictError, workflow_id: workflow_id, topic: topic

      :ok ->
        try do
          already_pending = SystemDb.notification_pending?(config, workflow_id, topic)

          timeout_occurred =
            if already_pending do
              false
            else
              deadline_ms =
                Runtime.run_step_at(sleep_step_id, StepNames.sleep(), [], fn ->
                  System.os_time(:millisecond) + timeout_ms
                end)

              remaining_ms = deadline_ms - System.os_time(:millisecond)

              if Waits.should_park?(config, remaining_ms, Runtime.current_step_id()) do
                Notifications.unsubscribe_recv(engine, workflow_id, topic)
                Waits.park(config, workflow_id, {:recv, topic}, deadline_ms)
              end

              result =
                Notifications.wait_until(engine, deadline_ms, fn ->
                  SystemDb.notification_pending?(config, workflow_id, topic) or
                    SystemDb.workflow_cancelled?(config, workflow_id)
                end)

              raise_if_cancelled!(config, workflow_id)
              result == :timeout
            end

          consume_recv(config, workflow_id, topic, step_id, timeout_occurred)
        after
          Notifications.unsubscribe_recv(engine, workflow_id, topic)
        end
    end
  end

  defp consume_recv(config, workflow_id, topic, step_id, timeout_occurred) do
    Runtime.run_step_at(step_id, StepNames.recv(), [], fn ->
      case SystemDb.consume_notification(config, workflow_id, topic) do
        {:ok, message} ->
          message

        :none ->
          if timeout_occurred do
            raise Dbos.RecvTimeoutError, workflow_id: workflow_id, topic: topic
          else
            nil
          end
      end
    end)
  end

  @doc """
  Sets `workflow_events[key]` for the current workflow, upserting `workflow_events_history`
  under this call's step id. One step id.
  """
  def set_event(config, key, value) do
    workflow_id = Runtime.current_workflow_id()
    function_id = Runtime.next_function_id()

    Runtime.run_step_at(function_id, StepNames.set_event(), [], fn ->
      SystemDb.set_event_value(config, workflow_id, function_id, key, value)
    end)
  end

  @doc """
  Reads `workflow_events[key]` for `target_workflow_id`, blocking up to `timeout_ms`, returning
  `nil` on timeout. Inside a workflow: allocates two step ids (`DBOS.getEvent` then
  `DBOS.sleep`), checkpointed like `recv_message/3` but with a non-exclusive registration —
  multiple `get_event` calls may wait on the same key concurrently. Outside a workflow: the same
  wait, but the read itself is not checkpointed (no workflow to checkpoint into).
  """
  def get_event(config, target_workflow_id, key, timeout_ms) do
    if Runtime.in_workflow?() do
      get_event_inside_workflow(config, target_workflow_id, key, timeout_ms)
    else
      get_event_outside_workflow(config, target_workflow_id, key, timeout_ms)
    end
  end

  defp get_event_inside_workflow(config, target_workflow_id, key, timeout_ms) do
    workflow_id = Runtime.current_workflow_id()
    step_id = Runtime.next_function_id()
    sleep_step_id = Runtime.next_function_id()

    case SystemDb.check_operation_execution(config, workflow_id, step_id, StepNames.get_event()) do
      {:replay, output} ->
        output

      {:replay_failure, failure} ->
        Serialization.reraise_failure(failure)

      :none when config.testing in [:inline, :manual] ->
        get_event_testing(config, workflow_id, target_workflow_id, key, step_id)

      :none ->
        engine = config.name
        :ok = Notifications.subscribe_event(engine, target_workflow_id, key)

        try do
          already_present = event_present?(config, target_workflow_id, key)

          unless already_present do
            deadline_ms =
              Runtime.run_step_at(sleep_step_id, StepNames.sleep(), [], fn ->
                System.os_time(:millisecond) + timeout_ms
              end)

            remaining_ms = deadline_ms - System.os_time(:millisecond)

            if Waits.should_park?(config, remaining_ms, Runtime.current_step_id()) do
              Notifications.unsubscribe_event(engine, target_workflow_id, key)
              Waits.park(config, workflow_id, {:event, target_workflow_id, key}, deadline_ms)
            end

            Notifications.wait_until(engine, deadline_ms, fn ->
              event_present?(config, target_workflow_id, key) or
                SystemDb.workflow_cancelled?(config, workflow_id)
            end)

            raise_if_cancelled!(config, workflow_id)
          end

          Runtime.run_step_at(step_id, StepNames.get_event(), [], fn ->
            case SystemDb.get_event_value(config, target_workflow_id, key) do
              {:ok, value} -> value
              :none -> nil
            end
          end)
        after
          Notifications.unsubscribe_event(engine, target_workflow_id, key)
        end
    end
  end

  defp get_event_testing(config, workflow_id, target_workflow_id, key, step_id) do
    if event_present?(config, target_workflow_id, key) do
      Runtime.run_step_at(step_id, StepNames.get_event(), [], fn ->
        case SystemDb.get_event_value(config, target_workflow_id, key) do
          {:ok, value} -> value
          :none -> nil
        end
      end)
    else
      raise Dbos.TestingModeWaitError,
        workflow_id: workflow_id,
        operation: "get_event",
        topic_or_key: key
    end
  end

  defp get_event_outside_workflow(config, target_workflow_id, key, _timeout_ms)
       when config.testing in [:inline, :manual] do
    case SystemDb.get_event_value(config, target_workflow_id, key) do
      {:ok, value} -> value
      :none -> nil
    end
  end

  defp get_event_outside_workflow(config, target_workflow_id, key, timeout_ms) do
    engine = config.name
    :ok = Notifications.subscribe_event(engine, target_workflow_id, key)

    try do
      deadline_ms = timeout_ms && System.os_time(:millisecond) + timeout_ms

      Notifications.wait_until(engine, deadline_ms, fn ->
        event_present?(config, target_workflow_id, key)
      end)

      case SystemDb.get_event_value(config, target_workflow_id, key) do
        {:ok, value} -> value
        :none -> nil
      end
    after
      Notifications.unsubscribe_event(engine, target_workflow_id, key)
    end
  end

  defp event_present?(config, workflow_id, key) do
    match?({:ok, _value}, SystemDb.get_event_value(config, workflow_id, key))
  end

  @doc "Appends `value` to stream `key` for the current workflow. One step id."
  def write_stream(config, key, value) do
    workflow_id = Runtime.current_workflow_id()
    function_id = Runtime.next_function_id()

    Runtime.run_step_at(function_id, StepNames.write_stream(), [], fn ->
      case SystemDb.write_stream(config, workflow_id, function_id, key, value) do
        :ok ->
          value

        {:error, :stream_closed} ->
          raise Dbos.StreamClosedError, workflow_id: workflow_id, key: key
      end
    end)
  end

  @doc "Writes the close sentinel for stream `key` for the current workflow. One step id."
  def close_stream(config, key) do
    workflow_id = Runtime.current_workflow_id()
    function_id = Runtime.next_function_id()

    Runtime.run_step_at(function_id, StepNames.close_stream(), [], fn ->
      case SystemDb.close_stream(config, workflow_id, function_id, key) do
        :ok ->
          :ok

        {:error, :stream_closed} ->
          raise Dbos.StreamClosedError, workflow_id: workflow_id, key: key
      end
    end)
  end

  @doc """
  An Elixir `Stream` over `workflow_id`'s stream `key`, from the beginning, terminating once the
  close sentinel is read (never yielded itself). Falls back to a final read-then-stop once the
  producing workflow reaches a terminal status, closing the race where the last write(s) landed
  after the last poll but before the workflow finished.
  """
  def read_stream(config, workflow_id, key) do
    Stream.resource(
      fn -> %{offset: 0, closed: false} end,
      fn
        %{closed: true} = state -> {:halt, state}
        state -> read_stream_next(config, workflow_id, key, state)
      end,
      fn _state -> :ok end
    )
  end

  defp read_stream_next(config, workflow_id, key, state) do
    engine = config.name
    :ok = Notifications.subscribe_stream(engine, workflow_id, key)

    try do
      {values, next_offset, closed} =
        SystemDb.read_stream_page(config, workflow_id, key, state.offset)

      cond do
        values != [] or closed ->
          {values, %{state | offset: next_offset, closed: closed}}

        workflow_terminal?(config, workflow_id) ->
          {final_values, final_offset, _closed} =
            SystemDb.read_stream_page(config, workflow_id, key, state.offset)

          {final_values, %{state | offset: final_offset, closed: true}}

        true ->
          Notifications.wait_until(engine, nil, fn ->
            {values, _offset, _closed} =
              SystemDb.read_stream_page(config, workflow_id, key, state.offset)

            values != []
          end)

          read_stream_next(config, workflow_id, key, state)
      end
    after
      Notifications.unsubscribe_stream(engine, workflow_id, key)
    end
  end

  defp workflow_terminal?(config, workflow_id) do
    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, %{status: status}} -> status not in [:pending, :enqueued, :delayed]
      {:error, :not_found} -> false
    end
  end

  @doc """
  Durably sleeps for `ms`: checkpoints the absolute wake time under one `DBOS.sleep` step, then
  waits only the remaining interval, so a recovered workflow does not wait the full duration
  again. Waits via `Dbos.Notifications.wait_until/3` (not `Process.sleep/1`) so cancelling this
  workflow wakes and interrupts the sleep immediately rather than waiting it out.
  """
  def sleep(config, ms) do
    function_id = Runtime.next_function_id()
    workflow_id = Runtime.current_workflow_id()

    deadline_ms =
      Runtime.run_step_at(function_id, StepNames.sleep(), [], fn ->
        System.os_time(:millisecond) + ms
      end)

    if config.testing in [:inline, :manual] do
      raise_if_cancelled!(config, workflow_id)
    else
      wait_for_deadline(config, workflow_id, deadline_ms)
    end
  end

  defp wait_for_deadline(config, workflow_id, deadline_ms) do
    engine = config.name
    remaining_ms = deadline_ms - System.os_time(:millisecond)

    if Waits.should_park?(config, remaining_ms, Runtime.current_step_id()) do
      Waits.park(config, workflow_id, :sleep, deadline_ms)
    end

    Notifications.wait_until(engine, deadline_ms, fn ->
      System.os_time(:millisecond) >= deadline_ms or
        SystemDb.workflow_cancelled?(config, workflow_id)
    end)

    raise_if_cancelled!(config, workflow_id)
  end

  defp raise_if_cancelled!(config, workflow_id) do
    if SystemDb.workflow_cancelled?(config, workflow_id) do
      raise Dbos.WorkflowCancelledError, workflow_id: workflow_id
    end
  end
end
