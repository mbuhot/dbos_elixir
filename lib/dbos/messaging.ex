# Messaging, event, and stream primitives: send/recv, set_event/get_event, and stream
# writes/reads, wired into Dbos.Runtime's step-id/checkpoint protocol and Dbos.Notifications'
# wake mechanism. Backs Dbos's public send_message/4, recv_message/3, set_event/3, get_event/4,
# write_stream/3, close_stream/2, and read_stream/3.
defmodule Dbos.Messaging do
  @moduledoc false

  alias Dbos.Notifications
  alias Dbos.Runtime
  alias Dbos.Serialization
  alias Dbos.StepNames
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.Waits

  @doc "The sentinel topic a no-topic `send`/`recv` uses."
  def null_topic, do: SystemDb.null_topic()

  @doc """
  Sends `message` to `destination_id` on `topic` (`nil` normalizes to `null_topic/0`). A
  durable, checkpointed step (one id) inside a workflow; a direct write (no id) outside one.
  """
  def send_message(config, destination_id, topic, message, opts \\ []) do
    topic = topic || null_topic()

    Runtime.run_step(StepNames.send_message(), compensation_opts(opts), fn ->
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

  Under an `:inline`/`:manual` testing-mode engine, nothing pending is the deadline having
  passed: no process exists to deliver a message later, so the outcome is already decided and
  `Dbos.RecvTimeoutError` is raised without waiting out `timeout_ms`.
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
    timed_out? = not SystemDb.notification_pending?(config, workflow_id, topic)
    consume_recv(config, workflow_id, topic, step_id, timed_out?)
  end

  defp do_recv(config, workflow_id, topic, step_id, sleep_step_id, timeout_ms) do
    engine = config.name

    case Notifications.subscribe_recv(engine, workflow_id, topic) do
      {:error, :conflict} ->
        raise Dbos.RecvConflictError, workflow_id: workflow_id, topic: topic

      :ok ->
        try do
          metadata = wait_metadata(config, workflow_id, :recv, timeout_ms, topic)

          timeout_occurred =
            Telemetry.span_wait(metadata, fn ->
              await_notification(config, workflow_id, topic, sleep_step_id, timeout_ms)
            end)

          consume_recv(config, workflow_id, topic, step_id, timeout_occurred)
        after
          Notifications.unsubscribe_recv(engine, workflow_id, topic)
        end
    end
  end

  defp await_notification(config, workflow_id, topic, sleep_step_id, timeout_ms) do
    engine = config.name

    if SystemDb.notification_pending?(config, workflow_id, topic) do
      {false, :resolved}
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
            SystemDb.workflow_cancellation(config, workflow_id) != nil
        end)

      raise_if_cancelled!(config, workflow_id)
      {result == :timeout, wait_outcome(result)}
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
  def set_event(config, key, value, opts \\ []) do
    workflow_id = Runtime.current_workflow_id()
    function_id = Runtime.next_function_id()

    Runtime.run_step_at(function_id, StepNames.set_event(), compensation_opts(opts), fn ->
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
          metadata =
            wait_metadata(config, workflow_id, :event, timeout_ms, key, target_workflow_id)

          Telemetry.span_wait(metadata, fn ->
            await_event(config, workflow_id, target_workflow_id, key, sleep_step_id, timeout_ms)
          end)

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

  defp await_event(config, workflow_id, target_workflow_id, key, sleep_step_id, timeout_ms) do
    engine = config.name

    if event_present?(config, target_workflow_id, key) do
      {:ok, :resolved}
    else
      deadline_ms =
        Runtime.run_step_at(sleep_step_id, StepNames.sleep(), [], fn ->
          System.os_time(:millisecond) + timeout_ms
        end)

      remaining_ms = deadline_ms - System.os_time(:millisecond)

      if Waits.should_park?(config, remaining_ms, Runtime.current_step_id()) do
        Notifications.unsubscribe_event(engine, target_workflow_id, key)
        Waits.park(config, workflow_id, {:event, target_workflow_id, key}, deadline_ms)
      end

      result =
        Notifications.wait_until(engine, deadline_ms, fn ->
          event_present?(config, target_workflow_id, key) or
            SystemDb.workflow_cancellation(config, workflow_id) != nil
        end)

      raise_if_cancelled!(config, workflow_id)
      {:ok, wait_outcome(result)}
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
      metadata = wait_metadata(config, nil, :event, timeout_ms, key, target_workflow_id)

      Telemetry.span_wait(metadata, fn ->
        deadline_ms = timeout_ms && System.os_time(:millisecond) + timeout_ms

        result =
          Notifications.wait_until(engine, deadline_ms, fn ->
            event_present?(config, target_workflow_id, key)
          end)

        {:ok, wait_outcome(result)}
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
  def write_stream(config, key, value, opts \\ []) do
    workflow_id = Runtime.current_workflow_id()
    function_id = Runtime.next_function_id()

    Runtime.run_step_at(function_id, StepNames.write_stream(), compensation_opts(opts), fn ->
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
  after the last poll but before the workflow finished. Under an `:inline`/`:manual` testing-mode
  engine, an open stream with nothing more to read raises `Dbos.TestingModeWaitError` rather than
  waiting on `Dbos.Notifications`, which never starts in those modes.
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
    {values, next_offset, closed} =
      SystemDb.read_stream_page(config, workflow_id, key, state.offset)

    cond do
      values != [] or closed ->
        {values, %{state | offset: next_offset, closed: closed}}

      workflow_halted?(config, workflow_id) ->
        {final_values, final_offset, _closed} =
          SystemDb.read_stream_page(config, workflow_id, key, state.offset)

        {final_values, %{state | offset: final_offset, closed: true}}

      config.testing in [:inline, :manual] ->
        raise Dbos.TestingModeWaitError,
          workflow_id: workflow_id,
          operation: "read_stream",
          topic_or_key: key

      true ->
        wait_for_stream_write(config, workflow_id, key, state.offset)
        read_stream_next(config, workflow_id, key, state)
    end
  end

  defp wait_for_stream_write(config, workflow_id, key, offset) do
    engine = config.name
    :ok = Notifications.subscribe_stream(engine, workflow_id, key)

    try do
      Notifications.wait_until(engine, nil, fn ->
        {values, _offset, _closed} = SystemDb.read_stream_page(config, workflow_id, key, offset)
        values != []
      end)
    after
      Notifications.unsubscribe_stream(engine, workflow_id, key)
    end
  end

  defp workflow_halted?(config, workflow_id) do
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
      Telemetry.span_wait(wait_metadata(config, workflow_id, :sleep, ms), fn ->
        wait_for_deadline(config, workflow_id, deadline_ms)
      end)
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
        SystemDb.workflow_cancellation(config, workflow_id) != nil
    end)

    {raise_if_cancelled!(config, workflow_id), :resolved}
  end

  defp wait_outcome(:found), do: :resolved
  defp wait_outcome(:timeout), do: :timeout

  defp wait_metadata(config, workflow_id, kind, timeout_ms, key \\ nil, target_workflow_id \\ nil) do
    %{
      engine: config.name,
      workflow_id: workflow_id,
      kind: kind,
      key: key,
      target_workflow_id: target_workflow_id,
      timeout_ms: timeout_ms
    }
  end

  # A durable primitive's `compensate:`, resolved once here so the runtime writes it onto the
  # checkpoint only if the operation actually commits one.
  defp compensation_opts(opts) do
    case Keyword.get(opts, :compensate) do
      nil ->
        []

      compensate ->
        record = Dbos.Compensation.record!(compensate)
        [compensation: fn -> record end]
    end
  end

  defp raise_if_cancelled!(config, workflow_id) do
    case SystemDb.workflow_cancellation(config, workflow_id) do
      nil -> nil
      :cancelled -> raise Dbos.WorkflowCancelledError, workflow_id: workflow_id
      :cancelling -> raise Dbos.WorkflowCancellingError, workflow_id: workflow_id
    end
  end
end
