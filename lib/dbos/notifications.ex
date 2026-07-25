defmodule Dbos.Notifications do
  @moduledoc """
  One dedicated `LISTEN` connection per engine, plus the in-process waiter registration
  recv/getEvent/streams share. `recv` waiters are exclusive
  (`Registry.register/3`'s built-in `:unique` semantics reject a second concurrent receiver on
  the same `(workflow_id, topic)`); event/stream waiters fan out to every registered process.

  If a dedicated connection cannot be established (no derivable connection options, or the
  connection attempt fails), falls back to `:poll` mode and logs a warning — `wait_until/3`'s
  bounded timeout in that mode is what stands in for the missing `NOTIFY`.

  A dropped listener connection is reconnected with backoff, without taking this GenServer down
  with it, and every registered waiter is woken to re-probe once the connection is back — a
  `NOTIFY` fired during the outage would otherwise be lost.
  """

  use GenServer
  require Logger

  alias Dbos.Config

  @notifications_channel "dbos_notifications_channel"
  @workflow_events_channel "dbos_workflow_events_channel"
  @streams_channel "dbos_streams_channel"
  @poll_interval_ms 1_000
  @reconnect_base_backoff_ms 200
  @reconnect_max_backoff_ms 5_000

  @doc "The polling fallback's fixed cadence."
  def poll_interval_ms, do: @poll_interval_ms

  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, name, name: process_name(name))
  end

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: process_name(name),
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  @doc "This engine's `Dbos.Notifications` GenServer name."
  def process_name(engine), do: Module.concat(engine, Notifications)

  @doc "This engine's exclusive `recv`-waiter registry name."
  def recv_registry_name(engine), do: Module.concat(engine, Notifications.RecvRegistry)

  @doc "This engine's fan-out event/stream-waiter registry name."
  def wait_registry_name(engine), do: Module.concat(engine, Notifications.WaitRegistry)

  @doc "The transport actually in effect for `engine`: `:listen` or `:poll`, which may differ from configured if the listener failed to start."
  def mode(engine), do: :persistent_term.get(mode_key(engine), :poll)

  defp mode_key(engine), do: {__MODULE__, :mode, engine}

  @doc "Registers the caller as the sole receiver for `(workflow_id, topic)`. `{:error, :conflict}` if another process already holds it."
  def subscribe_recv(engine, workflow_id, topic) do
    case Registry.register(recv_registry_name(engine), payload(workflow_id, topic), nil) do
      {:ok, _owner} -> :ok
      {:error, {:already_registered, _pid}} -> {:error, :conflict}
    end
  end

  @doc "Releases a `subscribe_recv/3` registration."
  def unsubscribe_recv(engine, workflow_id, topic) do
    release(recv_registry_name(engine), payload(workflow_id, topic))
  end

  @doc "Registers the caller as a waiter for `workflow_events` on `(workflow_id, key)`. Non-exclusive."
  def subscribe_event(engine, workflow_id, key) do
    {:ok, _owner} =
      Registry.register(wait_registry_name(engine), {:event, payload(workflow_id, key)}, nil)

    :ok
  end

  @doc "Releases a `subscribe_event/3` registration."
  def unsubscribe_event(engine, workflow_id, key) do
    release(wait_registry_name(engine), {:event, payload(workflow_id, key)})
  end

  @doc "Registers the caller as a waiter for stream `(workflow_id, key)`. Non-exclusive."
  def subscribe_stream(engine, workflow_id, key) do
    {:ok, _owner} =
      Registry.register(wait_registry_name(engine), {:stream, payload(workflow_id, key)}, nil)

    :ok
  end

  @doc "Releases a `subscribe_stream/3` registration."
  def unsubscribe_stream(engine, workflow_id, key) do
    release(wait_registry_name(engine), {:stream, payload(workflow_id, key)})
  end

  defp payload(id, topic_or_key), do: id <> "::" <> topic_or_key

  @doc """
  Registers the caller to be woken when `workflow_id` completes. A purely in-process signal from
  `Dbos.WorkflowProcess` right after it durably records an outcome; `Dbos.await/2` falls back to
  polling for a workflow finished by a process this engine instance didn't run (a different
  node, or before this engine started).
  """
  def subscribe_status(engine, workflow_id) do
    {:ok, _owner} = Registry.register(wait_registry_name(engine), {:status, workflow_id}, nil)
    :ok
  end

  @doc "Releases a `subscribe_status/2` registration."
  def unsubscribe_status(engine, workflow_id) do
    release(wait_registry_name(engine), {:status, workflow_id})
  end

  @doc "Wakes every waiter registered via `subscribe_status/2` for `workflow_id`."
  def notify_status(engine, workflow_id) do
    broadcast(wait_registry_name(engine), {:status, workflow_id}, :status, workflow_id)
  end

  @doc """
  Blocks until `recheck_fun.()` (a zero-arg predicate) is truthy or `deadline_ms` (an absolute
  `System.os_time(:millisecond)`, or `nil` for no deadline) passes, re-invoking `recheck_fun` on
  every wake. A wake — a real `NOTIFY`, or, in `:poll` mode, the bounded timeout standing in for
  one — is only a hint to recheck. Returns `:found` or `:timeout`.
  """
  def wait_until(engine, deadline_ms, recheck_fun) do
    if recheck_fun.() do
      :found
    else
      now = System.os_time(:millisecond)

      if deadline_ms && now >= deadline_ms do
        :timeout
      else
        receive do
          {:dbos_notify, _keyspace, _payload} -> :ok
        after
          wake_timeout(engine, deadline_ms, now) -> :ok
        end

        wait_until(engine, deadline_ms, recheck_fun)
      end
    end
  end

  defp wake_timeout(engine, nil, _now) do
    if mode(engine) == :poll, do: @poll_interval_ms, else: :infinity
  end

  defp wake_timeout(engine, deadline_ms, now) do
    remaining = max(deadline_ms - now, 0)
    if mode(engine) == :poll, do: min(remaining, @poll_interval_ms), else: remaining
  end

  @impl true
  def init(engine) do
    Process.flag(:trap_exit, true)
    config = Dbos.config(engine)

    case config.notifications do
      :poll ->
        :persistent_term.put(mode_key(engine), :poll)
        {:ok, %{engine: engine, conn_opts: nil, listener: nil, retry_attempt: 0}}

      :listen ->
        {:ok, start_listener(engine, config)}
    end
  end

  defp start_listener(engine, config) do
    base_state = %{engine: engine, conn_opts: nil, listener: nil, retry_attempt: 0}

    case notifications_conn_opts(config) do
      nil ->
        Logger.warning(
          "Dbos.Notifications(#{inspect(engine)}): no connection options could be derived for a dedicated LISTEN connection; falling back to polling"
        )

        :persistent_term.put(mode_key(engine), :poll)
        %{base_state | conn_opts: nil}

      conn_opts ->
        connect(%{base_state | conn_opts: conn_opts})
    end
  end

  defp connect(%{engine: engine, conn_opts: conn_opts} = state) do
    case Postgrex.Notifications.start_link(conn_opts) do
      {:ok, pid} ->
        Postgrex.Notifications.listen(pid, @notifications_channel)
        Postgrex.Notifications.listen(pid, @workflow_events_channel)
        Postgrex.Notifications.listen(pid, @streams_channel)
        :persistent_term.put(mode_key(engine), :listen)
        %{state | listener: pid, retry_attempt: 0}

      {:error, reason} ->
        Logger.warning(
          "Dbos.Notifications(#{inspect(engine)}): failed to start the LISTEN connection (#{inspect(reason)}); falling back to polling and retrying"
        )

        :persistent_term.put(mode_key(engine), :poll)
        schedule_reconnect(state.retry_attempt)
        %{state | listener: nil, retry_attempt: state.retry_attempt + 1}
    end
  end

  defp schedule_reconnect(retry_attempt) do
    delay =
      (@reconnect_base_backoff_ms * :math.pow(2, retry_attempt))
      |> min(@reconnect_max_backoff_ms)
      |> round()

    Process.send_after(self(), :reconnect_listener, delay)
  end

  defp wake_all_waiters(engine) do
    wake_all(recv_registry_name(engine))
    wake_all(wait_registry_name(engine))
  end

  defp wake_all(registry) do
    if Process.whereis(registry) do
      registry
      |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
      |> Enum.each(fn {_key, pid} -> send(pid, {:dbos_notify, :reconnect, nil}) end)
    end

    :ok
  end

  defp notifications_conn_opts(%Config{notifications_conn_opts: opts}) when is_list(opts),
    do: opts

  defp notifications_conn_opts(%Config{db: Dbos.DB.Ecto, conn: repo}) do
    if Code.ensure_loaded?(Ecto), do: repo.config()
  end

  defp notifications_conn_opts(_config), do: nil

  @impl true
  def handle_info({:notification, _pid, _ref, channel, payload}, state) do
    dispatch_notification(state.engine, channel, payload)
    {:noreply, state}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, %{listener: pid} = state) do
    Logger.warning(
      "Dbos.Notifications(#{inspect(state.engine)}): LISTEN connection lost (#{inspect(reason)}); reconnecting"
    )

    :persistent_term.put(mode_key(state.engine), :poll)
    schedule_reconnect(state.retry_attempt)
    {:noreply, %{state | listener: nil, retry_attempt: state.retry_attempt + 1}}
  end

  @impl true
  def handle_info(:reconnect_listener, %{conn_opts: nil} = state), do: {:noreply, state}

  @impl true
  def handle_info(:reconnect_listener, state) do
    case connect(state) do
      %{listener: pid} = new_state when is_pid(pid) ->
        wake_all_waiters(state.engine)
        {:noreply, new_state}

      new_state ->
        {:noreply, new_state}
    end
  end

  defp dispatch_notification(engine, @notifications_channel, payload) do
    broadcast(recv_registry_name(engine), payload, :recv, payload)
  end

  defp dispatch_notification(engine, @workflow_events_channel, payload) do
    broadcast(wait_registry_name(engine), {:event, payload}, :event, payload)
  end

  defp dispatch_notification(engine, @streams_channel, payload) do
    broadcast(wait_registry_name(engine), {:stream, payload}, :stream, payload)
  end

  defp broadcast(registry, key, keyspace, payload) do
    Registry.dispatch(registry, key, fn entries ->
      for {pid, _owner} <- entries, do: send(pid, {:dbos_notify, keyspace, payload})
    end)

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp release(registry, key) do
    Registry.unregister(registry, key)
    :ok
  rescue
    ArgumentError -> :ok
  end
end
