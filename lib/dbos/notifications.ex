defmodule Dbos.Notifications do
  @moduledoc """
  Subscribing a process to workflow progress, over one dedicated Postgres `LISTEN` connection per
  engine.

  `subscribe_status/2`, `subscribe_event/3` and `subscribe_stream/3` register the calling process
  as a waiter; it then receives a message each time the subscribed workflow's status changes, its
  event key is set, or its stream is appended to. This is the integration point for bridging
  workflow progress onto another transport — `Phoenix.PubSub`, a LiveView, a websocket. Each has
  a matching `unsubscribe_*`.

  Event and stream waiters fan out to every registered process. `subscribe_recv/3` is exclusive:
  a second concurrent receiver on the same `(workflow_id, topic)` gets `{:error, :conflict}`.

  A subscription is a wake-up, never a payload — the notification carries no value, so a woken
  process re-reads the current state. `mode/1` reports the transport actually in effect: `:listen`,
  or `:poll` when no dedicated connection could be established.

  `subscribe_all/2` is the engine-wide form: one registration covers every workflow on the engine,
  including workflows that did not exist when the subscription was made. See its documentation for
  the message shape and the transport it requires.
  """

  use GenServer
  require Logger

  alias Dbos.Config

  @engine_wide_kinds [:status, :recv, :event, :stream]

  # The Postgrex connection options worth carrying over from an Ecto repo's config. Everything
  # else there describes the repo rather than the connection, and `:pool` actively breaks a
  # dedicated listener: a `Ecto.Adapters.SQL.Sandbox` pool leaves it owned by nobody.
  @postgrex_conn_keys [
    :hostname,
    :port,
    :database,
    :username,
    :password,
    :socket,
    :socket_dir,
    :ssl,
    :ssl_opts,
    :parameters,
    :connect_timeout,
    :search_path,
    :endpoints,
    :types,
    :show_sensitive_data_on_connection_error
  ]

  @notifications_channel "dbos_notifications_channel"
  @workflow_events_channel "dbos_workflow_events_channel"
  @streams_channel "dbos_streams_channel"
  @queue_channel "dbos_queue_channel"
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

  @doc """
  Registers the caller to be woken whenever a workflow lands `ENQUEUED` on `queue_name`, from any
  node. This is what lets a queue runner poll on a long fallback interval instead of a short one:
  the wake-up is the notification, and the interval is only there for the case where no `LISTEN`
  connection is available.
  """
  def subscribe_queue(engine, queue_name) do
    {:ok, _owner} = Registry.register(wait_registry_name(engine), {:queue, queue_name}, nil)
    :ok
  end

  @doc "Releases a `subscribe_queue/2` registration."
  def unsubscribe_queue(engine, queue_name) do
    release(wait_registry_name(engine), {:queue, queue_name})
  end

  @doc """
  Registers the caller to be woken whenever a `DELAYED` workflow is written or its wake time
  moves, from any node — what `Dbos.Queue.Delayed` re-arms its timer on.
  """
  def subscribe_delayed(engine) do
    {:ok, _owner} = Registry.register(wait_registry_name(engine), :delayed, nil)
    :ok
  end

  @doc "Releases a `subscribe_delayed/1` registration."
  def unsubscribe_delayed(engine) do
    release(wait_registry_name(engine), :delayed)
  end

  defp payload(id, topic_or_key), do: id <> "::" <> topic_or_key

  @doc """
  Registers the caller to be woken when `workflow_id` completes. A purely in-process signal,
  fired right after this engine durably records the outcome; `Dbos.await/2` falls back to polling
  for a workflow finished by a process this engine instance didn't run (a different node, or
  before this engine started).
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
    notify_all(engine, :status, workflow_id, nil)
  end

  @doc "The kinds `subscribe_all/2` accepts: `#{inspect(@engine_wide_kinds)}`."
  def engine_wide_kinds, do: @engine_wide_kinds

  @doc """
  Registers the caller for every notification of each kind in `kinds` on `engine`, for every
  workflow, with no workflow id known in advance. One registration serves a long-lived bridge
  process — a `Phoenix.PubSub` republisher, a dashboard — for the lifetime of the engine.

  Each notification arrives as `{:dbos_notification, kind, workflow_id, key}`:

  | `kind` | Fires when | `key` |
  |---|---|---|
  | `:status` | a workflow reaches a terminal status | `nil` |
  | `:recv` | a message is sent to a workflow | the topic |
  | `:event` | a workflow sets an event key | the event key |
  | `:stream` | a workflow appends to a stream | the stream key |

  `{:dbos_notification, :reconnect, nil, nil}` arrives when the `LISTEN` connection dropped and
  came back; notifications in the gap were lost, so a subscriber that holds derived state resyncs
  it from the database. The four kinds carry an identifier and a change, never a value: a
  subscriber reads the current state with `Dbos.status/2`, `Dbos.get_event/4` or
  `Dbos.read_stream/3`.

  ## Transport

  `:recv`, `:event` and `:stream` ride Postgres `LISTEN`/`NOTIFY` and require `mode/1` to be
  `:listen`; subscribing to any of them raises `ArgumentError` under `:poll`. `:status` is an
  in-process signal fired by this engine instance, available under both transports, and covers
  only workflows this instance finished — a workflow completed on another node reaches no
  `:status` subscriber here.

  ## Cost

  Every notification on the engine is delivered to every engine-wide subscriber of that kind: one
  `Registry.dispatch/3` plus one `send/2` per subscriber, all from the single `Dbos.Notifications`
  process. A subscriber therefore receives the engine's whole notification volume rather than one
  workflow's, and slow subscribers accumulate mailbox rather than blocking the dispatcher. Keep
  the count of engine-wide subscribers small — one bridge per engine — and narrow `kinds` to what
  the bridge acts on.
  """
  def subscribe_all(engine, kinds \\ @engine_wide_kinds) do
    kinds = validate_kinds!(kinds)
    require_listen!(engine, kinds)

    Enum.each(kinds, fn kind ->
      {:ok, _owner} = Registry.register(wait_registry_name(engine), {:all, kind}, nil)
    end)

    :ok
  end

  @doc "Releases a `subscribe_all/2` registration for each kind in `kinds`."
  def unsubscribe_all(engine, kinds \\ @engine_wide_kinds) do
    kinds
    |> validate_kinds!()
    |> Enum.each(&release(wait_registry_name(engine), {:all, &1}))
  end

  defp validate_kinds!(kinds) do
    kinds = List.wrap(kinds)

    case kinds -- @engine_wide_kinds do
      [] ->
        kinds

      unknown ->
        raise ArgumentError,
              "unknown engine-wide notification kind(s) #{inspect(unknown)}; expected any of #{inspect(@engine_wide_kinds)}"
    end
  end

  defp require_listen!(engine, kinds) do
    listen_only = kinds -- [:status]

    if listen_only != [] and mode(engine) == :poll do
      raise ArgumentError,
            "engine-wide subscription to #{inspect(listen_only)} on #{inspect(engine)} requires the LISTEN transport, and this engine is in :poll mode; subscribe to [:status] only, or configure a dedicated notifications connection"
    end

    :ok
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
        Postgrex.Notifications.listen(pid, @queue_channel)
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
      {engine_wide, per_workflow} =
        registry
        |> Registry.select([{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
        |> Enum.split_with(&match?({{:all, _kind}, _pid}, &1))

      Enum.each(per_workflow, fn {_key, pid} -> send(pid, {:dbos_notify, :reconnect, nil}) end)

      engine_wide
      |> Enum.map(fn {_key, pid} -> pid end)
      |> Enum.uniq()
      |> Enum.each(&send(&1, {:dbos_notification, :reconnect, nil, nil}))
    end

    :ok
  end

  defp notifications_conn_opts(%Config{notifications_conn_opts: opts}) when is_list(opts),
    do: opts

  defp notifications_conn_opts(%Config{db: Dbos.DB.Ecto, conn: repo}) do
    if Code.ensure_loaded?(Ecto) do
      case Keyword.take(repo.config(), @postgrex_conn_keys) do
        [] -> nil
        opts -> opts
      end
    end
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
    notify_all_from_payload(engine, :recv, payload)
  end

  defp dispatch_notification(engine, @workflow_events_channel, payload) do
    broadcast(wait_registry_name(engine), {:event, payload}, :event, payload)
    notify_all_from_payload(engine, :event, payload)
  end

  defp dispatch_notification(engine, @streams_channel, payload) do
    broadcast(wait_registry_name(engine), {:stream, payload}, :stream, payload)
    notify_all_from_payload(engine, :stream, payload)
  end

  # The queue trigger's payload is "<status>::<queue_name>": an ENQUEUED row wakes that queue's
  # runner, a DELAYED one wakes the timer that will promote it.
  defp dispatch_notification(engine, @queue_channel, payload) do
    case String.split(payload, "::", parts: 2) do
      ["ENQUEUED", queue_name] ->
        broadcast(wait_registry_name(engine), {:queue, queue_name}, :queue, queue_name)

      ["DELAYED", queue_name] ->
        broadcast(wait_registry_name(engine), :delayed, :delayed, queue_name)

      _other ->
        :ok
    end
  end

  defp notify_all_from_payload(engine, kind, payload) do
    case String.split(payload, "::", parts: 2) do
      [workflow_id, key] -> notify_all(engine, kind, workflow_id, key)
      _other -> :ok
    end
  end

  defp notify_all(engine, kind, workflow_id, key) do
    dispatch(wait_registry_name(engine), {:all, kind}, {
      :dbos_notification,
      kind,
      workflow_id,
      key
    })
  end

  defp broadcast(registry, key, keyspace, payload) do
    dispatch(registry, key, {:dbos_notify, keyspace, payload})
  end

  defp dispatch(registry, key, message) do
    Registry.dispatch(registry, key, fn entries ->
      for {pid, _owner} <- entries, do: send(pid, message)
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
