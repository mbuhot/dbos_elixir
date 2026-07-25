defmodule Dbos.AdminServer do
  @moduledoc """
  The operator-facing HTTP API (`/dbos-healthz`, `/dbos-workflow-recovery`, `/deactivate`,
  `/dbos-workflow-queues-metadata`, `/dbos-garbage-collect`, `/dbos-global-timeout`, `/queues`,
  `/workflows`, `/workflows/{id}`, `/workflows/{id}/steps`, `/workflows/{id}/cancel`,
  `/workflows/{id}/resume`, `/workflows/{id}/fork`). Opt-in via `Dbos.Supervisor`'s
  `:admin_server` option; default port `3001`.

  Built directly on `:gen_tcp`: a dozen small JSON routes are simpler to implement and test as one
  `:gen_tcp.recv/3` + `:erlang.decode_packet(:http_bin, ...)` loop than through `:inets`'s `httpd`
  Erlang `mod`-callback/config-file surface, which is built for serving static content plus
  CGI-style modules.

  One process per accepted connection, one request per connection (no keep-alive) — see
  `Dbos.AdminServer.Handler`. Routing and rendering live in `Dbos.AdminServer.Router` and
  `Dbos.AdminServer.Render`, kept independent of the socket layer so they're plain-function
  testable.
  """

  use GenServer

  alias Dbos.AdminServer.Handler

  defstruct [:engine, :listen_socket, :port, :acceptor]

  @default_port 3001

  @doc "Starts the admin server for the engine named `opts[:name]`, listening on `opts[:port]` (default `3001`)."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: process_name(engine))
  end

  @doc "The `Dbos.AdminServer` process name for `engine`."
  def process_name(engine), do: Module.concat(engine, AdminServer)

  @doc "The port this engine's admin server is actually bound to (useful when `:port` was `0`)."
  def port(engine), do: GenServer.call(process_name(engine), :port)

  @impl true
  def init(opts) do
    engine = Keyword.fetch!(opts, :name)
    port = Keyword.get(opts, :port, @default_port)

    {:ok, listen_socket} =
      :gen_tcp.listen(port, [:binary, packet: :http_bin, active: false, reuseaddr: true])

    {:ok, actual_port} = :inet.port(listen_socket)
    acceptor = spawn_link(fn -> accept_loop(listen_socket, engine) end)

    {:ok,
     %__MODULE__{
       engine: engine,
       listen_socket: listen_socket,
       port: actual_port,
       acceptor: acceptor
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.listen_socket)
    :ok
  end

  defp accept_loop(listen_socket, engine) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        {:ok, pid} = Task.start(fn -> Handler.handle(client_socket, engine) end)
        :ok = :gen_tcp.controlling_process(client_socket, pid)
        accept_loop(listen_socket, engine)

      {:error, :closed} ->
        :ok
    end
  end
end
