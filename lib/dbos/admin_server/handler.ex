# Reads exactly one HTTP/1.1 request off an accepted socket, routes it through
# Dbos.AdminServer.Router, writes the response, and closes the connection (no keep-alive — the
# admin API is a low-traffic operator surface, so the simplicity is worth the extra TCP handshake
# per call).
defmodule Dbos.AdminServer.Handler do
  @moduledoc false

  require Logger

  @recv_timeout_ms 5_000

  @doc "Handles one request-response cycle on `socket` for `engine`, then closes it. Runs in its own process."
  def handle(socket, engine) do
    case read_request(socket) do
      {:ok, method, path, headers} ->
        body = read_body(socket, headers)

        {status, response_body, content_type} =
          Dbos.AdminServer.Router.route(engine, method, path, body)

        respond(socket, status, response_body, content_type)

      {:error, reason} ->
        Logger.debug("dbos admin server: failed to read request: #{inspect(reason)}")
    end
  after
    :gen_tcp.close(socket)
  end

  defp read_request(socket) do
    with {:ok, {:http_request, method, {:abs_path, path}, _version}} <-
           :gen_tcp.recv(socket, 0, @recv_timeout_ms) do
      {:ok, http_method(method), path, read_headers(socket, %{})}
    else
      {:ok, other} -> {:error, {:unexpected_request_line, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_headers(socket, acc) do
    case :gen_tcp.recv(socket, 0, @recv_timeout_ms) do
      {:ok, :http_eoh} ->
        acc

      {:ok, {:http_header, _, field, _, value}} ->
        read_headers(socket, Map.put(acc, header_name(field), value))

      _other ->
        acc
    end
  end

  defp header_name(field) when is_atom(field), do: field |> Atom.to_string() |> String.downcase()
  defp header_name(field), do: field |> to_string() |> String.downcase()

  defp read_body(socket, headers) do
    case Map.get(headers, "content-length") do
      nil ->
        ""

      length_str ->
        length = length_str |> to_string() |> String.to_integer()
        :inet.setopts(socket, packet: :raw)

        case length do
          0 -> ""
          _ -> recv_body(socket, length)
        end
    end
  end

  defp recv_body(_socket, 0), do: ""

  defp recv_body(socket, length) do
    case :gen_tcp.recv(socket, length, @recv_timeout_ms) do
      {:ok, data} -> data
      {:error, _reason} -> ""
    end
  end

  defp http_method(:GET), do: :get
  defp http_method(:POST), do: :post
  defp http_method({:string, string}), do: string |> String.downcase() |> String.to_atom()
  defp http_method(other), do: other |> to_string() |> String.downcase() |> String.to_atom()

  defp respond(socket, status, body, content_type) do
    status_line = "HTTP/1.1 #{status} #{reason_phrase(status)}\r\n"

    response_headers =
      "Content-Type: #{content_type}\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n"

    :gen_tcp.send(socket, status_line <> response_headers <> body)
  end

  defp reason_phrase(200), do: "OK"
  defp reason_phrase(204), do: "No Content"
  defp reason_phrase(400), do: "Bad Request"
  defp reason_phrase(404), do: "Not Found"
  defp reason_phrase(500), do: "Internal Server Error"
  defp reason_phrase(_status), do: "Unknown"
end
