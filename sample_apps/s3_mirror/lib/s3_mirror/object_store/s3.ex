defmodule S3Mirror.ObjectStore.S3 do
  @moduledoc """
  `S3Mirror.ObjectStore` over a real S3 bucket, signed with `S3Mirror.AwsSigv4` (SigV4,
  `UNSIGNED-PAYLOAD`) over `Req` — no AWS SDK dependency. The store is
  `%{bucket:, region:, access_key_id:, secret_access_key:}`. Used only when AWS credentials are
  present in the environment; see the README for which variables gate this path.
  """

  @behaviour S3Mirror.ObjectStore

  alias S3Mirror.AwsSigv4

  @doc "Builds a store from the standard `AWS_*` environment variables. Raises if any are missing."
  def from_env!(bucket) do
    %{
      bucket: bucket,
      region: fetch_env!("AWS_REGION"),
      access_key_id: fetch_env!("AWS_ACCESS_KEY_ID"),
      secret_access_key: fetch_env!("AWS_SECRET_ACCESS_KEY")
    }
  end

  @doc "Whether the environment has everything `from_env!/1` needs, without raising."
  def configured? do
    Enum.all?(~w(AWS_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY), &System.get_env/1)
  end

  @impl true
  def list_keys(store, prefix) do
    query = URI.encode_query(%{"list-type" => "2", "prefix" => prefix})

    case request(store, "GET", "/", query, "") do
      {:ok, %{status: 200, body: body}} -> {:ok, extract_keys(body)}
      {:ok, %{status: status, body: body}} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read(store, key) do
    case request(store, "GET", object_path(key), "", "") do
      {:ok, %{status: 200, body: body}} -> {:ok, body}
      {:ok, %{status: 404}} -> {:error, :not_found}
      {:ok, %{status: status, body: body}} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def write(store, key, data) do
    case request(store, "PUT", object_path(key), "", data) do
      {:ok, %{status: status}} when status in 200..299 -> :ok
      {:ok, %{status: status, body: body}} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def exists?(store, key) do
    case request(store, "HEAD", object_path(key), "", "") do
      {:ok, %{status: 200}} -> true
      _other -> false
    end
  end

  defp object_path(key), do: "/" <> URI.encode(key, &URI.char_unreserved?/1)

  defp host(store), do: "#{store.bucket}.s3.#{store.region}.amazonaws.com"

  defp request(store, method, path, query, body) do
    host = host(store)
    headers = if body == "", do: %{}, else: %{"content-length" => to_string(byte_size(body))}
    signed = AwsSigv4.sign(store, method, host, path, query, headers)
    url = %URI{scheme: "https", host: host, path: path, query: nonempty(query)} |> URI.to_string()

    Req.request(
      method: method,
      url: url,
      headers: Map.to_list(signed),
      body: body,
      decode_body: false
    )
  end

  defp nonempty(""), do: nil
  defp nonempty(query), do: query

  defp extract_keys(xml) do
    ~r{<Key>(.*?)</Key>}s
    |> Regex.scan(xml)
    |> Enum.map(fn [_full, key] -> key end)
    |> Enum.sort()
  end

  defp fetch_env!(name) do
    System.get_env(name) ||
      raise "#{name} is not set; export the AWS_* variables described in the README, or use " <>
              "S3Mirror.ObjectStore.Local instead"
  end
end
