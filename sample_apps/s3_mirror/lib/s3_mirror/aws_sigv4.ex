defmodule S3Mirror.AwsSigv4 do
  @moduledoc """
  A minimal AWS Signature Version 4 signer for S3 requests: just enough to sign a GET, PUT, or
  `ListObjectsV2` request with `UNSIGNED-PAYLOAD`, so the sample avoids a full AWS SDK dependency
  for three verbs.
  """

  @doc """
  Returns the headers (including `Authorization`) to add to a request. `creds`:
  `%{access_key_id:, secret_access_key:, region:}`. `method` is an upcase string
  (`"GET"`/`"PUT"`), `host` the bucket's virtual-hosted-style host, `path` URL-encoded already,
  `query` the raw query string (`""` if none), `headers` any headers already decided (must
  include `"host"`).
  """
  def sign(creds, method, host, path, query, headers) do
    now = DateTime.utc_now()
    amz_date = amz_date(now)
    date_stamp = date_stamp(now)

    headers =
      headers
      |> Map.put("host", host)
      |> Map.put("x-amz-date", amz_date)
      |> Map.put("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

    {signed_headers, canonical_headers} = canonical_headers(headers)

    canonical_request =
      Enum.join(
        [
          method,
          path,
          query,
          canonical_headers,
          "",
          signed_headers,
          "UNSIGNED-PAYLOAD"
        ],
        "\n"
      )

    credential_scope = "#{date_stamp}/#{creds.region}/s3/aws4_request"

    string_to_sign =
      Enum.join(
        [
          "AWS4-HMAC-SHA256",
          amz_date,
          credential_scope,
          hex_sha256(canonical_request)
        ],
        "\n"
      )

    signing_key = signing_key(creds.secret_access_key, date_stamp, creds.region)
    signature = hex_hmac(signing_key, string_to_sign)

    authorization =
      "AWS4-HMAC-SHA256 Credential=#{creds.access_key_id}/#{credential_scope}, " <>
        "SignedHeaders=#{signed_headers}, Signature=#{signature}"

    Map.put(headers, "authorization", authorization)
  end

  defp canonical_headers(headers) do
    sorted = headers |> Enum.map(fn {k, v} -> {String.downcase(k), String.trim(v)} end) |> Enum.sort()
    signed_headers = sorted |> Enum.map_join(";", fn {k, _v} -> k end)
    canonical = sorted |> Enum.map_join("", fn {k, v} -> "#{k}:#{v}\n" end)
    {signed_headers, canonical}
  end

  defp signing_key(secret_access_key, date_stamp, region) do
    ("AWS4" <> secret_access_key)
    |> hmac(date_stamp)
    |> hmac(region)
    |> hmac("s3")
    |> hmac("aws4_request")
  end

  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data)
  defp hex_hmac(key, data), do: key |> hmac(data) |> Base.encode16(case: :lower)
  defp hex_sha256(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)

  defp amz_date(dt), do: dt |> DateTime.to_iso8601(:basic) |> String.replace(~r/\.\d+Z$/, "Z")
  defp date_stamp(dt), do: dt |> amz_date() |> String.slice(0, 8)
end
