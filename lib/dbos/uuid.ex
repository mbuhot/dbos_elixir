defmodule Dbos.Uuid do
  @moduledoc "Generates workflow and owner identifiers."

  @doc "Generates a random UUIDv4 string."
  def v4 do
    <<part1::48, _::4, part2::12, _::2, part3::62>> = :crypto.strong_rand_bytes(16)

    <<part1::48, 4::4, part2::12, 2::2, part3::62>>
    |> Base.encode16(case: :lower)
    |> format()
  end

  defp format(
         <<a::binary-size(8), b::binary-size(4), c::binary-size(4), d::binary-size(4),
           e::binary-size(12)>>
       ) do
    "#{a}-#{b}-#{c}-#{d}-#{e}"
  end
end
