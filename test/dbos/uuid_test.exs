defmodule Dbos.UuidTest do
  use ExUnit.Case, async: true

  test "v4/0 produces a well-formed, non-repeating UUIDv4 string" do
    a = Dbos.Uuid.v4()
    b = Dbos.Uuid.v4()

    assert a =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    assert a != b
  end
end
