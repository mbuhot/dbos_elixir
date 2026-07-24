defmodule Dbos.SerializationTest do
  use ExUnit.Case, async: true

  alias Dbos.Serialization

  defmodule Money do
    defstruct [:amount, :currency]
  end

  defmodule CardDeclinedError do
    defexception [:amount, :currency]

    @impl true
    def message(%__MODULE__{amount: amount, currency: currency}) do
      "card declined for #{amount} #{currency}"
    end
  end

  test "round-trips an atom" do
    encoded = Serialization.encode(:aud)
    assert Serialization.decode(encoded) == :aud
  end

  test "round-trips a tuple" do
    encoded = Serialization.encode({:ok, 1, "two"})
    assert Serialization.decode(encoded) == {:ok, 1, "two"}
  end

  test "round-trips a nested map" do
    term = %{a: %{b: [1, 2, %{c: :d}]}}
    encoded = Serialization.encode(term)
    assert Serialization.decode(encoded) == term
  end

  test "round-trips a struct" do
    term = %Money{amount: 4999, currency: :aud}
    encoded = Serialization.encode(term)
    assert Serialization.decode(encoded) == term
  end

  test "round-trips a charlist" do
    term = ~c"hello"
    encoded = Serialization.encode(term)
    assert Serialization.decode(encoded) == term
  end

  test "round-trips a large binary" do
    term = :crypto.strong_rand_bytes(1_000_000)
    encoded = Serialization.encode(term)
    assert Serialization.decode(encoded) == term
  end

  test "round-trips nil" do
    encoded = Serialization.encode(nil)
    assert Serialization.decode(encoded) == nil
  end

  test "rejects a decoded pid" do
    encoded = Serialization.encode(self())
    assert_raise ArgumentError, fn -> Serialization.decode(encoded) end
  end

  test "format_name is erl_etf" do
    assert Serialization.format_name() == "erl_etf"
  end

  test "decode/2 with the correct format name returns ok" do
    encoded = Serialization.encode(%{a: 1})
    assert Serialization.decode(encoded, "erl_etf") == {:ok, %{a: 1}}
  end

  test "decode/2 with an unrecognized format name returns an error" do
    encoded = Serialization.encode(%{a: 1})

    assert Serialization.decode(encoded, "DBOS_JSON") ==
             {:error, {:unsupported_serialization, "DBOS_JSON"}}
  end

  describe "encode_failure/3 and reraise_failure/1" do
    test "round-trips a raised custom exception, preserving its type and fields" do
      {kind, value, stacktrace} =
        try do
          raise CardDeclinedError, amount: 4999, currency: :aud
        rescue
          exception -> {:error, exception, __STACKTRACE__}
        end

      encoded = Serialization.encode_failure(kind, value, stacktrace)
      decoded = Serialization.decode(encoded)

      assert_raise CardDeclinedError, fn -> Serialization.reraise_failure(decoded) end

      try do
        Serialization.reraise_failure(decoded)
      rescue
        exception ->
          assert exception == %CardDeclinedError{amount: 4999, currency: :aud}
      end
    end

    test "round-trips a thrown value" do
      {kind, value, stacktrace} =
        try do
          throw({:card_declined, "insufficient funds"})
        catch
          kind, value -> {kind, value, __STACKTRACE__}
        end

      encoded = Serialization.encode_failure(kind, value, stacktrace)
      decoded = Serialization.decode(encoded)

      caught =
        try do
          Serialization.reraise_failure(decoded)
        catch
          kind, value -> {kind, value}
        end

      assert caught == {:throw, {:card_declined, "insufficient funds"}}
    end

    test "round-trips an exit" do
      {kind, value, stacktrace} =
        try do
          exit(:normal)
        catch
          kind, value -> {kind, value, __STACKTRACE__}
        end

      encoded = Serialization.encode_failure(kind, value, stacktrace)
      decoded = Serialization.decode(encoded)

      caught =
        try do
          Serialization.reraise_failure(decoded)
        catch
          kind, value -> {kind, value}
        end

      assert caught == {:exit, :normal}
    end

    test "reraise_failure attaches the original stacktrace" do
      {kind, value, stacktrace} =
        try do
          raise CardDeclinedError, amount: 1, currency: :usd
        rescue
          exception -> {:error, exception, __STACKTRACE__}
        end

      encoded = Serialization.encode_failure(kind, value, stacktrace)
      decoded = Serialization.decode(encoded)

      try do
        Serialization.reraise_failure(decoded)
      rescue
        _ ->
          reraised_stacktrace = __STACKTRACE__
          assert reraised_stacktrace == stacktrace
      end
    end
  end
end
