defmodule Dbos.StatusTest do
  use ExUnit.Case, async: true

  alias Dbos.Status

  test "converts each status atom to its upstream string" do
    assert Status.to_string(:pending) == "PENDING"
    assert Status.to_string(:enqueued) == "ENQUEUED"
    assert Status.to_string(:delayed) == "DELAYED"
    assert Status.to_string(:success) == "SUCCESS"
    assert Status.to_string(:error) == "ERROR"
    assert Status.to_string(:cancelled) == "CANCELLED"
    assert Status.to_string(:max_recovery_attempts_exceeded) == "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
  end

  test "parses each upstream string back to its status atom" do
    assert Status.from_string("PENDING") == :pending
    assert Status.from_string("ENQUEUED") == :enqueued
    assert Status.from_string("DELAYED") == :delayed
    assert Status.from_string("SUCCESS") == :success
    assert Status.from_string("ERROR") == :error
    assert Status.from_string("CANCELLED") == :cancelled
    assert Status.from_string("MAX_RECOVERY_ATTEMPTS_EXCEEDED") == :max_recovery_attempts_exceeded
  end

  test "from_string raises on an unknown value" do
    assert_raise ArgumentError, fn -> Status.from_string("BOGUS") end
  end

  test "terminal? is true for success, error, cancelled, and max_recovery_attempts_exceeded" do
    assert Status.terminal?(:success)
    assert Status.terminal?(:error)
    assert Status.terminal?(:cancelled)
    assert Status.terminal?(:max_recovery_attempts_exceeded)
  end

  test "terminal? is false for pending, enqueued, and delayed" do
    refute Status.terminal?(:pending)
    refute Status.terminal?(:enqueued)
    refute Status.terminal?(:delayed)
  end
end
