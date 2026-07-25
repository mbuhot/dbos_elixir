defmodule Dbos.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias Dbos.RetryPolicy

  test "new/1 applies defaults when no options are given" do
    assert RetryPolicy.new() == %RetryPolicy{
             max_retries: 0,
             base_interval_ms: 100,
             backoff_factor: 2.0,
             max_interval_ms: 5000
           }
  end

  test "new/1 overrides defaults with given options" do
    assert RetryPolicy.new(
             max_retries: 5,
             base_interval_ms: 10,
             backoff_factor: 3.0,
             max_interval_ms: 1000
           ) == %RetryPolicy{
             max_retries: 5,
             base_interval_ms: 10,
             backoff_factor: 3.0,
             max_interval_ms: 1000
           }
  end

  test "delay_ms/2 grows by the backoff factor with each attempt" do
    policy = RetryPolicy.new(base_interval_ms: 100, backoff_factor: 2.0, max_interval_ms: 5000)

    assert RetryPolicy.delay_ms(policy, 1) == 100
    assert RetryPolicy.delay_ms(policy, 2) == 200
    assert RetryPolicy.delay_ms(policy, 3) == 400
    assert RetryPolicy.delay_ms(policy, 4) == 800
  end

  test "delay_ms/2 is capped at max_interval_ms" do
    policy = RetryPolicy.new(base_interval_ms: 100, backoff_factor: 2.0, max_interval_ms: 500)

    assert RetryPolicy.delay_ms(policy, 3) == 400
    assert RetryPolicy.delay_ms(policy, 4) == 500
    assert RetryPolicy.delay_ms(policy, 5) == 500
  end

  test "retry?/2 allows attempts up to max_retries and refuses beyond it" do
    policy = RetryPolicy.new(max_retries: 2)

    assert RetryPolicy.retry?(policy, 1)
    assert RetryPolicy.retry?(policy, 2)
    refute RetryPolicy.retry?(policy, 3)
  end

  test "retry?/2 refuses any retry when max_retries is 0" do
    policy = RetryPolicy.new(max_retries: 0)

    refute RetryPolicy.retry?(policy, 1)
  end
end
