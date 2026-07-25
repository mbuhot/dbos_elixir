defmodule Dbos.DiscoveryFixture do
  @moduledoc """
  A `defworkflow` module with a cron schedule, compiled into the `:dbos` OTP application's own
  `test/support` path so `Dbos.SupervisorDiscoveryTest` can exercise `otp_app:`-based discovery
  against a real, already-compiled module.
  """

  use Dbos

  defworkflow discovered_add(a, b), name: "discovery_fixture_add" do
    a + b
  end

  defworkflow discovered_tick(scheduled_time_ms, context),
    name: "discovery_fixture_tick",
    schedule: "0 0 0 1 1 *" do
    {scheduled_time_ms, context}
  end
end
