defmodule Dbos.ScheduledWorkflows do
  @moduledoc "Fixture module exercising `defworkflow`'s `schedule:` option, used by `Dbos.SchedulerTest`."

  use Dbos

  defworkflow tick(scheduled_time_ms, context), name: "sched_tick", schedule: "* * * * * *" do
    {scheduled_time_ms, context}
  end

  defworkflow backfill_tick(scheduled_time_ms, _context),
    name: "sched_backfill_tick",
    schedule: [cron: "* * * * * *", automatic_backfill: true] do
    scheduled_time_ms
  end
end
