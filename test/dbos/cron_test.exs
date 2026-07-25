defmodule Dbos.CronTest do
  use ExUnit.Case, async: true

  alias Dbos.Cron

  defp epoch_ms(y, mo, d, h, mi, s) do
    DateTime.new!(Date.new!(y, mo, d), Time.new!(h, mi, s)) |> DateTime.to_unix(:millisecond)
  end

  test "rejects an expression without exactly 6 fields" do
    assert_raise ArgumentError, fn -> Cron.parse!("* * * * *") end
  end

  test "every minute: next_after finds the next minute boundary" do
    cron = Cron.parse!("0 * * * * *")
    from = epoch_ms(2026, 1, 1, 10, 15, 30)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 1, 10, 16, 0)
  end

  test "daily at a fixed time: next_after jumps to the next day when today's time has passed" do
    cron = Cron.parse!("0 30 9 * * *")
    from = epoch_ms(2026, 1, 1, 12, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 2, 9, 30, 0)
  end

  test "a restricted month field skips forward to the next matching month" do
    cron = Cron.parse!("0 0 0 1 3 *")
    from = epoch_ms(2026, 1, 15, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 3, 1, 0, 0, 0)
  end

  test "when both day-of-month and day-of-week are restricted, either matching is enough (POSIX OR)" do
    cron = Cron.parse!("0 0 0 1 * 1")
    from = epoch_ms(2026, 2, 1, 0, 0, 1)
    next = Cron.next_after(cron, from)
    next_dt = DateTime.from_unix!(next, :millisecond)
    assert next_dt.day == 1 or Date.day_of_week(next_dt, :sunday) - 1 == 1
  end

  test "due_between returns every fire time in the window, ascending" do
    cron = Cron.parse!("0 * * * * *")
    from = epoch_ms(2026, 1, 1, 10, 0, 0)
    to = epoch_ms(2026, 1, 1, 10, 3, 0)

    assert Cron.due_between(cron, from, to) == [
             epoch_ms(2026, 1, 1, 10, 1, 0),
             epoch_ms(2026, 1, 1, 10, 2, 0),
             epoch_ms(2026, 1, 1, 10, 3, 0)
           ]
  end
end
