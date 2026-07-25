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
    assert Cron.next_after(cron, from) == epoch_ms(2026, 2, 2, 0, 0, 0)
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

  test "a single month name is equivalent to its numeric field" do
    cron = Cron.parse!("0 0 0 1 MAR *")
    from = epoch_ms(2026, 1, 15, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 3, 1, 0, 0, 0)
  end

  test "a month name range JAN-MAR admits every month in the range" do
    cron = Cron.parse!("0 0 0 1 JAN-MAR *")
    from = epoch_ms(2026, 1, 2, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 2, 1, 0, 0, 0)
  end

  test "a month name list, case-insensitive, admits exactly the listed months" do
    cron = Cron.parse!("0 0 0 1 jan,mar,may *")
    from = epoch_ms(2026, 1, 2, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 3, 1, 0, 0, 0)
  end

  test "a day-of-week name range MON-FRI admits only weekdays" do
    cron = Cron.parse!("0 0 9 * * MON-FRI")
    from = epoch_ms(2026, 1, 2, 9, 0, 1)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 5, 9, 0, 0)
  end

  test "a day-of-week name list, case-insensitive, admits exactly the listed days" do
    cron = Cron.parse!("0 0 9 * * sun,sat")
    from = epoch_ms(2026, 1, 1, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 3, 9, 0, 0)
  end

  test "a step over a day-of-week name range picks every other weekday" do
    cron = Cron.parse!("0 0 9 * * MON-FRI/2")
    from = epoch_ms(2026, 1, 1, 9, 0, 1)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 2, 9, 0, 0)
  end

  test "? is a synonym for * in the day-of-month field" do
    cron = Cron.parse!("0 0 9 ? * MON")
    from = epoch_ms(2026, 1, 1, 9, 0, 1)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 5, 9, 0, 0)
  end

  test "? is a synonym for * in the day-of-week field" do
    cron = Cron.parse!("0 0 0 1 * ?")
    from = epoch_ms(2026, 1, 15, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 2, 1, 0, 0, 0)
  end

  test "name-based day-of-month/day-of-week OR logic, lowercase" do
    cron = Cron.parse!("0 0 0 1 * mon")
    from = epoch_ms(2026, 2, 2, 0, 0, 1)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 2, 9, 0, 0, 0)
  end

  test "@yearly and @annually are equivalent to midnight on January 1st" do
    from = epoch_ms(2026, 3, 1, 0, 0, 0)
    assert Cron.next_after(Cron.parse!("@yearly"), from) == epoch_ms(2027, 1, 1, 0, 0, 0)
    assert Cron.next_after(Cron.parse!("@annually"), from) == epoch_ms(2027, 1, 1, 0, 0, 0)
  end

  test "@monthly is equivalent to midnight on the first of the month" do
    cron = Cron.parse!("@monthly")
    from = epoch_ms(2026, 1, 15, 0, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 2, 1, 0, 0, 0)
  end

  test "@weekly is equivalent to midnight on Sunday" do
    cron = Cron.parse!("@weekly")
    from = epoch_ms(2026, 1, 1, 0, 0, 1)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 4, 0, 0, 0)
  end

  test "@daily and @midnight are equivalent to midnight every day" do
    from = epoch_ms(2026, 1, 1, 1, 0, 0)
    assert Cron.next_after(Cron.parse!("@daily"), from) == epoch_ms(2026, 1, 2, 0, 0, 0)
    assert Cron.next_after(Cron.parse!("@midnight"), from) == epoch_ms(2026, 1, 2, 0, 0, 0)
  end

  test "@hourly is equivalent to the top of every hour" do
    cron = Cron.parse!("@hourly")
    from = epoch_ms(2026, 1, 1, 10, 15, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 1, 11, 0, 0)
  end

  test "@every <duration> fires at a fixed interval from the previous fire time" do
    cron = Cron.parse!("@every 1h30m")
    from = epoch_ms(2026, 1, 1, 10, 0, 0)
    assert Cron.next_after(cron, from) == epoch_ms(2026, 1, 1, 11, 30, 0)
  end

  test "@every <duration> with seconds-only precision" do
    cron = Cron.parse!("@every 45s")
    from = epoch_ms(2026, 1, 1, 10, 0, 0)
    assert Cron.next_after(cron, from) == from + 45_000
  end

  test "@every <duration> due_between collects every tick in the window" do
    cron = Cron.parse!("@every 1m")
    from = epoch_ms(2026, 1, 1, 10, 0, 0)
    to = epoch_ms(2026, 1, 1, 10, 3, 0)

    assert Cron.due_between(cron, from, to) == [
             epoch_ms(2026, 1, 1, 10, 1, 0),
             epoch_ms(2026, 1, 1, 10, 2, 0),
             epoch_ms(2026, 1, 1, 10, 3, 0)
           ]
  end

  test "an out-of-range second raises naming the second field" do
    assert_raise ArgumentError, ~r/second/, fn -> Cron.parse!("70 * * * * *") end
  end

  test "an out-of-range day-of-month raises naming the day-of-month field" do
    assert_raise ArgumentError, ~r/day-of-month/, fn -> Cron.parse!("* * * 32 * *") end
  end

  test "an out-of-range month raises naming the month field" do
    assert_raise ArgumentError, ~r/month/, fn -> Cron.parse!("* * * * 13 *") end
  end

  test "an out-of-range day-of-week raises naming the day-of-week field" do
    assert_raise ArgumentError, ~r/day-of-week/, fn -> Cron.parse!("* * * * * 8") end
  end

  test "an unknown descriptor raises a clear error" do
    assert_raise ArgumentError, ~r/@bogus/, fn -> Cron.parse!("@bogus") end
  end

  test "an unparseable @every duration raises a clear error" do
    assert_raise ArgumentError, ~r/nonsense/, fn -> Cron.parse!("@every nonsense") end
  end
end
