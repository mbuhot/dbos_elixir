defmodule Dbos.Cron do
  @moduledoc """
  Parses and evaluates the 6-field cron expression `Dbos.Scheduler` uses: `second minute hour
  day-of-month month day-of-week`. Upstream (`scheduler.go`, `models.NewScheduleCronParser`)
  configures `robfig/cron` with `Second | Minute | Hour | Dom | Month | Dow | Descriptor` — six
  numeric fields plus `@`-prefixed descriptors (`@daily`, `@every 1h`, ...). This port implements
  the six numeric fields only (no descriptors, no month/weekday names) — a deliberate scope cut
  from the reference's full grammar, noted in `DECISIONS.md`.

  Each field accepts `*`, a single integer, a comma-separated list, a range `a-b`, and a stepped
  range/wildcard `a-b/n` or `*/n`. Field ranges: second/minute `0-59`, hour `0-23`, day-of-month
  `1-31`, month `1-12`, day-of-week `0-6` (`0` = Sunday). Per POSIX cron, when *both*
  day-of-month and day-of-week are restricted (neither is `*`), a candidate matches if *either*
  matches; if only one is restricted, only that one is checked.

  Only UTC is supported — the engine has no IANA timezone database dependency. `cron_timezone`
  values other than `nil`/`"UTC"` raise `Dbos.NotSupportedError`.
  """

  defstruct [:seconds, :minutes, :hours, :doms, :months, :dows, :dom_wildcard?, :dow_wildcard?]

  @type t :: %__MODULE__{}

  @field_ranges [
    seconds: 0..59,
    minutes: 0..59,
    hours: 0..23,
    doms: 1..31,
    months: 1..12,
    dows: 0..6
  ]

  @doc "Parses a 6-field cron expression, raising `ArgumentError` on a malformed one."
  def parse!(expression) when is_binary(expression) do
    fields = expression |> String.trim() |> String.split(~r/\s+/)

    if length(fields) != 6 do
      raise ArgumentError,
            "cron expression #{inspect(expression)} must have exactly 6 fields " <>
              "(second minute hour day-of-month month day-of-week), got #{length(fields)}"
    end

    [second, minute, hour, dom, month, dow] = fields

    %__MODULE__{
      seconds: parse_field(second, :seconds),
      minutes: parse_field(minute, :minutes),
      hours: parse_field(hour, :hours),
      doms: parse_field(dom, :doms),
      months: parse_field(month, :months),
      dows: parse_field(dow, :dows),
      dom_wildcard?: dom == "*",
      dow_wildcard?: dow == "*"
    }
  end

  @doc """
  The next fire time strictly after `from_epoch_ms`, as an epoch-ms integer. Bounded to search at
  most 4 years forward; raises `ArgumentError` if no match is found in that window (an
  impossible-to-satisfy expression, e.g. `31` for every month with `dow` also restricted to
  contradict it).
  """
  def next_after(%__MODULE__{} = cron, from_epoch_ms) do
    start = DateTime.from_unix!(div(from_epoch_ms, 1000) + 1, :second)
    search(cron, %{start | microsecond: {0, 0}}, start_epoch_ms(from_epoch_ms))
  end

  @doc "Every fire time in `(from_epoch_ms, to_epoch_ms]`, ascending, per `notes/` automatic backfill."
  def due_between(%__MODULE__{} = cron, from_epoch_ms, to_epoch_ms) do
    collect(cron, from_epoch_ms, to_epoch_ms, [])
  end

  defp collect(cron, from_epoch_ms, to_epoch_ms, acc) do
    case next_after(cron, from_epoch_ms) do
      next when next <= to_epoch_ms -> collect(cron, next, to_epoch_ms, [next | acc])
      _too_late -> Enum.reverse(acc)
    end
  end

  defp start_epoch_ms(from_epoch_ms), do: (div(from_epoch_ms, 1000) + 1) * 1000

  @max_years_forward 4

  defp search(cron, %DateTime{} = dt, guard_epoch_ms) do
    limit_ms = guard_epoch_ms + @max_years_forward * 365 * 24 * 60 * 60 * 1000

    if DateTime.to_unix(dt, :millisecond) > limit_ms do
      raise ArgumentError,
            "cron expression has no matching fire time within #{@max_years_forward} years"
    end

    cond do
      dt.month not in cron.months ->
        search(cron, next_month(dt), guard_epoch_ms)

      not day_matches?(cron, dt) ->
        search(cron, next_day(dt), guard_epoch_ms)

      dt.hour not in cron.hours ->
        search(cron, next_hour(dt), guard_epoch_ms)

      dt.minute not in cron.minutes ->
        search(cron, next_minute(dt), guard_epoch_ms)

      dt.second not in cron.seconds ->
        search(cron, DateTime.add(dt, 1, :second), guard_epoch_ms)

      true ->
        DateTime.to_unix(dt, :millisecond)
    end
  end

  defp day_matches?(%__MODULE__{dom_wildcard?: true, dow_wildcard?: true}, _dt), do: true

  defp day_matches?(%__MODULE__{dom_wildcard?: true} = cron, dt),
    do: (Date.day_of_week(dt, :sunday) - 1) in cron.dows

  defp day_matches?(%__MODULE__{dow_wildcard?: true} = cron, dt), do: dt.day in cron.doms

  defp day_matches?(cron, dt),
    do: dt.day in cron.doms or (Date.day_of_week(dt, :sunday) - 1) in cron.dows

  defp next_month(dt) do
    {year, month} = if dt.month == 12, do: {dt.year + 1, 1}, else: {dt.year, dt.month + 1}
    %{dt | year: year, month: month, day: 1, hour: 0, minute: 0, second: 0}
  end

  defp next_day(dt) do
    date = Date.new!(dt.year, dt.month, dt.day) |> Date.add(1)
    %{dt | year: date.year, month: date.month, day: date.day, hour: 0, minute: 0, second: 0}
  end

  defp next_hour(dt) do
    if dt.hour == 23, do: next_day(dt), else: %{dt | hour: dt.hour + 1, minute: 0, second: 0}
  end

  defp next_minute(dt) do
    if dt.minute == 59, do: next_hour(dt), else: %{dt | minute: dt.minute + 1, second: 0}
  end

  defp parse_field(spec, field) do
    range = Keyword.fetch!(@field_ranges, field)

    spec
    |> String.split(",")
    |> Enum.flat_map(&parse_term(&1, range))
    |> Enum.uniq()
    |> Enum.sort()
    |> MapSet.new()
  end

  defp parse_term(term, range) do
    case String.split(term, "/") do
      [base, step] -> parse_base(base, range) |> Enum.take_every(String.to_integer(step))
      [base] -> parse_base(base, range)
    end
  end

  defp parse_base("*", range), do: Enum.to_list(range)

  defp parse_base(base, _range) do
    case String.split(base, "-") do
      [single] -> [String.to_integer(single)]
      [from, to] -> Enum.to_list(String.to_integer(from)..String.to_integer(to))
    end
  end
end
