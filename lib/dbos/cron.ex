defmodule Dbos.Cron do
  @moduledoc """
  Parses and evaluates the cron grammar `Dbos.Scheduler` uses, matching `robfig/cron/v3`
  configured with `WithSeconds()`: six space-separated fields `second minute hour day-of-month
  month day-of-week`, `@`-prefixed descriptors, or a fixed `@every <duration>` interval.

  Each numeric field accepts `*` (or `?` for day-of-month/day-of-week), a single integer, a
  comma-separated list, a range `a-b`, and a stepped range/wildcard `a-b/n` or `*/n`. Field
  ranges: second/minute `0-59`, hour `0-23`, day-of-month `1-31`, month `1-12` (or `JAN`-`DEC`,
  case-insensitive), day-of-week `0-6` (`0` = Sunday, or `SUN`-`SAT`, case-insensitive). Per
  POSIX cron, when *both* day-of-month and day-of-week are restricted (neither is a wildcard), a
  candidate matches if *either* matches; if only one is restricted, only that one is checked.

  Descriptors `@yearly`/`@annually`, `@monthly`, `@weekly`, `@daily`/`@midnight`, and `@hourly`
  expand to their equivalent 6-field expression. `@every <duration>` (e.g. `@every 1h30m`) is a
  fixed interval measured from the previous fire time rather than a wall-clock schedule.

  Only UTC is supported — the engine has no IANA timezone database dependency. `cron_timezone`
  values other than `nil`/`"UTC"` raise `Dbos.NotSupportedError`.
  """

  defstruct [
    :kind,
    :interval_ms,
    :seconds,
    :minutes,
    :hours,
    :doms,
    :months,
    :dows,
    :dom_wildcard?,
    :dow_wildcard?
  ]

  @type t :: %__MODULE__{}

  @field_ranges [
    seconds: 0..59,
    minutes: 0..59,
    hours: 0..23,
    doms: 1..31,
    months: 1..12,
    dows: 0..6
  ]

  @field_labels [
    seconds: "second",
    minutes: "minute",
    hours: "hour",
    doms: "day-of-month",
    months: "month",
    dows: "day-of-week"
  ]

  @month_names Enum.zip(~w(JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC), 1..12)
  @dow_names Enum.zip(~w(SUN MON TUE WED THU FRI SAT), 0..6)

  @descriptors %{
    "@yearly" => "0 0 0 1 1 *",
    "@annually" => "0 0 0 1 1 *",
    "@monthly" => "0 0 0 1 * *",
    "@weekly" => "0 0 0 * * 0",
    "@daily" => "0 0 0 * * *",
    "@midnight" => "0 0 0 * * *",
    "@hourly" => "0 0 * * * *"
  }

  @doc """
  Parses a cron expression (6 fields, a `@`-descriptor, or `@every <duration>`), raising
  `ArgumentError` on a malformed one.
  """
  def parse!(expression) when is_binary(expression) do
    trimmed = String.trim(expression)

    cond do
      String.starts_with?(trimmed, "@every ") ->
        parse_every!(trimmed)

      Map.has_key?(@descriptors, trimmed) ->
        parse_calendar!(Map.fetch!(@descriptors, trimmed))

      String.starts_with?(trimmed, "@") ->
        raise ArgumentError, "unknown cron descriptor #{inspect(trimmed)}"

      true ->
        parse_calendar!(trimmed)
    end
  end

  defp parse_every!(trimmed) do
    duration_text = trimmed |> String.replace_prefix("@every ", "") |> String.trim()
    %__MODULE__{kind: :every, interval_ms: parse_duration_ms!(duration_text)}
  end

  defp parse_calendar!(expression) do
    fields = expression |> String.trim() |> String.split(~r/\s+/)

    if length(fields) != 6 do
      raise ArgumentError,
            "cron expression #{inspect(expression)} must have exactly 6 fields " <>
              "(second minute hour day-of-month month day-of-week), got #{length(fields)}"
    end

    [second, minute, hour, dom, month, dow] = fields
    normalized_dom = normalize_field(dom, :doms)
    normalized_month = normalize_field(month, :months)
    normalized_dow = normalize_field(dow, :dows)

    %__MODULE__{
      kind: :calendar,
      seconds: parse_field(second, :seconds),
      minutes: parse_field(minute, :minutes),
      hours: parse_field(hour, :hours),
      doms: parse_field(normalized_dom, :doms),
      months: parse_field(normalized_month, :months),
      dows: parse_field(normalized_dow, :dows),
      dom_wildcard?: normalized_dom == "*",
      dow_wildcard?: normalized_dow == "*"
    }
  end

  @doc """
  The next fire time strictly after `from_epoch_ms`, as an epoch-ms integer. For a calendar
  schedule, bounded to search at most 4 years forward; raises `ArgumentError` if no match is
  found in that window (an impossible-to-satisfy expression, e.g. `31` for every month with `dow`
  also restricted to contradict it). For an `@every` schedule, simply `from_epoch_ms +
  interval_ms`.
  """
  def next_after(%__MODULE__{kind: :every, interval_ms: interval_ms}, from_epoch_ms) do
    from_epoch_ms + interval_ms
  end

  def next_after(%__MODULE__{kind: :calendar} = cron, from_epoch_ms) do
    start = DateTime.from_unix!(div(from_epoch_ms, 1000) + 1, :second)
    search(cron, %{start | microsecond: {0, 0}}, start_epoch_ms(from_epoch_ms))
  end

  @doc "Every fire time in `(from_epoch_ms, to_epoch_ms]`, ascending."
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

  defp normalize_field(spec, :doms), do: String.replace(spec, "?", "*")

  defp normalize_field(spec, :dows) do
    spec |> String.replace("?", "*") |> String.upcase() |> replace_names(@dow_names)
  end

  defp normalize_field(spec, :months), do: spec |> String.upcase() |> replace_names(@month_names)

  defp replace_names(spec, names), do: Enum.reduce(names, spec, &replace_name/2)

  defp replace_name({name, number}, spec),
    do: String.replace(spec, name, Integer.to_string(number))

  defp parse_field(spec, field) do
    range = Keyword.fetch!(@field_ranges, field)

    values =
      spec
      |> String.split(",")
      |> Enum.flat_map(&parse_term(&1, range))
      |> Enum.uniq()
      |> Enum.sort()

    unless Enum.all?(values, &(&1 in range)), do: raise_field_error!(field, spec)

    MapSet.new(values)
  rescue
    _e in ArgumentError -> raise_field_error!(field, spec)
  end

  defp raise_field_error!(field, spec) do
    label = Keyword.fetch!(@field_labels, field)
    raise ArgumentError, "invalid #{label} field #{inspect(spec)} in cron expression"
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

  defp parse_duration_ms!(duration_text) do
    matches = Regex.scan(~r/(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/, duration_text)
    matched_text = matches |> Enum.map(&Enum.at(&1, 0)) |> Enum.join()

    if matches == [] or matched_text != duration_text do
      raise ArgumentError, "invalid duration #{inspect(duration_text)} in @every schedule"
    end

    matches
    |> Enum.reduce(0.0, fn [_whole, amount, unit], acc ->
      {number, ""} = Float.parse(amount)
      acc + number * unit_to_ms(unit)
    end)
    |> round()
  end

  defp unit_to_ms("ns"), do: 1.0e-6
  defp unit_to_ms("us"), do: 1.0e-3
  defp unit_to_ms("µs"), do: 1.0e-3
  defp unit_to_ms("ms"), do: 1.0
  defp unit_to_ms("s"), do: 1000.0
  defp unit_to_ms("m"), do: 60_000.0
  defp unit_to_ms("h"), do: 3_600_000.0
end
