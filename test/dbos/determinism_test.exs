defmodule Dbos.DeterminismTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  defp fresh_module_name do
    :"Elixir.Dbos.DeterminismFixture#{System.unique_integer([:positive])}"
  end

  defp compile(body, opts \\ []) do
    module = fresh_module_name()
    use_line = use_line(Keyword.get(opts, :use_opts, ""))

    source = """
    defmodule #{inspect(module)} do
      #{use_line}

      defworkflow run(x), name: "run" do
        #{body}
      end
    end
    """

    Code.compile_string(source, "test/fixture.ex")
  end

  defp use_line(""), do: "use Dbos"
  defp use_line(opts), do: "use Dbos, #{opts}"

  test "defworkflow without name: is a compile error naming the workflow and explaining why" do
    module = fresh_module_name()

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      defworkflow run(x) do
        x
      end
    end
    """

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end

    assert error.description =~ "run/1"
    assert error.description =~ "requires a name:"
    assert error.description =~ "recovery dispatches on the workflow's name"
  end

  test "multiple defworkflow clauses for the same name/arity are rejected" do
    module = fresh_module_name()

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      defworkflow run(x), name: "a" do
        x
      end

      defworkflow run(x), name: "b" do
        x
      end
    end
    """

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end

    assert error.description =~ "run/1"
    assert error.description =~ "declared more than once"
  end

  test "a when guard on a defworkflow head is rejected" do
    module = fresh_module_name()

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      defworkflow run(x) when is_integer(x), name: "run" do
        x
      end
    end
    """

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end

    assert error.description =~ "run/1"
    assert error.description =~ "cannot have a `when` guard"
  end

  @banned [
    {":rand.uniform()", ":rand.uniform"},
    {"DateTime.utc_now()", "DateTime.utc_now"},
    {"NaiveDateTime.utc_now()", "NaiveDateTime.utc_now"},
    {"Date.utc_today()", "Date.utc_today"},
    {"System.system_time()", "System.system_time"},
    {"System.os_time()", "System.os_time"},
    {"System.monotonic_time()", "System.monotonic_time"},
    {"System.unique_integer()", "System.unique_integer"},
    {"Process.sleep(10)", "Process.sleep"},
    {"receive do\n  :go -> :ok\nend", "receive"},
    {"spawn(fn -> :ok end)", "spawn"},
    {"spawn_link(fn -> :ok end)", "spawn_link"},
    {"spawn_monitor(fn -> :ok end)", "spawn_monitor"},
    {"Task.async(fn -> :ok end)", "Task.async"},
    {"Task.await(Task.async(fn -> :ok end))", "Task.await"},
    {"Task.async_stream([1], fn i -> i end)", "Task.async_stream"},
    {"send(self(), :go)", "send/2"},
    {"make_ref()", "make_ref"}
  ]

  for {{code, expected}, index} <- Enum.with_index(@banned) do
    test "banned construct #{index}: #{expected}" do
      error =
        assert_raise CompileError, fn ->
          compile(unquote(code))
        end

      assert error.description =~ unquote(expected)
      assert error.description =~ ~r/test\/fixture\.ex:\d+/
    end
  end

  test "a violation nested inside a case branch is caught" do
    body = """
    case x do
      1 -> DateTime.utc_now()
      _ -> :ok
    end
    """

    error =
      assert_raise CompileError, fn ->
        compile(body)
      end

    assert error.description =~ "DateTime.utc_now"
  end

  test "a violation nested inside an anonymous function is caught" do
    body = """
    fun = fn -> Process.sleep(5) end
    fun.()
    """

    error =
      assert_raise CompileError, fn ->
        compile(body)
      end

    assert error.description =~ "Process.sleep"
  end

  test "a direct call to the configured repo is banned" do
    error =
      assert_raise CompileError, fn ->
        compile("Dbos.DeterminismFixtureRepo.insert!(x)",
          use_opts: "repo: Dbos.DeterminismFixtureRepo"
        )
      end

    assert error.description =~ "Dbos.DeterminismFixtureRepo.insert!"
    assert error.description =~ "deftransaction"
  end

  test "collects every violation found, not just the first" do
    body = """
    DateTime.utc_now()
    Process.sleep(5)
    """

    error =
      assert_raise CompileError, fn ->
        compile(body)
      end

    assert error.description =~ "DateTime.utc_now"
    assert error.description =~ "Process.sleep"
  end

  test "an undeclared cross-module call warns" do
    output =
      capture_io(:stderr, fn ->
        compile("Dbos.DeterminismFixtureHelper.side_effect(x)")
      end)

    assert output =~ "Dbos.DeterminismFixtureHelper.side_effect/1"
    assert output =~ "not a registered step"
  end

  test "the undeclared cross-module call warning is suppressible" do
    output =
      capture_io(:stderr, fn ->
        compile("Dbos.DeterminismFixtureHelper.side_effect(x)",
          use_opts: "warn_cross_module_calls: false"
        )
      end)

    refute output =~ "not a registered step"
  end

  test "calling a registered step in another module does not warn" do
    fixture_module = fresh_module_name()

    step_source = """
    defmodule Dbos.DeterminismFixtureSteps do
      use Dbos

      defstep helper(x) do
        x
      end
    end
    """

    Code.compile_string(step_source, "test/fixture_steps.ex")

    source = """
    defmodule #{inspect(fixture_module)} do
      use Dbos

      defworkflow run(x), name: "run" do
        Dbos.DeterminismFixtureSteps.helper(x)
      end
    end
    """

    output =
      capture_io(:stderr, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end)

    refute output =~ "not a registered step"
  end
end
