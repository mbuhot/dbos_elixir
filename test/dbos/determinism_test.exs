defmodule Dbos.DeterminismTest do
  use ExUnit.Case, async: true

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
        _ = x
        #{body}
      end
    end
    """

    Code.compile_string(source, "test/fixture.ex")
  end

  defp use_line(""), do: "use Dbos"
  defp use_line(opts), do: "use Dbos, #{opts}"

  defp compile_step(body, opts \\ []) do
    module = fresh_module_name()
    use_line = use_line(Keyword.get(opts, :use_opts, ""))
    macro = Keyword.get(opts, :macro, "defstep")

    source = """
    defmodule #{inspect(module)} do
      #{use_line}

      #{macro} run(x) do
        _ = x
        #{body}
      end
    end
    """

    Code.compile_string(source, "test/fixture.ex")
  end

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

  @step_banned [
    {"spawn(fn -> :ok end)", "spawn"},
    {"spawn_link(fn -> :ok end)", "spawn_link"},
    {"spawn_monitor(fn -> :ok end)", "spawn_monitor"},
    {"Task.async(fn -> :ok end)", "Task.async"},
    {"Task.await(Task.async(fn -> :ok end))", "Task.await"},
    {"Task.async_stream([1], fn i -> i end)", "Task.async_stream"},
    {"Task.start(fn -> :ok end)", "Task.start"},
    {"Task.start_link(fn -> :ok end)", "Task.start_link"}
  ]

  for {{code, expected}, index} <- Enum.with_index(@step_banned) do
    test "banned construct in a step body #{index}: #{expected}" do
      error =
        assert_raise CompileError, fn ->
          compile_step(unquote(code))
        end

      assert error.description =~ unquote(expected)
      assert error.description =~ ~r/test\/fixture\.ex:\d+/
      assert error.description =~ "no workflow context"
    end

    test "banned construct in a transaction body #{index}: #{expected}" do
      error =
        assert_raise CompileError, fn ->
          compile_step(unquote(code), macro: "deftransaction")
        end

      assert error.description =~ unquote(expected)
      assert error.description =~ ~r/test\/fixture\.ex:\d+/
      assert error.description =~ "no workflow context"
    end
  end

  test "a violation nested inside a case branch in a step body is caught" do
    body = """
    case x do
      1 -> Task.async(fn -> :ok end)
      _ -> :ok
    end
    """

    error =
      assert_raise CompileError, fn ->
        compile_step(body)
      end

    assert error.description =~ "Task.async"
  end

  test "a violation nested inside an anonymous function within a step body is caught" do
    body = """
    fun = fn -> spawn(fn -> :ok end) end
    fun.()
    """

    error =
      assert_raise CompileError, fn ->
        compile_step(body)
      end

    assert error.description =~ "spawn/1"
  end

  test "DateTime.utc_now/0 is allowed in a step body" do
    assert [{_module, _binary}] = compile_step("DateTime.utc_now()")
  end

  test ":rand.uniform/1 is allowed in a step body" do
    assert [{_module, _binary}] = compile_step(":rand.uniform(10)")
  end

  test "System.system_time/1 is allowed in a step body" do
    assert [{_module, _binary}] = compile_step("System.system_time(:millisecond)")
  end

  test "a repo call is allowed in a step body" do
    assert [{_module, _binary}] = compile_step("Dbos.DeterminismFixtureRepo.insert!(x)")
  end

  test "Process.sleep/1 is allowed in a step body" do
    assert [{_module, _binary}] = compile_step("Process.sleep(10)")
  end

  test "receive is allowed in a step body" do
    assert [{_module, _binary}] =
             compile_step("""
             receive do
               :go -> :ok
             end
             """)
  end
end
