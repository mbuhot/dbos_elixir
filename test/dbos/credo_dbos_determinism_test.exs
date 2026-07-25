defmodule Credo.Check.Warning.DbosDeterminismTest do
  use Credo.Test.Case

  alias Credo.Check.Warning.DbosDeterminism

  test "accepts a workflow whose helpers are deterministic" do
    """
    defmodule Sample do
      use Dbos, repo: Sample.Repo

      defworkflow process(id), name: "process" do
        fan_out(id)
      end

      defp fan_out(id), do: Enum.map([id], &double/1)
      defp double(n), do: n * 2
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end

  test "reports a workflow that reaches Task.async_stream through a private helper" do
    """
    defmodule Sample do
      use Dbos, repo: Sample.Repo

      defworkflow process(id), name: "process" do
        fan_out(id)
      end

      defp fan_out(id) do
        Task.async_stream(id, &work/1)
      end

      defp work(item), do: item
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ "workflow process/1"
      assert issue.message =~ "Task.async_stream/2"
      assert issue.message =~ "fan_out/1"
      assert issue.line_no == 9
    end)
  end

  test "reports a violation reached through a chain of helpers" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        outer(id)
      end

      defp outer(id), do: inner(id)
      defp inner(_id), do: DateTime.utc_now()
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ "DateTime.utc_now/0"
      assert issue.message =~ "inner/1"
    end)
  end

  test "reports a violation reached through a function capture passed to Enum" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(ids), name: "process" do
        Enum.map(ids, &stamp/1)
      end

      defp stamp(id), do: {id, System.system_time()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ "System.system_time/0"
      assert issue.message =~ "stamp/1"
    end)
  end

  test "reports a step that reaches a process-spawning helper" do
    """
    defmodule Sample do
      use Dbos

      defstep charge(order), name: "charge" do
        dispatch(order)
      end

      defp dispatch(order), do: Task.async(fn -> order end)
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ "step charge/1"
      assert issue.message =~ "Task.async/1"
    end)
  end

  test "accepts a step that reaches a helper reading the current time" do
    """
    defmodule Sample do
      use Dbos

      defstep charge(order), name: "charge" do
        stamp(order)
      end

      defp stamp(order), do: {order, DateTime.utc_now()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end

  test "leaves violations in the literal workflow body to the compile-time checker" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        DateTime.utc_now()
        id
      end
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end

  test "ignores helpers that no workflow or step calls" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        id
      end

      defp unused(id), do: {id, :rand.uniform()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end

  test "terminates on mutually recursive helpers" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        ping(id)
      end

      defp ping(id), do: pong(id)
      defp pong(id), do: ping(id)
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end

  test "reports every clause of a multi-clause helper that offends" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        pick(id)
      end

      defp pick(:a), do: :rand.uniform()
      defp pick(_other), do: Process.sleep(10)
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issues(fn issues ->
      assert length(issues) == 2
    end)
  end

  test "attributes a helper shared by two workflows to each of them" do
    """
    defmodule Sample do
      use Dbos

      defworkflow first(id), name: "first" do
        helper(id)
      end

      defworkflow second(id), name: "second" do
        helper(id)
      end

      defp helper(id), do: {id, :rand.uniform()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issues(fn issues ->
      messages = Enum.map(issues, & &1.message)
      assert Enum.count(messages, &(&1 =~ "workflow first/1")) == 1
      assert Enum.count(messages, &(&1 =~ "workflow second/1")) == 1
    end)
  end

  test "does not follow a call into another workflow's body" do
    """
    defmodule Sample do
      use Dbos

      defworkflow parent(id), name: "parent" do
        child(id)
      end

      defworkflow child(id), name: "child" do
        helper(id)
      end

      defp helper(id), do: {id, :rand.uniform()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ "workflow child/1"
    end)
  end

  test "checks each module in a file that defines several" do
    """
    defmodule Outer do
      defmodule Inner do
        use Dbos

        defworkflow process(id), name: "process" do
          helper(id)
        end

        defp helper(id), do: {id, :rand.uniform()}
      end
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> assert_issue(fn issue ->
      assert issue.message =~ ":rand.uniform/0"
    end)
  end

  test "ignores a helper defined in another module with the same name" do
    """
    defmodule Sample do
      use Dbos

      defworkflow process(id), name: "process" do
        Other.helper(id)
      end
    end

    defmodule Other do
      def helper(id), do: {id, :rand.uniform()}
    end
    """
    |> to_source_file()
    |> run_check(DbosDeterminism)
    |> refute_issues()
  end
end
