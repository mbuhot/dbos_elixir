defmodule Dbos.CompileDbosTest do
  use ExUnit.Case, async: false

  alias Dbos.Compiler.Analysis
  alias Dbos.Compiler.State

  @moduledoc """
  Drives the compilation tracer over fixture source and asserts on the diagnostics the whole
  application analysis produces from what it collected.
  """

  setup context do
    app = :"dbos_tracer_#{System.unique_integer([:positive])}"
    manifest = Path.join(context.tmp_dir, "manifest")
    :persistent_term.put({Mix.Tasks.Compile.Dbos, :foreign}, MapSet.new())
    pid = start_run(app, manifest, force: true)

    on_exit(fn ->
      :persistent_term.erase({Mix.Tasks.Compile.Dbos, :foreign})
      :persistent_term.erase({Dbos.Compiler.State, :app})
    end)

    %{app: app, manifest: manifest, state: pid}
  end

  @tag :tmp_dir
  test "a nondeterministic call in a helper is reported against the workflow that reaches it" do
    suffix = compile_fixture()

    assert [diagnostic] = diagnostics()
    assert diagnostic.severity == :warning
    assert diagnostic.file == "lib/helpers.ex"
    assert diagnostic.position == 4
    assert diagnostic.message =~ "nondeterministic call reachable from a workflow body"
    assert diagnostic.message =~ ~s|Orders#{suffix}.place/1  (workflow "place")|
    assert diagnostic.message =~ ":rand.uniform/1"
  end

  @tag :tmp_dir
  test "the report names every hop between the workflow and the banned call" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def outer(x), do: inner(x)
        def inner(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.outer(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()

    assert diagnostic.message =~ "→ Helpers#{suffix}.outer/1  lib/orders.ex:5"
    assert diagnostic.message =~ "→ Helpers#{suffix}.inner/1  lib/helpers.ex:2"
    assert diagnostic.message =~ "→ :rand.uniform/1  lib/helpers.ex:3"
  end

  @tag :tmp_dir
  test "a private helper in the workflow's own module is followed" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          jitter(x)
        end

        defp jitter(x), do: :rand.uniform(x)
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.message =~ "Orders#{suffix}.jitter/1"
    assert diagnostic.message =~ ":rand.uniform/1"
  end

  @tag :tmp_dir
  test "a helper reached only from a step keeps its nondeterminism" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          choose(x)
        end

        defstep choose(x) do
          Helpers#{suffix}.pick(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert diagnostics() == []
  end

  @tag :tmp_dir
  test "a step is still reported for handing work to another process" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def fan_out(x), do: Task.async(fn -> x end)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defstep choose(x) do
          Helpers#{suffix}.fan_out(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.message =~ "reachable from a step body"
    assert diagnostic.message =~ "Task.async/1"
  end

  @tag :tmp_dir
  test "engine internals reached from a workflow are not followed" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Dbos.sleep(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert diagnostics() == []
  end

  @tag :tmp_dir
  test "a helper no workflow calls is not reported" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def unrelated(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          x
        end
      end
      """,
      "lib/orders.ex"
    )

    assert diagnostics() == []
  end

  @tag :tmp_dir
  test "a function captured and handed to another function is followed" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Enum.map([x], &Helpers#{suffix}.pick/1)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.message =~ ":rand.uniform/1"
  end

  @tag :tmp_dir
  test "a direct repo call reached through a helper is reported" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Repo#{suffix} do
        def insert(record), do: record
      end

      defmodule Helpers#{suffix} do
        def save(x), do: Repo#{suffix}.insert(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos, repo: Repo#{suffix}

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.save(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.message =~ "Repo#{suffix}.insert/1"
    assert diagnostic.message =~ "deftransaction"
  end

  @tag :tmp_dir
  test "@dbos_deterministic on a helper silences the workflow that reaches it" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        use Dbos

        @dbos_deterministic "reads a compile-time-frozen table"
        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.pick(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert diagnostics() == []
  end

  @tag :tmp_dir
  test "@dbos_deterministic that silences nothing is reported" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        use Dbos

        @dbos_deterministic "reads a compile-time-frozen table"
        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          x
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.severity == :hint
    assert diagnostic.message =~ "suppresses nothing"
    assert diagnostic.message =~ "Helpers#{suffix}.pick/1"
  end

  @tag :tmp_dir
  test "@dbos_deterministic without a reason fails to compile" do
    suffix = unique_suffix()

    assert_raise CompileError, ~r/takes a string explaining why/, fn ->
      compile(
        """
        defmodule Helpers#{suffix} do
          use Dbos

          @dbos_deterministic true
          def pick(x), do: :rand.uniform(x)
        end
        """,
        "lib/helpers.ex"
      )
    end
  end

  @tag :tmp_dir
  test "a module listed as trusted in the project is not followed" do
    suffix = compile_fixture()

    assert diagnostics(trusted: [Module.concat(["Helpers#{suffix}"])]) == []
  end

  @tag :tmp_dir
  test "a trusted entry that silences nothing is reported" do
    compile_fixture()

    assert Enum.any?(diagnostics(trusted: [SomeApp.Nowhere]), fn diagnostic ->
             diagnostic.severity == :hint and diagnostic.message =~ "SomeApp.Nowhere"
           end)
  end

  @tag :tmp_dir
  test "a dynamic dispatch reachable from a workflow is reported as a blind spot" do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def run(mod, x), do: apply(mod, :call, [x])
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.run(String, x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.severity == :hint
    assert diagnostic.message =~ "cannot see through a dynamic dispatch"
  end

  @tag :tmp_dir
  test "recompiling one helper still reports the workflow it is reached from", context do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        def pick(x), do: x + 1
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.pick(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    assert diagnostics() == []
    finish_run(context)

    start_run(context.app, context.manifest)

    compile(
      """
      defmodule Helpers#{suffix} do
        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    assert [diagnostic] = diagnostics()
    assert diagnostic.message =~ ~s|Orders#{suffix}.place/1  (workflow "place")|
    assert diagnostic.file == "lib/helpers.ex"
  end

  @tag :tmp_dir
  test "a module deleted from the application stops being reported" do
    suffix = compile_fixture()
    assert [_diagnostic] = diagnostics()

    State.flush([Module.concat(["Orders#{suffix}"])])

    assert diagnostics() == []
  end

  @tag :tmp_dir
  test "a manifest written by another format is reported as incomplete", context do
    finish_run(context)
    File.write!(context.manifest, :erlang.term_to_binary(%{format: 0}))
    start_run(context.app, context.manifest)

    assert State.manifest_stale?()
  end

  defp compile_fixture do
    suffix = unique_suffix()

    compile(
      """
      defmodule Helpers#{suffix} do
        @moduledoc false

        def pick(x), do: :rand.uniform(x)
      end
      """,
      "lib/helpers.ex"
    )

    compile(
      """
      defmodule Orders#{suffix} do
        use Dbos

        defworkflow place(x), name: "place" do
          Helpers#{suffix}.pick(x)
        end
      end
      """,
      "lib/orders.ex"
    )

    suffix
  end

  defp unique_suffix, do: System.unique_integer([:positive])

  defp start_run(app, manifest, opts \\ []) do
    {:ok, pid} = State.start_run([app: app, manifest_path: manifest] ++ opts)
    pid
  end

  defp finish_run(context) do
    State.flush(all_traced_modules())
    GenServer.stop(context.state)
  end

  defp all_traced_modules do
    State.calls() |> Enum.map(&elem(&1.from, 0)) |> Enum.uniq()
  end

  defp diagnostics(opts \\ []) do
    Analysis.diagnostics(State.calls(), State.entries(), traced_modules(), opts)
  end

  defp traced_modules do
    entry_modules =
      Enum.flat_map(State.entries(), fn
        %{mfa: {module, _fun, _arity}} -> [module]
        _other -> []
      end)

    (State.calls() |> Enum.map(&elem(&1.from, 0))) ++ entry_modules
  end

  defp compile(source, file) do
    previous = Code.get_compiler_option(:tracers)
    Code.put_compiler_option(:tracers, [Mix.Tasks.Compile.Dbos | previous])

    try do
      ExUnit.CaptureIO.capture_io(:stderr, fn ->
        send(self(), {:compiled, Code.compile_string(source, file)})
      end)

      receive do
        {:compiled, modules} -> Enum.map(modules, &elem(&1, 0))
      end
    after
      Code.put_compiler_option(:tracers, previous)
    end
  end
end
