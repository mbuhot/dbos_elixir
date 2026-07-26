defmodule Mix.Tasks.Compile.Dbos do
  @shortdoc "Reports nondeterministic calls reachable from a workflow body"

  @moduledoc """
  Whole-application determinism checking for `dbos`.

  A compilation tracer records a function-level call graph while `compile.elixir` runs, and once
  the application is built the compiler walks that graph forward from every `defworkflow`,
  `defstep` and `deftransaction` body, reporting the banned constructs it reaches. This catches
  what the `defworkflow` macro cannot: a violation that lives in a helper function, in any module
  of the application.

  Install it ahead of the standard compilers, in `:dev` and `:test`:

      def project do
        [
          compilers: [:dbos] ++ Mix.compilers(),
          ...
        ]
      end

  Findings are reported as warnings. `mix compile --warnings-as-errors` turns them into a failed
  compile, which is the recommended CI setting. Literal violations inside a workflow body remain
  a hard `CompileError` from the macro itself.

  ## Escape hatches

  A single function, in a module that has `use Dbos`:

      @dbos_deterministic "reads a compile-time-frozen config map"
      def lookup_region(code), do: :persistent_term.get({:regions, code})

  A module or MFA the project does not own, in `mix.exs`:

      dbos: [trusted: [MyApp.PureHelpers, {Some.Dep, :fetch_config, 1}]]

  Both make the function a cut vertex: it is not descended into and its own calls are not
  reported. An entry that suppresses nothing is itself reported, so the list does not rot.

  ## Limits

  Dynamic dispatch is invisible. `apply/3` with a computed module, protocol dispatch and a
  behaviour callback reached through a variable module are all reported as blind spots at most,
  never followed. A dependency is an opaque leaf: its calls are checked against the rule table,
  its internals are not traced. A branch that never runs is still reported.

  A compile run that recompiles the tracer's own modules loses the events raised while their old
  code is purged, which under-reports until the next full recompile. Only the `dbos` repository
  itself can be in that position.
  """

  use Mix.Task.Compiler

  alias Dbos.Compiler.Analysis
  alias Dbos.Compiler.State
  alias Dbos.Determinism.Rules

  @recursive true

  @apply_mfas [
    {Kernel, :apply, 2},
    {Kernel, :apply, 3},
    {:erlang, :apply, 2},
    {:erlang, :apply, 3}
  ]

  @switches [force: :boolean, warnings_as_errors: :boolean]

  @tracer :dbos_determinism_tracer

  @tracer_modules [__MODULE__, Dbos.Compiler.State, Dbos.Determinism.Rules]

  @impl Mix.Task.Compiler
  def run(argv) do
    {opts, _argv, _errors} = OptionParser.parse(argv, switches: @switches)

    if rebuilding_own_tracer?(opts), do: skip(), else: start(opts)
  end

  defp skip do
    Mix.shell().info([
      :yellow,
      "dbos: skipping determinism checking for this run, which rebuilds the tracer itself. " <>
        "Compile without --force for a report.",
      :reset
    ])

    {:noop, []}
  end

  defp rebuilding_own_tracer?(opts) do
    opts[:force] == true and own_beam?()
  end

  defp own_beam? do
    case :code.which(__MODULE__) do
      path when is_list(path) ->
        Path.expand(Path.dirname(to_string(path))) == Path.expand(Mix.Project.compile_path())

      _other ->
        false
    end
  end

  defp start(opts) do
    State.start_run(force: opts[:force] || false)
    :persistent_term.put({__MODULE__, :foreign}, foreign_modules())

    Mix.Task.Compiler.after_compiler(:elixir, &remove_tracer/1)
    Mix.Task.Compiler.after_compiler(:app, &check(&1, opts))

    Code.put_compiler_option(:tracers, [ensure_shim() | Code.get_compiler_option(:tracers)])
    {:ok, []}
  end

  defp ensure_shim do
    unless Code.ensure_loaded?(@tracer), do: Code.compile_quoted(shim())
    @tracer
  end

  defp shim do
    quote do
      defmodule unquote(@tracer) do
        @moduledoc false

        def trace(event, env) do
          Mix.Tasks.Compile.Dbos.trace(event, env)
        rescue
          error in UndefinedFunctionError ->
            if error.module in unquote(@tracer_modules),
              do: :ok,
              else: reraise(error, __STACKTRACE__)
        end
      end
    end
  end

  @impl Mix.Task.Compiler
  def manifests, do: [State.manifest_path()]

  @impl Mix.Task.Compiler
  def clean, do: File.rm_rf!(State.manifest_path())

  @doc false
  def trace(event, env)

  def trace({event, meta, module, fun, arity}, %{module: caller} = env)
      when not is_nil(caller) and
             event in [:remote_function, :imported_function, :remote_macro, :imported_macro] do
    State.initialize_module(caller)
    record(env, meta, {module, fun, arity}, mode(event, env))
  end

  def trace({event, meta, fun, arity}, %{module: caller} = env)
      when not is_nil(caller) and event in [:local_function, :local_macro] do
    State.initialize_module(caller)
    record(env, meta, {caller, fun, arity}, mode(event, env))
  end

  def trace(_event, %{module: caller}) when not is_nil(caller) do
    State.initialize_module(caller)
  end

  def trace(_event, _env), do: :ok

  defp mode(event, _env) when event in [:remote_macro, :imported_macro, :local_macro],
    do: :compile

  defp mode(_event, %{function: nil}), do: :compile
  defp mode(_event, _env), do: :runtime

  defp record(env, meta, {module, fun, arity} = to, mode) do
    if record?(module, fun, arity) do
      State.add_call(env.module, %{
        from: from_node(env),
        to: to,
        file: Path.relative_to_cwd(env.file),
        line: Keyword.get(meta, :line),
        mode: mode
      })
    end

    :ok
  end

  defp from_node(%{module: module, function: nil}), do: {module, :__module_body__, 0}
  defp from_node(%{module: module, function: {fun, arity}}), do: {module, fun, arity}

  defp record?(module, fun, arity) do
    not foreign?(module) or
      Rules.match_mfa(module, fun, arity, :workflow) != nil or
      Rules.match_mfa(module, fun, arity, :step) != nil or
      {module, fun, arity} in @apply_mfas
  end

  defp foreign?(module) do
    case :persistent_term.get({__MODULE__, :foreign}, nil) do
      nil -> false
      set -> MapSet.member?(set, module)
    end
  end

  defp remove_tracer(status) do
    tracers = Enum.reject(Code.get_compiler_option(:tracers), &(&1 == @tracer))
    Code.put_compiler_option(:tracers, tracers)
    status
  end

  defp check({status, diagnostics}, opts) when status in [:ok, :noop] do
    app = Mix.Project.config()[:app]
    modules = app_modules(app)
    State.flush(modules)

    found =
      Analysis.diagnostics(State.calls(), State.entries(), modules, trusted: trusted_config()) ++
        stale_manifest_diagnostics()

    Enum.each(found, &print/1)
    {result_status(status, found, opts), diagnostics ++ found}
  end

  defp check(result, _opts), do: result

  defp result_status(status, found, opts) do
    if opts[:warnings_as_errors] == true and Enum.any?(found, &(&1.severity == :warning)) do
      :error
    else
      status
    end
  end

  defp stale_manifest_diagnostics do
    if State.manifest_stale?() do
      [
        %Mix.Task.Compiler.Diagnostic{
          compiler_name: "dbos",
          file: "mix.exs",
          position: 0,
          severity: :hint,
          message:
            "the dbos determinism manifest was missing or written in another format, so only " <>
              "the modules compiled in this run were checked. Run `mix compile --force` for a " <>
              "complete report.",
          details: %{rule: :stale_manifest}
        }
      ]
    else
      []
    end
  end

  defp app_modules(app) do
    Application.unload(app)
    Application.load(app)
    Application.spec(app, :modules) || []
  end

  defp trusted_config do
    Mix.Project.config() |> Keyword.get(:dbos, []) |> Keyword.get(:trusted, [])
  end

  defp foreign_modules do
    app = Mix.Project.config()[:app]

    deps =
      if function_exported?(Mix.Project, :deps_apps, 0), do: Mix.Project.deps_apps(), else: []

    loaded = Enum.map(Application.loaded_applications(), &elem(&1, 0))

    modules =
      for other <- Enum.uniq(deps ++ loaded),
          other != app,
          _ = Application.load(other),
          module <- Application.spec(other, :modules) || [],
          do: module

    MapSet.new(modules ++ :erlang.pre_loaded())
  end

  defp print(diagnostic) do
    Mix.shell().info([
      colour(diagnostic.severity),
      "#{diagnostic.severity}: ",
      :reset,
      diagnostic.message,
      "\n\n  ",
      :cyan,
      location(diagnostic),
      :reset,
      "\n"
    ])
  end

  defp location(%{file: file, position: position}) when position in [nil, 0], do: file
  defp location(%{file: file, position: position}), do: "#{file}:#{position}"

  defp colour(:warning), do: :yellow
  defp colour(:error), do: :red
  defp colour(_severity), do: :cyan
end
