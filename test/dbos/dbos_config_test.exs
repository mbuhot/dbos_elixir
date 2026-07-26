defmodule Dbos.DbosConfigTest do
  use ExUnit.Case, async: true

  test "config/1 raises Dbos.NotStartedError when no engine by that name has started" do
    assert_raise Dbos.NotStartedError, fn ->
      Dbos.config(Module.concat(__MODULE__, :"NeverStarted#{System.unique_integer([:positive])}"))
    end
  end

  test "config/0 raises Dbos.NotStartedError when the default engine has not started" do
    assert_raise Dbos.NotStartedError, fn -> Dbos.config() end
  end

  test "put_config/1 then config/1 round-trips the config for a named engine" do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    config = %Dbos.Config{name: name, db: Dbos.DB.Postgrex, conn: :fake_conn}

    Dbos.put_config(config)

    assert Dbos.config(name) == config
  end

  describe "put_engine/1" do
    test "a process with no engine set resolves to Dbos" do
      assert Dbos.current_engine() == Dbos
    end

    test "config/0 resolves the engine this process was pointed at" do
      name = Module.concat(__MODULE__, :"Ambient#{System.unique_integer([:positive])}")
      config = %Dbos.Config{name: name, db: Dbos.DB.Postgrex, conn: :fake_conn}
      Dbos.put_config(config)

      Dbos.put_engine(name)

      assert Dbos.current_engine() == name
      assert Dbos.config() == config
    end

    test "a spawned task inherits it through $callers" do
      name = Module.concat(__MODULE__, :"Inherited#{System.unique_integer([:positive])}")
      Dbos.put_engine(name)

      assert Task.async(fn -> Dbos.current_engine() end) |> Task.await() == name
    end

    test "an unrelated process is unaffected, having no caller to inherit from" do
      name = Module.concat(__MODULE__, :"Isolated#{System.unique_integer([:positive])}")
      Dbos.put_engine(name)

      test_pid = self()
      spawn(fn -> send(test_pid, {:resolved, Dbos.current_engine()}) end)

      assert_receive {:resolved, Dbos}
    end
  end
end
