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
end
