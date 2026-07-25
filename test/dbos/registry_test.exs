defmodule Dbos.RegistryTest do
  use ExUnit.Case, async: true

  alias Dbos.Registry

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    start_supervised!({Registry, name: name})
    {:ok, name: name}
  end

  test "register then lookup returns the mfa", %{name: name} do
    Registry.register(name, "process_order", {Checkout, :process_order, 2})
    assert Registry.lookup(name, "process_order") == {:ok, {Checkout, :process_order, 2}}
  end

  test "lookup of an unregistered name returns :error", %{name: name} do
    assert Registry.lookup(name, "unknown") == :error
  end

  test "re-registering the identical mfa is a no-op", %{name: name} do
    Registry.register(name, "process_order", {Checkout, :process_order, 2})
    Registry.register(name, "process_order", {Checkout, :process_order, 2})
    assert Registry.lookup(name, "process_order") == {:ok, {Checkout, :process_order, 2}}
  end

  test "registering the same name with a different mfa raises", %{name: name} do
    Registry.register(name, "process_order", {Checkout, :process_order, 2})

    assert_raise RuntimeError, fn ->
      Registry.register(name, "process_order", {Checkout, :other, 1})
    end
  end

  test "registered_names/1 lists every registered name", %{name: name} do
    Registry.register(name, "a", {Checkout, :a, 0})
    Registry.register(name, "b", {Checkout, :b, 0})
    assert Enum.sort(Registry.registered_names(name)) == ["a", "b"]
  end

  test "modules/1 lists the distinct modules registered", %{name: name} do
    Registry.register(name, "a", {Checkout, :a, 0})
    Registry.register(name, "b", {Refund, :b, 0})
    assert Enum.sort(Registry.modules(name)) == [Checkout, Refund]
  end

  test "name_for_mfa/2 finds the name registered to an mfa" do
    other = Module.concat(__MODULE__, :"WorkflowsEngine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Registry, name: other, workflows: [{"process_order", {Checkout, :process_order, 2}}]},
      id: :workflows_engine
    )

    assert Registry.name_for_mfa(other, {Checkout, :process_order, 2}) == {:ok, "process_order"}
    assert Registry.name_for_mfa(other, {Checkout, :other, 1}) == :error
  end

  test "starting with a conflicting workflows list raises during init" do
    name = Module.concat(__MODULE__, :"ConflictEngine#{System.unique_integer([:positive])}")

    Process.flag(:trap_exit, true)

    assert {:error, _reason} =
             Registry.start_link(
               name: name,
               workflows: [
                 {"process_order", {Checkout, :process_order, 2}},
                 {"process_order", {Checkout, :other, 1}}
               ]
             )
  end

  test "each engine owns its own table, independent of other engines", %{name: name} do
    other_name = Module.concat(__MODULE__, :"OtherEngine#{System.unique_integer([:positive])}")
    start_supervised!({Registry, name: other_name}, id: :other)

    Registry.register(name, "shared", {Checkout, :process_order, 2})
    assert Registry.lookup(other_name, "shared") == :error
  end
end
