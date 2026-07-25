defmodule Dbos.StepNamesTest do
  use ExUnit.Case, async: true

  alias Dbos.StepNames

  test "reserved names match the upstream DBOS.* strings verbatim" do
    assert StepNames.get_result() == "DBOS.getResult"
    assert StepNames.sleep() == "DBOS.sleep"
    assert StepNames.recv() == "DBOS.recv"
    assert StepNames.set_event() == "DBOS.setEvent"
    assert StepNames.get_event() == "DBOS.getEvent"
  end

  test "patch/1 prefixes the caller-supplied patch name" do
    assert StepNames.patch("add_field") == "DBOS.patch-add_field"
  end
end
