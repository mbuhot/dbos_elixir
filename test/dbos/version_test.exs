defmodule Dbos.VersionTest do
  use ExUnit.Case, async: true

  alias Dbos.Version

  test "computing the same modules twice yields the same digest" do
    assert Version.compute([Dbos.Config, Dbos.Status]) ==
             Version.compute([Dbos.Config, Dbos.Status])
  end

  test "different module sets yield different digests" do
    refute Version.compute([Dbos.Config]) == Version.compute([Dbos.Status])
  end

  test "module order does not affect the digest" do
    assert Version.compute([Dbos.Config, Dbos.Status]) ==
             Version.compute([Dbos.Status, Dbos.Config])
  end
end
