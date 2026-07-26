defmodule AgentInbox.Repo.Migrations.AddDbos do
  use Ecto.Migration

  def up, do: Dbos.Migration.up()

  def down, do: Dbos.Migration.down()
end
