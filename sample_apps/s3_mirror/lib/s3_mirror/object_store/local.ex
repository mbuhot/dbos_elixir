defmodule S3Mirror.ObjectStore.Local do
  @moduledoc """
  `S3Mirror.ObjectStore` over a local directory tree. The store is `%{root: path}`; a key maps
  to `Path.join(root, key)`. Runs with no cloud account, so it is the sample's default path.
  """

  @behaviour S3Mirror.ObjectStore

  @impl true
  def list_keys(%{root: root}, prefix) do
    root
    |> ensure_dir!()
    |> Path.join("**")
    |> Path.wildcard(match_dot: true)
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.relative_to(&1, root))
    |> Enum.filter(&String.starts_with?(&1, prefix))
    |> Enum.sort()
    |> then(&{:ok, &1})
  end

  @impl true
  def read(%{root: root}, key) do
    File.read(Path.join(root, key))
  end

  @impl true
  def write(%{root: root}, key, data) do
    path = Path.join(root, key)
    File.mkdir_p!(Path.dirname(path))
    File.write(path, data)
  end

  @impl true
  def exists?(%{root: root}, key) do
    File.regular?(Path.join(root, key))
  end

  defp ensure_dir!(root) do
    File.mkdir_p!(root)
    root
  end
end
