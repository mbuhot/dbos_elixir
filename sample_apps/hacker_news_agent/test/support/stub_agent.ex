defmodule HackerNewsAgent.StubAgent do
  @moduledoc "Deterministic, network-free `HackerNewsAgent.Agent` for tests. Bumps a `:persistent_term` counter per call so a test can prove a checkpointed call did not re-execute after a simulated crash."

  @behaviour HackerNewsAgent.Agent

  @impl true
  def evaluate(topic, _stories, _threads) do
    bump({:evaluate, topic})
    %{relevance: 8, insights: ["insight about #{topic}"]}
  end

  @impl true
  def should_continue?(topic, _findings, iteration, _max_iterations) do
    bump({:should_continue?, topic, iteration})
    true
  end

  @impl true
  def generate_follow_up(topic, _findings) do
    bump({:generate_follow_up, topic})
    "#{topic} follow up"
  end

  @impl true
  def synthesize(topic, findings) do
    bump({:synthesize, topic})
    "# Report on #{topic}\n\n#{length(findings)} rounds of findings."
  end

  @doc "How many times `call` (e.g. `{:evaluate, topic}`) has actually executed."
  def call_count(call), do: :persistent_term.get({__MODULE__, call}, 0)

  defp bump(call) do
    key = {__MODULE__, call}
    :persistent_term.put(key, :persistent_term.get(key, 0) + 1)
  end
end
