defmodule HackerNewsAgent.ClaudeAgent do
  @moduledoc "LLM-backed `HackerNewsAgent.Agent`: prompts Claude and parses its strict-JSON replies into research judgments."

  @behaviour HackerNewsAgent.Agent

  alias HackerNewsAgent.Claude

  @impl true
  def evaluate(topic, stories, threads) do
    system = "You are a research analyst evaluating Hacker News search results for relevance and insight."

    prompt = """
    Topic: #{topic}

    Stories found:
    #{format_stories(stories)}

    Discussion excerpts:
    #{format_threads(threads)}

    Respond with strict JSON only, matching this shape:
    {"relevance": <number 1-10>, "insights": ["...", "..."]}
    """

    %{"relevance" => relevance, "insights" => insights} = system |> Claude.complete(prompt) |> decode_json!()
    %{relevance: relevance / 1, insights: insights}
  end

  @impl true
  def should_continue?(topic, findings, iteration, max_iterations) do
    system = "You are deciding whether a Hacker News research loop needs another iteration."

    prompt = """
    Topic: #{topic}
    Iteration: #{iteration} of #{max_iterations}
    Findings so far: #{inspect(findings)}

    Should research continue? Only continue if relevance scores are strong (>= 5) and new
    information keeps emerging. Respond with strict JSON only: {"continue": true|false}
    """

    system |> Claude.complete(prompt) |> decode_json!() |> Map.fetch!("continue")
  end

  @impl true
  def generate_follow_up(topic, findings) do
    system = "You generate concise Hacker News search queries."

    prompt = """
    Topic: #{topic}
    Findings so far: #{inspect(findings)}

    Suggest one focused, 2-4 word follow-up search query covering an angle not yet explored.
    Respond with strict JSON only: {"query": "..."}
    """

    system |> Claude.complete(prompt) |> decode_json!() |> Map.fetch!("query")
  end

  @impl true
  def synthesize(topic, findings) do
    system = "You write clear, well-organized research reports from collected findings."

    prompt = """
    Topic: #{topic}
    Findings: #{inspect(findings)}

    Write a concise markdown report synthesizing these findings.
    """

    Claude.complete(system, prompt)
  end

  defp format_stories(stories) do
    Enum.map_join(stories, "\n", fn story ->
      "- ##{story.id} #{story.title} (#{story.points} points, #{story.num_comments} comments)"
    end)
  end

  defp format_threads(threads) do
    Enum.map_join(threads, "\n\n", fn thread ->
      comments = Enum.map_join(thread.comments, "\n", &"  - #{&1}")
      "#{thread.title}:\n#{comments}"
    end)
  end

  defp decode_json!(text) do
    text
    |> strip_code_fence()
    |> Jason.decode!()
  end

  defp strip_code_fence(text) do
    text
    |> String.trim()
    |> String.replace_prefix("```json", "")
    |> String.replace_prefix("```", "")
    |> String.replace_suffix("```", "")
    |> String.trim()
  end
end
