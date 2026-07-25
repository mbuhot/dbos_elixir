defmodule HackerNewsAgent.Research do
  @moduledoc """
  Researches a topic by iteratively searching Hacker News, reading comment threads, and asking an
  LLM to evaluate findings and decide whether another round is worth it. Every search, every
  thread read, and every LLM call is a step: a crash mid-research resumes at the first
  uncompleted step instead of repeating the expensive, non-idempotent calls already checkpointed.

  The loop's iteration count is derived entirely from `continue_research/4`'s recorded step
  output, never from wall-clock time or randomness — replaying this workflow after a crash takes
  the exact same number of iterations it took the first time, because every branch it takes is a
  branch on an already-checkpointed step result.
  """

  use Dbos

  @default_max_iterations 3
  @stories_per_iteration 5

  defworkflow research(topic, max_iterations \\ @default_max_iterations), name: "hn_research" do
    state = %{query: topic, iteration: 1, findings: []}

    final =
      Enum.reduce_while(1..max_iterations, state, fn iteration, state ->
        stories = search_stories(state.query)
        story_ids = stories |> Enum.take(@stories_per_iteration) |> Enum.map(& &1.id)
        threads = Enum.map(story_ids, &read_thread/1)
        evaluation = evaluate_findings(topic, stories, threads)
        findings = state.findings ++ [evaluation]
        next_state = %{state | findings: findings, iteration: iteration}

        if iteration >= max_iterations do
          {:halt, next_state}
        else
          if continue_research(topic, findings, iteration, max_iterations) do
            next_query = next_query_step(topic, findings)
            {:cont, %{next_state | query: next_query, iteration: iteration + 1}}
          else
            {:halt, next_state}
          end
        end
      end)

    synthesize_report(topic, final.findings)
  end

  @doc "Searches Hacker News for `query`."
  defstep search_stories(query) do
    IO.puts("[step] search_stories(#{inspect(query)})")
    hn_client().search_stories(query)
  end

  @doc "Reads `story_id`'s full comment thread."
  defstep read_thread(story_id) do
    IO.puts("[step] read_thread(#{story_id})")
    hn_client().read_thread(story_id)
  end

  @doc "Asks the LLM to extract relevance and insights from this round's stories and threads."
  defstep evaluate_findings(topic, stories, threads) do
    IO.puts("[step] evaluate_findings(#{inspect(topic)})")
    agent().evaluate(topic, stories, threads)
  end

  @doc "Asks the LLM whether the research findings so far justify another iteration."
  defstep continue_research(topic, findings, iteration, max_iterations) do
    IO.puts("[step] continue_research (iteration #{iteration}/#{max_iterations})")
    agent().should_continue?(topic, findings, iteration, max_iterations)
  end

  @doc "Asks the LLM for the next iteration's follow-up query."
  defstep next_query_step(topic, findings) do
    IO.puts("[step] next_query_step(#{inspect(topic)})")
    agent().generate_follow_up(topic, findings)
  end

  @doc "Asks the LLM to synthesize every round's findings into a final report."
  defstep synthesize_report(topic, findings) do
    IO.puts("[step] synthesize_report(#{inspect(topic)})")
    agent().synthesize(topic, findings)
  end

  defp agent, do: Application.get_env(:hacker_news_agent, :agent, HackerNewsAgent.ClaudeAgent)
  defp hn_client, do: Application.get_env(:hacker_news_agent, :hn_client, HackerNewsAgent.HnClient.Algolia)
end
