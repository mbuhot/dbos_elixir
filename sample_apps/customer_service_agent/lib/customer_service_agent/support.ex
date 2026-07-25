defmodule CustomerServiceAgent.Support do
  @moduledoc """
  A customer service conversation: `handle_request/2` calls an LLM with tools bound, executes
  whichever tools it picks as steps, and loops until it has a final reply.

  `process_refund/1` backs the `request_refund` tool: it refunds immediately when the amount is
  at or under the approval threshold, and escalates larger amounts to `approval_workflow/1`,
  which emails an approver and durably waits — up to hours — for their decision before finalizing
  the refund.

  Every LLM call and every tool execution is a step or a child workflow: a crash anywhere in this
  conversation, including hours into an escalated wait, resumes from the last checkpoint with the
  full message history intact, because that history is nothing more than recorded step outputs.
  A refund never runs twice, no matter how many times the process handling it dies.
  """

  use Dbos

  @refund_approval_threshold 1000
  @approval_timeout_ms :timer.hours(6)
  @max_turns 8

  defworkflow handle_request(customer_id, message), name: "customer_request" do
    messages = [%{role: "user", content: message}]

    final_messages =
      Enum.reduce_while(1..@max_turns, messages, fn _turn, messages ->
        case call_llm(messages) do
          {:text, text} ->
            {:halt, messages ++ [%{role: "assistant", content: text}]}

          {:tool_calls, calls} ->
            assistant_message = %{role: "assistant", content: tool_use_blocks(calls)}
            tool_results = Enum.map(calls, &run_tool/1)
            {:cont, messages ++ [assistant_message | tool_results]}
        end
      end)

    %{customer_id: customer_id, reply: last_assistant_text(final_messages)}
  end

  defworkflow process_refund(order_id), name: "process_refund" do
    case get_purchase_step(order_id) do
      nil ->
        %{status: "error", message: "order #{order_id} not found"}

      %{amount: amount} = purchase when amount > @refund_approval_threshold ->
        {:ok, handle} =
          Dbos.start("approval_workflow", [purchase], workflow_id: approval_workflow_id(order_id))

        {:ok, result} = Dbos.await(handle)
        result

      _purchase ->
        update_purchase_status_step(order_id, :refunded)
    end
  end

  defworkflow approval_workflow(purchase), name: "approval_workflow" do
    send_approval_email_step(purchase)

    decision =
      try do
        Dbos.recv_message("approval_decision", @approval_timeout_ms)
      rescue
        Dbos.RecvTimeoutError -> "reject"
      end

    case decision do
      "approve" -> update_purchase_status_step(purchase.order_id, :refunded)
      _other -> update_purchase_status_step(purchase.order_id, :refund_rejected)
    end
  end

  @doc "Looks up a purchase order."
  defstep get_purchase_step(order_id) do
    IO.puts("[step] get_purchase_step(#{order_id})")
    bump_execution_count({:get_purchase, order_id})
    Process.sleep(100)
    CustomerServiceAgent.OrderStore.get(order_id)
  end

  @doc "Sets a purchase order's status."
  defstep update_purchase_status_step(order_id, status) do
    IO.puts("[step] update_purchase_status_step(#{order_id}, #{status})")
    bump_execution_count({:update_purchase_status, order_id, status})
    CustomerServiceAgent.OrderStore.update_status(order_id, status)
  end

  @doc "Notifies a human approver that `purchase` needs a refund decision."
  defstep send_approval_email_step(purchase) do
    IO.puts(
      "[step] send_approval_email_step: approve order #{purchase.order_id} with " <>
        "Dbos.send_message(#{inspect(approval_workflow_id(purchase.order_id))}, \"approval_decision\", \"approve\")"
    )

    :ok
  end

  @doc "One LLM turn over the conversation so far, with tools bound."
  defstep call_llm(messages) do
    IO.puts("[step] call_llm (#{length(messages)} messages so far)")
    llm().chat(messages, tools())
  end

  @doc "The deterministic workflow id an escalated refund's approval wait runs under."
  def approval_workflow_id(order_id), do: "approval-#{order_id}"

  @doc "How many times a step keyed by `call` has actually executed."
  def execution_count(call), do: :persistent_term.get({__MODULE__, call}, 0)

  defp bump_execution_count(call) do
    key = {__MODULE__, call}
    :persistent_term.put(key, :persistent_term.get(key, 0) + 1)
  end

  defp run_tool(%{id: id, name: "get_purchase", input: %{"order_id" => order_id}}) do
    tool_result_message(id, get_purchase_step(order_id))
  end

  defp run_tool(%{id: id, name: "request_refund", input: %{"order_id" => order_id}}) do
    tool_result_message(id, process_refund(order_id))
  end

  defp tool_result_message(tool_use_id, result) do
    %{
      role: "user",
      content: [%{type: "tool_result", tool_use_id: tool_use_id, content: inspect(result)}]
    }
  end

  defp tool_use_blocks(calls) do
    Enum.map(calls, fn call -> %{type: "tool_use", id: call.id, name: call.name, input: call.input} end)
  end

  defp last_assistant_text(messages) do
    messages
    |> Enum.reverse()
    |> Enum.find_value("I was unable to complete this request in time.", fn
      %{role: "assistant", content: content} when is_binary(content) -> content
      _other -> nil
    end)
  end

  defp llm, do: Application.get_env(:customer_service_agent, :llm, CustomerServiceAgent.ClaudeLLM)

  defp tools do
    [
      %{
        name: "get_purchase",
        description: "Look up a purchase order by id.",
        input_schema: %{
          type: "object",
          properties: %{order_id: %{type: "integer"}},
          required: ["order_id"]
        }
      },
      %{
        name: "request_refund",
        description:
          "Request a refund for a purchase order. Refunds over $#{@refund_approval_threshold} " <>
            "require human approval and may take a while.",
        input_schema: %{
          type: "object",
          properties: %{order_id: %{type: "integer"}},
          required: ["order_id"]
        }
      }
    ]
  end
end
