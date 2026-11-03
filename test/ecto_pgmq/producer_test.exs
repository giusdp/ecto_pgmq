defmodule EctoPGMQ.ProducerTest do
  use EctoPGMQ.TestCase

  import Ecto.Query, only: [where: 3]

  alias EctoPGMQ.Message
  alias EctoPGMQ.TestPipeline

  @tag queue_attributes: %{notifications: 0}
  test "will consume messages from a queue", ctx do
    start_link_supervised!({TestPipeline, [queue: ctx.queue.name]})
    encoded_pid = TestPipeline.get_encoded_pid()

    # Validate that producer can read and acknowledge messages
    payloads = [Message.build(%{"encoded_pid" => encoded_pid})]
    response = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)
    assert_receive {:success, ^message_id}
    assert poll_queue_empty?(ctx.queue.name)
    refute archived_message_exists?(ctx.queue.name, message_id)

    # Validate that producer can configure acknowledgements
    payloads = [Message.build(%{"encoded_pid" => encoded_pid, "archive" => true})]
    response = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)
    assert_receive {:success, ^message_id}
    assert poll_queue_empty?(ctx.queue.name)
    assert archived_message_exists?(ctx.queue.name, message_id)

    # Validate that producer can acknowledge failed messages
    payloads = [Message.build(%{"encoded_pid" => encoded_pid, "fail" => true})]
    response = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)
    assert poll_queue_empty?(ctx.queue.name)
    assert archived_message_exists?(ctx.queue.name, message_id)
  end

  test "will poll a queue every read interval without a listener", ctx do
    handler = {__MODULE__, :reads}
    :telemetry.attach(handler, [:ecto_pgmq, :test_repo, :query], &__MODULE__.send_read/4, self())
    on_exit(fn -> :telemetry.detach(handler) end)
    start_link_supervised!({TestPipeline, [queue: ctx.queue.name, listener: nil, read_interval: 200]})

    # Validate that reads of an empty queue wait for the read interval
    assert_receive {:read, first}
    assert_receive {:read, second}
    assert_receive {:read, third}
    assert second - first >= 150
    assert third - second >= 150

    # Validate that producer reads messages sent between polls
    payloads = [Message.build(%{"encoded_pid" => TestPipeline.get_encoded_pid()})]
    response = EctoPGMQ.send_messages(Repo, ctx.queue.name, payloads)

    assert {:ok, [message_id]} = Map.fetch(response, ctx.queue.name)
    assert_receive {:success, ^message_id}
  end

  @tag :capture_log
  test "will not start without a listener or a read interval", ctx do
    opts = [queue: ctx.queue.name, listener: nil, read_interval: :infinity]
    assert {:error, _} = start_supervised({TestPipeline, opts})
  end

  @doc false
  def send_read(_, _, %{query: query}, test_pid) do
    if query =~ "pgmq.read(", do: send(test_pid, {:read, System.monotonic_time(:millisecond)})
  end

  ################################
  # Private API
  ################################

  defp poll_queue_empty?(queue) do
    with_poll(fn -> queue_empty?(Repo, queue) end)
  end

  defp archived_message_exists?(queue, message_id) do
    queue
    |> Message.archive_query()
    |> where([m], m.id == ^message_id)
    |> Repo.exists?()
  end
end
