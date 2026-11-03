defmodule EctoPGMQ.NotificationsTest do
  use EctoPGMQ.TestCase

  alias Ecto.Adapters.SQL.Sandbox
  alias EctoPGMQ.Notifications

  @moduletag queue_attributes: %{notifications: 250}

  setup_all do
    listener = EctoPGMQ.Listener

    # Start a shared listener process for tests in this module
    start_supervised!(%{
      id: listener,
      start: {Notifications, :start_link, [[{:name, listener} | Repo.config()]]}
    })

    %{listener: listener}
  end

  describe "subscribe/3" do
    test "will subscribe to queue insert notifications", ctx do
      # Validate that subscription was successful
      assert {:ok, subscription, channel} = Notifications.subscribe(ctx.listener, ctx.queue.name)

      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      # Validate that a notification is received
      assert_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end

  describe "insert notifications" do
    test "will not make concurrent senders wait for each other", ctx do
      assert {:ok, subscription, channel} = Notifications.subscribe(ctx.listener, ctx.queue.name)
      test_pid = self()

      # Send from a transaction that stays open until told to commit
      sender =
        Task.async(fn ->
          :ok = Sandbox.checkout(Repo, sandbox: false)

          Repo.transaction(fn ->
            EctoPGMQ.send_messages(Repo, ctx.queue.name, [Message.build(%{"id" => 1})])
            send(test_pid, :sent)
            assert_receive :commit
          end)
        end)

      assert_receive :sent

      # Validate that another sender doesn't wait for the open transaction
      Repo.transaction(fn ->
        Repo.query!("SET LOCAL lock_timeout = '500ms'")
        EctoPGMQ.send_messages(Repo, ctx.queue.name, [Message.build(%{"id" => 2})])
      end)

      send(sender.pid, :commit)
      Task.await(sender)

      # Validate that a notification is received once the senders commit
      assert_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end

  describe "unsubscribe/3" do
    test "will unsubscribe from queue insert notifications", ctx do
      # Validate that subscription was successful
      assert {:ok, subscription, channel} = Notifications.subscribe(ctx.listener, ctx.queue.name)

      # Validate that unsubscription was successful
      assert Notifications.unsubscribe(ctx.listener, subscription) == :ok

      messages = [Message.build(%{"id" => 1}), Message.build(%{"id" => 2})]
      EctoPGMQ.send_messages(Repo, ctx.queue.name, messages)

      # Validate that a notification is NOT received
      refute_receive {:notification, _, ^subscription, ^channel, ""}
    end
  end
end
