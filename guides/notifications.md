# Notifications

This guide covers the usage of notifications.

## When to Use

In general, notifications are most useful for queues with sporadic message
bursts. If a queue is expected to be empty for long periods of time, subscribing
to notifications can reduce polling overhead for consumers. If a queue has a
fairly steady flow of messages, then notifications may not be as useful.

## Throttling

In order to prevent high-volume message bursts from flooding a listener with
notifications, PGMQ supports millisecond-granularity throttling on a per-queue
basis. After a notification is sent, additional notifications within the
configured throttle interval will be suppressed. Throttling can be disabled by
setting the throttle interval to `0`.

`EctoPGMQ` enables notifications with an initially deferred trigger, so the
throttle is checked as a sending transaction commits and concurrent senders
don't wait for each other's transactions.

## Managing Notifications

The notification configuration for a queue can be managed with
`EctoPGMQ.create_queue/4` and `EctoPGMQ.update_queue/4` (or the corresponding
functions in the `EctoPGMQ.Migrations` module):

```elixir
# Create a queue with notifications enabled
EctoPGMQ.create_queue(MyApp.Repo, "my_queue", %{notifications: 250})

# Update the notification configuration for a queue
EctoPGMQ.update_queue(MyApp.Repo, "my_queue", %{notifications: 1_000})

# Disable notifications for a queue
EctoPGMQ.update_queue(MyApp.Repo, "my_queue", %{notifications: nil})
```

## Receiving Notifications

In order to receive notifications, a listener process must be started:

```elixir
{:ok, _listener_pid} =
  MyApp.Repo.config()
  |> Keyword.put(:name, MyApp.Listener)
  |> EctoPGMQ.Notifications.start_link()
```

Once a listener is started, other processes can subscribe to the notifications
for a particular queue:

```elixir
_subscription = EctoPGMQ.Notifications.subscribe(MyApp.Listener, "my_queue")
```

Once subscribed, a process will receive a message whenever a notification is
sent:

```elixir
defmodule MyApp.Subscriber do
  @moduledoc false

  use GenServer

  ...

  @doc false
  @impl GenServer
  def handle_info({:notification, _listener_pid, _subscription, channel, ""}, state) do
    IO.puts("Received a message from #{channel}")
    {:noreply, state}
  end
end
```
