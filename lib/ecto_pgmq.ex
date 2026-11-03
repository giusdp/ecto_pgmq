defmodule EctoPGMQ do
  @moduledoc """
  An opinionated PGMQ client for Elixir that builds on top of `Ecto` and the
  `Ecto.Adapters.Postgres` adapter.
  """

  alias Ecto.Repo
  alias EctoPGMQ.Binding
  alias EctoPGMQ.DurationType
  alias EctoPGMQ.PGMQ
  alias EctoPGMQ.Queue
  alias EctoPGMQ.Throttle

  require EctoPGMQ.Message, as: Message

  ################################
  # Types
  ################################

  @typedoc """
  A delay before a message becomes visible.

  This can take any of the following forms:

    * A `t:Duration.t/0` denoting the time to wait before a message becomes
      visible.

    * An `t:integer/0` denoting the time (in seconds) to wait before a message
      becomes visible.

    * A `t:DateTime.t/0` denoting when a message should become visible.

  For more information about this type, see `t:EctoPGMQ.PGMQ.delay/0`.
  """
  @type delay :: Duration.t() | PGMQ.delay()

  @typedoc "A message destination."
  @type destination :: Queue.name() | {:queue, Queue.name()} | {:routing_key, PGMQ.routing_key()}

  @typedoc """
  Message update attributes.

  The following attributes are supported:

    * `:visibility_timeout` - A required `t:delay/0` for the messages.
  """
  @type message_update_attributes :: %{visibility_timeout: delay()}

  @typedoc """
  The minimum time between notifications.

  This can take either of the following forms:

    * A `t:Duration.t/0` denoting the minimum time between notifications.

    * A `t:non_neg_integer/0` denoting the minimum time (in milliseconds)
      between notifications.

  For more information about notifications, see `EctoPGMQ.Notifications`.

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.throttle_interval/0`.
  """
  @type notification_throttle :: Duration.t() | PGMQ.throttle_interval()

  @typedoc """
  A queue partition configuration.

  A partition configuration is a tuple containing two elements: the partition
  interval and the retention interval.

  Both elements can take either of the following forms:

    * A `t:Duration.t/0` denoting a time-based interval.

    * A `t:pos_integer/0` denoting a message-based interval.

  For more information about partitioning, see
  [Partitioning](`m:EctoPGMQ.PGMQ#partitioning`).

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.partition_interval/0` and
  `t:EctoPGMQ.PGMQ.retention_interval/0`.
  """
  @type partition_config :: {PGMQ.partition_interval(), PGMQ.retention_interval()}

  @typedoc """
  A message polling configuration.

  A polling configuration is a tuple containing two elements: the poll interval
  and the poll timeout.

  Both elements can take either of the following forms:

    * A `t:Duration.t/0` denoting a length of time.

    * A `t:pos_integer/0` denoting a length of time. The unit for the poll
      interval is milliseconds and the unit for the timeout is seconds.

  For more information about polling, see
  [Polling](`m:EctoPGMQ.PGMQ#polling`).

  For more information about this type, see `t:EctoPGMQ.PGMQ.poll_interval/0`
  and `t:EctoPGMQ.PGMQ.poll_timeout/0`.
  """
  @type poll_config :: {Duration.t() | PGMQ.poll_interval(), Duration.t() | PGMQ.poll_timeout()}

  @typedoc """
  Queue creation attributes.

  The following attributes are supported:

    * `:bindings` - An optional `t:list/0` of `t:EctoPGMQ.Binding.pattern/0`
      for the queue. Defaults to `[]`. For more information about bindings, see
      [Message Routing](message_routing.md).

    * `:message_groups?` - An optional `t:boolean/0` denoting whether or not the
      queue should be optimized for FIFO message group reads. Defaults to
      `false`. For more information about FIFO message groups, see
      [FIFO Message Groups](fifo_message_groups.md).

    * `:notifications` - An optional `t:notification_throttle/0` for the queue
      or `nil` to leave notifications disabled. Defaults to `nil`. For more
      information about notifications, see `EctoPGMQ.Notifications`.

    * `:partitions` - An optional `t:partition_config/0` for the queue or
      `nil` to disable partitioning. This option is ignored for unlogged queues.
      Defaults to `nil`. For more information about partitioning, see
      [Partitioning](#partitioning).

    * `:unlogged?` - An optional `t:boolean/0` denoting whether or not the queue
      should be unlogged. Defaults to `false`.
  """
  @type queue_create_attributes :: %{
          optional(:bindings) => [Binding.pattern()],
          optional(:message_groups?) => boolean(),
          optional(:notifications) => notification_throttle() | nil,
          optional(:partitions) => partition_config() | nil,
          optional(:unlogged?) => boolean()
        }

  @typedoc """
  Queue update attributes.

  The following attributes are supported:

    * `:bindings` - An optional `t:list/0` of `t:EctoPGMQ.Binding.pattern/0`
      for the queue. For more information about bindings, see
      [Message Routing](message_routing.md).

       > #### Replace Behavior {: .warning}
       >
       > When given, this attribute will **REPLACE** the existing bindings for
       > the queue. This means that this attribute must contain **ALL** of the
       > desired bindings for the queue, not just a delta.

    * `:message_groups?` - `true` to optimize the queue for FIFO message group
      reads. Note that `true` is the only valid value because this operation
      cannot be undone. For more information about FIFO message groups, see
      [FIFO Message Groups](fifo_message_groups.md).

    * `:notifications` - An optional `t:notification_throttle/0` for the queue or
      `nil` to disable notifications. For more information about notifications,
      see `EctoPGMQ.Notifications`.
  """
  @type queue_update_attributes :: %{
          optional(:bindings) => [Binding.pattern()],
          optional(:message_groups?) => true,
          optional(:notifications) => notification_throttle() | nil
        }

  @typedoc """
  Options for reading messages.

  In addition to the standard [query options](`m:EctoPGMQ.PGMQ#query-options`),
  messages can be read with the following options:

    * `:delete?` - An optional `t:boolean/0` denoting whether or not to delete
      messages immediately after reading them. Defaults to `false`. For more
      information, see `EctoPGMQ.PGMQ.pop/4`.

    * `:message_grouping` - An optional value specifying how to handle message
      groups when reading messages. Possible values are `:round_robin`,
      `:throughput_optimized`, or `nil` to ignore message groups when reading.
      This option is ignored when deleting on read. Defaults to `nil`. For more
      information about FIFO message groups, see
      [FIFO Message Groups](fifo_message_groups.md).

    * `:payload_type` - An optional `t:EctoPGMQ.Message.payload_type/0` for the
      message payloads. Defaults to `:map`.

    * `:polling` - An optional `t:poll_config/0` for the read operation or nil
      to disable polling. This option is ignored when deleting on read. Defaults
      to `nil`. For more information about polling, see [Polling](#polling).
  """
  @type read_messages_opts :: [
          {:delete?, boolean()}
          | {:polling, poll_config() | nil}
          | {:payload_type, Message.payload_type()}
          | {:message_grouping, :head | :round_robin | :throughput_optimized | nil}
          | PGMQ.query_opt()
        ]

  @typedoc """
  The time from now that a message is invisible.

  This can take either of the following forms:

    * A `t:Duration.t/0` denoting how long a message is invisible.

    * An `t:integer/0` denoting how long (in seconds) a message is invisible.

  For more information about this type, see
  `t:EctoPGMQ.PGMQ.visibility_timeout/0`.
  """
  @type visibility_timeout :: Duration.t() | PGMQ.visibility_timeout()

  ################################
  # Queue API
  ################################

  @doc """
  Lists all queues.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> [%Queue{} | _] = EctoPGMQ.all_queues(Repo)
  """
  @doc group: "Queue API"
  @spec all_queues(Repo.t()) :: [Queue.t()]
  @spec all_queues(Repo.t(), [PGMQ.query_opt()]) :: [Queue.t()]
  def all_queues(repo, opts \\ []), do: repo.all(Queue.query(), opts)

  @doc """
  Creates a queue with the given name.

  To create a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.create_queue/2`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> queue = EctoPGMQ.create_queue(Repo, "my_unpartitioned_queue", %{notifications: 1_000})
      iex> %Queue{notifications: %Throttle{}} = queue

      iex> queue = EctoPGMQ.create_queue(Repo, "my_partitioned_queue", %{partitions: {10_000, 100_000}})
      iex> %Queue{partitioned?: true} = queue

      iex> partitions = {Duration.new!(hour: 1), Duration.new!(day: 1)}
      iex> queue = EctoPGMQ.create_queue(Repo, "my_partitioned_queue", %{partitions: partitions})
      iex> %Queue{partitioned?: true} = queue

      iex> queue = EctoPGMQ.create_queue(Repo, "my_unlogged_queue", %{unlogged?: true})
      iex> %Queue{unlogged?: true} = queue
  """
  @doc group: "Queue API"
  @spec create_queue(Repo.t(), Queue.name()) :: Queue.t()
  @spec create_queue(Repo.t(), Queue.name(), queue_create_attributes()) :: Queue.t()
  @spec create_queue(Repo.t(), Queue.name(), queue_create_attributes(), [PGMQ.query_opt()]) :: Queue.t()
  def create_queue(repo, queue, attributes \\ %{}, opts \\ []) do
    notification_throttle = attributes[:notifications]
    partition_config = attributes[:partitions]

    transaction(
      repo,
      fn ->
        cond do
          # Create unlogged queue when specified
          Map.get(attributes, :unlogged?, false) ->
            PGMQ.create_unlogged(repo, queue, opts)

          # Create non-partitioned queue when specified
          is_nil(partition_config) ->
            PGMQ.create_non_partitioned(repo, queue, opts)

          # Create partitioned queue
          true ->
            {partition, retention} = partition_config
            PGMQ.create_partitioned(repo, queue, partition, retention, opts)
        end

        # Enable notifications when specified
        if notification_throttle do
          enable_notifications(repo, queue, notification_throttle, opts)
        end

        # Create FIFO index when specified
        if Map.get(attributes, :message_groups?, false) do
          PGMQ.create_fifo_index(repo, queue, opts)
        end

        # Create bindings when specified
        attributes
        |> Map.get(:bindings, [])
        |> Enum.each(&PGMQ.bind_topic(repo, &1, queue, opts))

        # Fetch created queue record
        repo.get!(Queue.query(), queue, opts)
      end,
      opts
    )
  end

  @doc """
  Drops the given queue.

  To drop a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.drop_queue/2`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> EctoPGMQ.drop_queue(Repo, "my_queue")
      :ok
  """
  @doc group: "Queue API"
  @spec drop_queue(Repo.t(), Queue.name()) :: :ok
  @spec drop_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: :ok
  defdelegate drop_queue(repo, queue, opts \\ []), to: PGMQ

  @doc """
  Gets the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> %Queue{} = EctoPGMQ.get_queue(Repo, "my_queue")

      iex> EctoPGMQ.get_queue(Repo, "my_non_existent_queue")
      nil
  """
  @doc group: "Queue API"
  @spec get_queue(Repo.t(), Queue.name()) :: Queue.t() | nil
  @spec get_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: Queue.t() | nil
  def get_queue(repo, queue, opts \\ []), do: repo.get(Queue.query(), queue, opts)

  @doc """
  Purges the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> messages = [Message.build(%{"id" => 1})]
      iex> EctoPGMQ.send_messages(Repo, "my_queue", messages)
      iex> EctoPGMQ.purge_queue(Repo, "my_queue")
      1
  """
  @doc group: "Queue API"
  @spec purge_queue(Repo.t(), Queue.name()) :: PGMQ.purged_messages()
  @spec purge_queue(Repo.t(), Queue.name(), [PGMQ.query_opt()]) :: PGMQ.purged_messages()
  defdelegate purge_queue(repo, queue, opts \\ []), to: PGMQ

  @doc """
  Updates the given queue.

  > #### Unexpected Results {: .warning}
  >
  > Because the underlying tables are owned by PGMQ, this function avoids row
  > locks so as not to disturb any internal PGMQ processes. As a result, if
  > multiple processes attempt to update a queue simultaneously, they may get
  > unexpected results.

  To update a queue in an `Ecto.Migration`, see
  `EctoPGMQ.Migrations.update_queue/3`.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> throttle = Duration.new!(second: 5)
      iex> queue = EctoPGMQ.update_queue(Repo, "my_queue", %{notifications: throttle})
      iex> %Queue{notifications: %Throttle{}} = queue
  """
  @doc group: "Queue API"
  @spec update_queue(Repo.t(), Queue.name(), queue_update_attributes()) :: Queue.t()
  @spec update_queue(Repo.t(), Queue.name(), queue_update_attributes(), [PGMQ.query_opt()]) :: Queue.t()
  def update_queue(repo, queue, attributes, opts \\ []) do
    # credo:disable-for-lines:50 Credo.Check.Refactor.Nesting
    transaction(
      repo,
      fn ->
        # Fetch existing queue record
        queue = repo.get!(Queue.query(), queue, opts)

        # Update notifications when specified
        case {queue.notifications, Map.fetch(attributes, :notifications)} do
          # Enable notifications when not already enabled
          {nil, {:ok, notifications}} when not is_nil(notifications) ->
            enable_notifications(repo, queue.name, notifications, opts)

          # Disable notifications when specified
          {%Throttle{}, {:ok, nil}} ->
            PGMQ.disable_notify_insert(repo, queue.name, opts)

          # Update notification throttle when specified
          {%Throttle{}, {:ok, notifications}} ->
            notifications = maybe_to_time(notifications, :millisecond)
            PGMQ.update_notify_insert(repo, queue.name, notifications, opts)

          # Do nothing when no change is specified
          _ ->
            :ok
        end

        # Create FIFO index when specified
        if Map.get(attributes, :message_groups?, false) do
          PGMQ.create_fifo_index(repo, queue.name, opts)
        end

        # Update bindings when specified
        with {:ok, bindings} <- Map.fetch(attributes, :bindings) do
          existing = Enum.map(queue.bindings, & &1.pattern)
          bindings = Enum.uniq(bindings)

          Enum.each(bindings -- existing, fn pattern ->
            PGMQ.bind_topic(repo, pattern, queue.name, opts)
          end)

          Enum.each(existing -- bindings, fn pattern ->
            PGMQ.unbind_topic(repo, pattern, queue.name, opts)
          end)
        end

        # Fetch updated queue record
        repo.get!(Queue.query(), queue.name, opts)
      end,
      opts
    )
  end

  ################################
  # Message API
  ################################

  @doc """
  Archives the given messages from the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> messages = [Message.build(%{"id" => 1})]
      iex> %{"my_queue" => ids} = EctoPGMQ.send_messages(Repo, "my_queue", messages)
      iex> EctoPGMQ.archive_messages(Repo, "my_queue", ids)
      :ok
  """
  @doc group: "Message API"
  @spec archive_messages(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec archive_messages(Repo.t(), Queue.name(), [Message.id()], [PGMQ.query_opt()]) :: :ok
  defdelegate archive_messages(repo, queue, message_ids, opts \\ []), to: PGMQ, as: :archive

  @doc """
  Deletes the given messages from the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> messages = [Message.build(%{"id" => 1})]
      iex> %{"my_queue" => ids} = EctoPGMQ.send_messages(Repo, "my_queue", messages)
      iex> EctoPGMQ.delete_messages(Repo, "my_queue", ids)
      :ok
  """
  @doc group: "Message API"
  @spec delete_messages(Repo.t(), Queue.name(), [Message.id()]) :: :ok
  @spec delete_messages(Repo.t(), Queue.name(), [Message.id()], [PGMQ.query_opt()]) :: :ok
  defdelegate delete_messages(repo, queue, message_ids, opts \\ []), to: PGMQ, as: :delete

  @doc """
  Reads messages from the given queue.

  ## Options

  See `t:read_messages_opts/0` for information about the options supported by
  this function.

  ## Examples

      iex> vt = Duration.new!(second: 5)
      iex> messages = [Message.build(%{"id" => 1})]
      iex> EctoPGMQ.send_messages(Repo, "my_queue", messages)
      iex> [%Message{reads: 1}] = EctoPGMQ.read_messages(Repo, "my_queue", vt, 2)
  """
  @doc group: "Message API"
  @spec read_messages(Repo.t(), Queue.name(), visibility_timeout(), PGMQ.quantity()) :: [Message.t()]
  @spec read_messages(
          Repo.t(),
          Queue.name(),
          visibility_timeout(),
          PGMQ.quantity(),
          read_messages_opts()
        ) :: [Message.t()]
  # credo:disable-for-lines:40 Credo.Check.Refactor.CyclomaticComplexity
  def read_messages(repo, queue, visibility_timeout, quantity, opts \\ []) do
    {delete?, opts} = Keyword.pop(opts, :delete?, false)
    {poll_config, opts} = Keyword.pop(opts, :polling)
    {grouping, opts} = Keyword.pop(opts, :message_grouping)
    {payload_type, opts} = Keyword.pop(opts, :payload_type, :map)
    visibility_timeout = maybe_to_time(visibility_timeout, :second)

    query =
      case {delete?, poll_config, grouping} do
        # Immediately delete messages when specified
        {true, _, _} ->
          PGMQ.pop_query(queue, quantity, payload_type)

        # Otherwise, delegate based on poll config and grouping
        {false, nil, nil} ->
          PGMQ.read_query(queue, visibility_timeout, quantity, %{}, payload_type)

        {false, nil, :head} ->
          PGMQ.read_grouped_head_query(queue, visibility_timeout, quantity, payload_type)

        {false, nil, :round_robin} ->
          PGMQ.read_grouped_rr_query(queue, visibility_timeout, quantity, payload_type)

        {false, nil, :throughput_optimized} ->
          PGMQ.read_grouped_query(queue, visibility_timeout, quantity, payload_type)

        {false, poll_config, nil} ->
          {interval, timeout} = parse_poll_config(poll_config)
          PGMQ.read_with_poll_query(queue, visibility_timeout, quantity, timeout, interval, %{}, payload_type)

        {false, poll_config, :head} ->
          {interval, timeout} = parse_poll_config(poll_config)
          PGMQ.read_grouped_head_with_poll_query(queue, visibility_timeout, quantity, timeout, interval, payload_type)

        {false, poll_config, :round_robin} ->
          {interval, timeout} = parse_poll_config(poll_config)
          PGMQ.read_grouped_rr_with_poll_query(queue, visibility_timeout, quantity, timeout, interval, payload_type)

        {false, poll_config, :throughput_optimized} ->
          {interval, timeout} = parse_poll_config(poll_config)
          PGMQ.read_grouped_with_poll_query(queue, visibility_timeout, quantity, timeout, interval, payload_type)
      end

    # We wrap read operations in a transaction so that a post-query processing
    # failure (ie loading custom payloads) does NOT update messages.
    transaction(repo, fn -> repo.all(query, opts) end, opts)
  end

  @doc """
  Sends messages to the given queue.

  ## Options

  In addition to the standard [query options](`m:EctoPGMQ.PGMQ#query-options`),
  this function also supports the following options:

    * `:delay` - An optional `t:delay/0` for the messages. Defaults to `0`.

    * `:payload_type` - An optional `t:EctoPGMQ.Message.payload_type/0` for the
      message payloads. Defaults to `:map`.

  ## Examples

      iex> messages = [Message.build(%{"id" => 1})]
      iex> delay = Duration.new!(hour: 1)
      iex> %{"my_queue" => [id]} = EctoPGMQ.send_messages(Repo, "my_queue", messages, delay: delay)
      iex> is_integer(id)
      true

      iex> messages = [Message.build(%{"id" => 1})]
      iex> EctoPGMQ.PGMQ.bind_topic(Repo, "#", "my_queue")
      iex> destination = {:routing_key, "my.routing.key"}
      iex> %{"my_queue" => [id]} = EctoPGMQ.send_messages(Repo, destination, messages)
      iex> is_integer(id)
      true
  """
  @doc group: "Message API"
  @spec send_messages(Repo.t(), destination(), [Message.message()]) :: PGMQ.queue_message_ids()
  @spec send_messages(
          Repo.t(),
          destination(),
          [Message.message()],
          [{:delay, delay()} | {:payload_type, Message.payload_type()} | PGMQ.query_opt()]
        ) :: PGMQ.queue_message_ids()
  def send_messages(repo, destination, messages, opts \\ []) do
    {delay, opts} = Keyword.pop(opts, :delay, 0)
    delay = maybe_to_time(delay, :second)
    {payload_type, opts} = Keyword.pop(opts, :payload_type, :map)
    {payloads, headers} = Message.to_pgmq_payloads_and_headers(messages, payload_type)

    destination
    |> case do
      queue when is_binary(queue) -> {:queue, queue}
      destination -> destination
    end
    |> case do
      {:queue, queue} ->
        message_ids = PGMQ.send_batch(repo, queue, payloads, headers, delay, opts)
        %{queue => message_ids}

      {:routing_key, routing_key} ->
        PGMQ.send_batch_topic(repo, routing_key, payloads, headers, delay, opts)
    end
  end

  @doc """
  Updates the given messages in the given queue.

  ## Options

  This function supports the standard
  [query options](`m:EctoPGMQ.PGMQ#query-options`).

  ## Examples

      iex> messages = [Message.build(%{"id" => 1})]
      iex> visibility_timeout = Duration.new!(minute: 5)
      iex> %{"my_queue" => ids} = EctoPGMQ.send_messages(Repo, "my_queue", messages)
      iex> EctoPGMQ.update_messages(Repo, "my_queue", ids, %{visibility_timeout: visibility_timeout})
      :ok
  """
  @doc group: "Message API"
  @spec update_messages(Repo.t(), Queue.name(), [Message.id()], message_update_attributes()) :: :ok
  @spec update_messages(Repo.t(), Queue.name(), [Message.id()], message_update_attributes(), [PGMQ.query_opt()]) :: :ok
  def update_messages(repo, queue, message_ids, %{visibility_timeout: visibility_timeout}, opts \\ []) do
    visibility_timeout = maybe_to_time(visibility_timeout, :second)
    PGMQ.set_vt(repo, queue, message_ids, visibility_timeout, opts)
    :ok
  end

  ################################
  # Private API
  ################################

  defp transaction(repo, fun, opts) do
    {:ok, result} =
      repo.transact(
        fn ->
          result = fun.()
          {:ok, result}
        end,
        opts
      )

    result
  end

  defp enable_notifications(repo, queue, throttle, opts) do
    throttle = maybe_to_time(throttle, :millisecond)
    PGMQ.enable_notify_insert(repo, queue, throttle, opts)
    defer_notifications(repo, queue, opts)
  end

  # Recreate PGMQ's insert trigger as initially deferred so that its throttle
  # row is locked as senders commit rather than for their whole transaction
  defp defer_notifications(repo, queue, opts) do
    table = "#{PGMQ.schema()}.#{PGMQ.queue_table_name(queue)}"
    trigger = "trigger_notify_queue_insert_listeners"

    repo.query!("DROP TRIGGER #{trigger} ON #{table}", [], opts)

    repo.query!(
      """
      CREATE CONSTRAINT TRIGGER #{trigger} AFTER INSERT ON #{table}
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
      EXECUTE PROCEDURE #{PGMQ.schema()}.notify_queue_listeners('#{queue}')
      """,
      [],
      opts
    )

    :ok
  end

  defp parse_poll_config({interval, timeout}) do
    interval = maybe_to_time(interval, :millisecond)
    timeout = maybe_to_time(timeout, :second)
    {interval, timeout}
  end

  defp maybe_to_time(%Duration{} = duration, unit) do
    DurationType.to_time(duration, unit)
  end

  defp maybe_to_time(time, _) when is_integer(time), do: time
end
