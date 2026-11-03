if Code.ensure_loaded?(Broadway) do
  defmodule EctoPGMQ.Producer do
    @moduledoc """
    A `Broadway.Producer` implementation for PGMQ queues.

    This module requires the optional `Broadway` dependency.

    This module can be used in a Broadway pipeline like any other producer:

    ```elixir
    Broadway.start_link(MyBroadway,
      producer: [
        # Second tuple element contains producer options
        module: {EctoPGMQ.Producer, [repo: Repo, queue: "my_queue", ...]},
        ...
      ],
      ...
    )
    ```

    ## Notifications and Producer Polling

    `EctoPGMQ.Producer` supports poll-based, notification-based, and hybrid
    queue consumption.

    ### Poll-Based Consumption

    When using poll-based consumption, a producer will attempt to read messages
    whenever demand is received. If demand cannot be met, the producer will poll
    for messages until it can. The polling interval is determined by the
    `:read_interval` option. Sample producer options for poll-based consumption
    can be seen below:

    ```elixir
    [listener: nil, read_interval: 5_000, ...]
    ```

    > #### Info {: .info}
    >
    > This is the default approach to consumption.

    ### Notification-Based Consumption

    When using notification-based consumption, a producer will listen for
    notifications and only attempt to read messages if it has reason to believe
    that the queue is not empty. Depending on the characteristics of the queue,
    this approach to consumption may be less taxing on the repo connection pool
    than poll-based consumption. For more information about notifications and
    when to use them see `EctoPGMQ.Notifications`. Sample producer options for
    notification-based consumption can be seen below:

    ```elixir
    [listener: MyListener, read_interval: :infinity, ...]
    ```

    > #### Race Conditions {: .warning}
    >
    > A purely notification-based consumer will be subject to race conditions
    > when connecting/reconnecting to the DB. In practice, this means that there
    > is a risk of messages being available in the queue but not being read
    > until _more_ messages arrive and trigger another notification. For this
    > reason, [Hybrid Consumption](#hybrid-consumption) is preferred over
    > Notification-Based consumption.

    ### Hybrid Consumption

    Like with notification-based consumption, a producer using hybrid
    consumption will listen for notifications **BUT** it will also poll for
    messages to make notification race conditions less problematic. The
    `:read_interval` option then effectively becomes the maximum amount of time
    a visible message can wait in the queue if a notification is missed. Sample
    producer options for hybrid consumption can be seen below:

    ```elixir
    [listener: MyListener, read_interval: 300_000, ...]
    ```

    > #### Tip {: .tip}
    >
    > This is the preferred approach to consumption when leveraging
    > notifications.

    ## Acknowledgements

    `EctoPGMQ.Producer` supports a number of different acknowledgement actions
    (see `t:ack_action/0`). Default acknowledgement actions can be set for both
    successful and failed messages. Additionally, `EctoPGMQ.Producer` supports
    configuring individual message acknowledgements by calling
    `Broadway.Message.configure_ack/2` with the following options:

      * `:ack_action` - A required `t:ack_action/0` denoting how to acknowledge
        the message.

    When used together, default acknowledgement actions and individual message
    acknowledgement configuration can be used to implement more complex message
    handling. For example, an `EctoPGMQ.Producer` can use `:delete` as the
    default success acknowledgement, `:nothing` as the default failure
    acknowledgement, and a code snippet like the one below to implement a
    maximum number of attempts for each message:

    ```elixir
    alias Broadway.Message

    @max_attempts 3

    @impl Broadway
    def handle_message(_, message, _) do
      case do_process_pgmq_message(message.data) do
        :ok -> message
        {:error, reason} -> Message.failed(message, reason)
      end
    end

    @impl Broadway
    def handle_failed(messages, _) do
      Enum.map(messages, fn
        %{data: %{reads: r}} = msg when r < @max_attempts -> msg
        msg -> Message.configure_ack(msg, ack_action: :archive)
      end)
    end
    ```

    > #### Deleting on Read {: .warning}
    >
    > All acknowledgement configuration is effectively ignored when deleting
    > messages on read.

    ## Message Structure

    The `Broadway.Message` structs emitted by this module have the following
    structure:

      * `:data` - The `t:EctoPGMQ.Message.payload/0` of the message.

      * `:metadata` - A `t:map/0` containing the following fields:

          * `:queue` - The `t:EctoPGMQ.Queue.name/0` of the source queue

          * All `t:EctoPGMQ.Message.t/0` fields except `:payload`

    ## Options

    An `EctoPGMQ.Producer` can be started with the following options:

      * `:dynamic_repo` - An optional `t:atom/0` name or `t:pid/0` of a dynamic
        repo to use for all DB operations. For more information about dynamic
        repositories, see the
        [`Ecto` docs](https://hexdocs.pm/ecto/replicas-and-dynamic-repositories.html#dynamic-repositories).

      * `:listener` - An optional listener specification that can take any of
        the following forms:

          * An opts `t:keyword/0` to be passed to
            `EctoPGMQ.Notifications.start_link/1` to start a listener under
            Broadway's supervision tree. The `t:keyword/0` **MUST** contain a
            `:name` key.

          * An existing `t:EctoPGMQ.Notifications.listener/0`. This is useful
            for sharing a single listener (and, by proxy, a single Postgres
            connection) between multiple producers.

          * `nil` to not subscribe to notifications.

        Defaults to `nil`. For more information about configuring notifications
        for a producer, see
        [Notifications and Producer Polling](#notifications-and-producer-polling).
        For more information about notifications in general, see
        `EctoPGMQ.Notifications`.

      * `:on_failure` - An optional `t:ack_action/0` denoting the default
        acknowledgement for failed messages. Defaults to `:archive`. For more
        information about acknowledgements, see
        [Acknowledgements](#acknowledgements).

      * `:on_success` - An optional `t:ack_action/0` denoting the default
        acknowledgement for successful messages. Defaults to `:delete`. For
        more information about acknowledgements, see
        [Acknowledgements](#acknowledgements).

      * `:queue` - A required `t:EctoPGMQ.Queue.name/0` to read messages from.

      * `:read_interval` - An optional `t:Duration.t/0` or `t:timeout/0`
        denoting how long to wait between polls when there is outstanding demand
        (`:infinity` to disable polling, which requires a `:listener`). Defaults
        to `5_000`. For more information about configuring polling for a
        producer, see
        [Notifications and Producer Polling](#notifications-and-producer-polling).

      * `:read_opts` - An optional `t:EctoPGMQ.read_messages_opts/0` to be used
        when reading messages. Defaults to `[]`.

      * `:repo` - A required `t:Ecto.Repo.t/0` to be used for all DB operations.

      * `:visibility_timeout` - A required `t:EctoPGMQ.visibility_timeout/0` for
        read operations.
    """

    @behaviour Broadway.Acknowledger
    @behaviour Broadway.Producer

    use GenStage

    alias Broadway.Message
    alias EctoPGMQ.Notifications

    @opts_schema [
      :repo,
      :queue,
      :visibility_timeout,
      dynamic_repo: nil,
      listener: nil,
      on_success: :delete,
      on_failure: :archive,
      read_interval: 5_000,
      read_opts: []
    ]

    ################################
    # Public Types
    ################################

    @typedoc """
    An acknowledgement action for PGMQ messages.

    This can take any of the following forms:

      * `:delete` to delete messages from the queue.

      * `:archive` to move messages from the queue to the archive.

      * `:nothing` to retry messages once the visibility timeout expires.

      * `{:update_visibility_timeout, visibility_timeout}` where
        `visibility_timeout` is an updated `t:EctoPGMQ.visibility_timeout/0`.
    """
    @type ack_action ::
            :delete
            | :archive
            | :nothing
            | {:update_visibility_timeout, EctoPGMQ.delay()}

    ################################
    # Private Types
    ################################

    @typep ack_data :: %{
             :message_id => EctoPGMQ.Message.id(),
             :on_success => ack_action(),
             :on_failure => ack_action(),
             optional(:ack_action) => ack_action()
           }

    @typep ack_ref :: {Ecto.Repo.t(), dynamic_repo(), EctoPGMQ.Queue.name()}

    @typep dynamic_repo :: atom() | pid() | nil

    @typep state :: %{
             repo: Ecto.Repo.t(),
             dynamic_repo: dynamic_repo(),
             queue: EctoPGMQ.Queue.name(),
             visibility_timeout: EctoPGMQ.visibility_timeout(),
             listener: Notifications.listener(),
             on_success: ack_action(),
             on_failure: ack_action(),
             read_interval: Duration.t() | timeout(),
             read_opts: keyword(),
             subscription: Notifications.subscription() | nil,
             consuming?: boolean(),
             notified?: boolean(),
             read_timer: reference() | nil,
             worker: {Task.t(), pos_integer()} | nil,
             demand: non_neg_integer()
           }

    ################################
    # GenStage Callbacks
    ################################

    @doc false
    @impl GenStage
    @spec init(keyword()) :: {:producer, state()}
    def init(opts) do
      opts =
        opts
        |> Keyword.delete(:broadway)
        |> validate_opts()

      # Subscribe to PGMQ notifications when specified
      subscription =
        with listener when not is_nil(listener) <- Keyword.fetch!(opts, :listener) do
          queue = Keyword.fetch!(opts, :queue)
          {_, subscription, _} = Notifications.subscribe(listener, queue)
          subscription
        end

      # Schedule read when specified
      read_timer =
        case Keyword.fetch!(opts, :read_interval) do
          :infinity -> nil
          interval -> schedule_read(interval)
        end

      state =
        opts
        |> Map.new()
        |> Map.merge(%{
          subscription: subscription,
          read_timer: read_timer,
          consuming?: true,
          notified?: false,
          worker: nil,
          demand: 0
        })

      {:producer, state}
    end

    @doc false
    @impl GenStage
    @spec handle_demand(pos_integer(), state()) :: {:noreply, [Message.t()], state()}
    def handle_demand(incoming_demand, state) do
      demand = state.demand + incoming_demand

      worker =
        case state do
          # Immediately ready to start another worker
          %{consuming?: true, worker: nil} -> start_worker(state, demand)
          # Worker is busy or no reason to consume
          %{worker: worker} -> worker
        end

      state = %{state | demand: demand, worker: worker}
      {:noreply, [], state}
    end

    @doc false
    @impl GenStage
    @spec handle_info(term(), state()) :: {:noreply, [Message.t()], state()}
    def handle_info({task_ref, messages}, %{worker: {%{ref: task_ref}, _}} = state) do
      # Worker Task completion message handling
      received = length(messages)
      outstanding = max(state.demand - received, 0)
      {_, requested} = state.worker

      consuming? =
        cond do
          # Worker was able to meet demand and no more demand has been received
          outstanding == 0 -> true
          # Worker was not able to meet demand and no notification was received,
          # so wait for the next notification or read interval
          received < requested and not state.notified? -> false
          # Worker was able to meet demand but more demand has been received or
          # worker was not able to meet demand but a notification was received
          true -> true
        end

      worker =
        if outstanding > 0 and consuming? do
          start_worker(state, outstanding)
        end

      state = %{state | consuming?: consuming?, demand: outstanding, notified?: false, worker: worker}
      {:noreply, messages, state}
    end

    def handle_info({:notification, _, s, _, _}, %{subscription: s} = state) do
      # PGMQ notification message handling
      notified? = not is_nil(state.worker)

      worker =
        case state do
          # Start another worker to handle outstanding demand
          %{demand: demand, worker: nil} when demand > 0 -> start_worker(state, demand)
          # Worker is busy or no outstanding demand
          %{worker: worker} -> worker
        end

      state = %{state | consuming?: true, notified?: notified?, worker: worker}
      {:noreply, [], state}
    end

    def handle_info({:timeout, r, :read}, %{read_timer: r} = state) do
      # Read interval timer message handling
      {consuming?, worker} =
        case state do
          # Defer scheduled read if there is no outstanding demand
          %{demand: 0, worker: nil} ->
            {true, nil}

          # Start another worker to handle outstanding demand
          %{demand: demand, worker: nil} = state when demand > 0 ->
            worker = start_worker(state, demand)
            {state.consuming?, worker}

          # Outstanding demand but worker is busy
          state ->
            {state.consuming?, state.worker}
        end

      read_timer = schedule_read(state.read_interval)
      state = %{state | consuming?: consuming?, read_timer: read_timer, worker: worker}
      {:noreply, [], state}
    end

    def handle_info(_, state), do: {:noreply, [], state}

    ################################
    # Producer Callbacks
    ################################

    @doc false
    @impl Broadway.Producer
    @spec prepare_for_start(module(), keyword()) :: {[Supervisor.child_spec()], keyword()}
    def prepare_for_start(_, opts) do
      opts
      |> get_and_update_in(
        [:producer, :module],
        fn {module, producer_opts} ->
          # Get listener child spec and replace with name when applicable
          {listener_spec, producer_opts} =
            Keyword.get_and_update(producer_opts, :listener, fn
              listener_opts when is_list(listener_opts) ->
                name = Keyword.fetch!(listener_opts, :name)
                child_spec = %{id: name, start: {Notifications, :start_link, [listener_opts]}}
                {child_spec, name}

              listener ->
                {nil, listener}
            end)

          {listener_spec, {module, producer_opts}}
        end
      )
      |> case do
        {nil, opts} -> {[], opts}
        {child_spec, opts} -> {[child_spec], opts}
      end
    end

    @doc false
    @impl Broadway.Producer
    @spec prepare_for_draining(state()) :: {:noreply, [Message.t()], state()}
    def prepare_for_draining(state) do
      # Shutdown running worker when applicable
      if state.worker do
        {task, _} = state.worker
        Task.shutdown(task, :brutal_kill)
      end

      # Unsubscribe from PGMQ notifications when applicable
      if state.subscription do
        Notifications.unsubscribe(state.listener, state.subscription)
      end

      # Cancel read timer when applicable
      if state.read_timer do
        Process.cancel_timer(state.read_timer)
      end

      state = %{state | read_timer: nil, subscription: nil, worker: nil}
      {:noreply, [], state}
    end

    ################################
    # Acknowledger Callbacks
    ################################

    @doc false
    @impl Broadway.Acknowledger
    @spec ack(ack_ref(), [Message.t()], [Message.t()]) :: :ok
    def ack({repo, dynamic_repo, queue}, successful, failed) do
      successful
      |> Enum.map(&normalize_ack_data(&1, :on_success))
      |> Enum.concat(Enum.map(failed, &normalize_ack_data(&1, :on_failure)))
      |> Enum.group_by(fn {action, _} -> action end, fn {_, id} -> id end)
      |> Enum.each(fn
        {:delete, ids} ->
          with_dynamic_repo(repo, dynamic_repo, fn ->
            EctoPGMQ.delete_messages(repo, queue, ids)
          end)

        {:archive, ids} ->
          with_dynamic_repo(repo, dynamic_repo, fn ->
            EctoPGMQ.archive_messages(repo, queue, ids)
          end)

        {{:update_visibility_timeout, vt}, ids} ->
          with_dynamic_repo(repo, dynamic_repo, fn ->
            EctoPGMQ.update_messages(repo, queue, ids, %{visibility_timeout: vt})
          end)

        {:nothing, _} ->
          :ok
      end)
    end

    @doc false
    @impl Broadway.Acknowledger
    @spec configure(ack_ref(), ack_data(), keyword()) :: {:ok, ack_data()}
    def configure(_, ack_data, opts) do
      ack_action = Keyword.fetch!(opts, :ack_action)
      ack_data = Map.put(ack_data, :ack_action, ack_action)
      {:ok, ack_data}
    end

    ################################
    # Private API
    ################################

    defp validate_opts(opts) do
      opts = Keyword.validate!(opts, @opts_schema)

      # Validate presence of required keys
      Enum.each([:repo, :queue, :visibility_timeout], &Keyword.fetch!(opts, &1))

      # Validate that the producer polls or listens
      if is_nil(Keyword.fetch!(opts, :listener)) and Keyword.fetch!(opts, :read_interval) == :infinity do
        raise ArgumentError, "Producers without a listener require a finite read interval"
      end

      opts
    end

    defp schedule_read(%Duration{} = duration) do
      duration
      |> EctoPGMQ.DurationType.to_time(:millisecond)
      |> schedule_read()
    end

    defp schedule_read(time) when is_integer(time) do
      # Use `:erlang.start_timer/3` to include the timer ref in the message.
      :erlang.start_timer(time, self(), :read)
    end

    # credo:disable-for-lines:30 Credo.Check.Refactor.Nesting
    defp start_worker(state, quantity) do
      task =
        Task.async(fn ->
          with_dynamic_repo(state.repo, state.dynamic_repo, fn ->
            state.repo
            |> EctoPGMQ.read_messages(
              state.queue,
              state.visibility_timeout,
              quantity,
              state.read_opts
            )
            |> Enum.map(fn pgmq_message ->
              {payload, metadata} =
                pgmq_message
                |> Map.from_struct()
                |> Map.pop!(:payload)

              %Message{
                data: payload,
                metadata: Map.put(metadata, :queue, state.queue),
                acknowledger:
                  {__MODULE__, {state.repo, state.dynamic_repo, state.queue},
                   %{
                     message_id: pgmq_message.id,
                     on_success: state.on_success,
                     on_failure: state.on_failure
                   }}
              }
            end)
          end)
        end)

      {task, quantity}
    end

    defp with_dynamic_repo(_, nil, fun), do: fun.()

    defp with_dynamic_repo(repo, dynamic_repo, fun) do
      repo.put_dynamic_repo(dynamic_repo)
      fun.()
    end

    defp normalize_ack_data(message, default_field) do
      {_, _, ack_data} = message.acknowledger
      default = Map.fetch!(ack_data, default_field)
      ack_action = Map.get(ack_data, :ack_action, default)
      {ack_action, ack_data.message_id}
    end
  end
end
