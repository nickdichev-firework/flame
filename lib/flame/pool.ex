defmodule FLAME.Pool.RunnerState do
  @moduledoc false

  @type t :: %__MODULE__{}
  defstruct count: nil, pid: nil, monitor_ref: nil, schedulable?: true
end

defmodule FLAME.Pool.WaitingState do
  @moduledoc false

  @type t :: %__MODULE__{}
  defstruct from: nil, monitor_ref: nil, deadline: nil
end

defmodule FLAME.Pool.Caller do
  @moduledoc false

  defstruct checkout_ref: nil, monitor_ref: nil, runner_ref: nil
end

defmodule FLAME.Pool do
  @moduledoc """
  Manages a pool of `FLAME.Runner` processes.

  Pools support elastic growth and shrinking of the number of runners.

  ## Examples

      children = [
        ...,
        {FLAME.Pool, name: MyRunner, min: 1, max: 10}
      ]

  See `start_link/1` for supported options.

  ## TODO
  [ ] interface to configure min/max at runtime

  """
  use GenServer

  alias FLAME.{Pool, Runner, Queue, CodeSync}
  alias FLAME.Pool.{RunnerState, WaitingState, Caller}

  require Logger

  @default_strategy {Pool.PerRunnerMaxConcurrencyStrategy, [max_concurrency: 100]}
  @boot_timeout 30_000
  @idle_shutdown_after 30_000
  @async_boot_debounce 1_000

  @type t :: %__MODULE__{}
  defstruct name: nil,
            runner_sup: nil,
            task_sup: nil,
            terminator_sup: nil,
            child_placement_sup: nil,
            boot_max_concurrency: 10,
            boot_timeout: nil,
            idle_shutdown_after: nil,
            min: nil,
            min_idle_shutdown_after: nil,
            min_blocking_threshold: nil,
            max: nil,
            strategy: nil,
            callers: %{},
            waiting: Queue.new(),
            runners: %{},
            pending_runners: %{},
            runner_opts: [],
            on_grow_start: nil,
            on_grow_end: nil,
            on_shrink: nil,
            async_boot_timer: nil,
            track_resources: false,
            base_sync_stream: nil

  def child_spec(opts) do
    %{
      id: {__MODULE__, Keyword.fetch!(opts, :name)},
      start: {FLAME.Pool.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc """
  Starts a pool of runners.

  ## Options

    * `:name` - The name of the pool, for example: `MyApp.FFMPegRunner`

    * `:min` - The minimum number of runners to keep in the pool at all times.
      For "scale to zero" behavior you may pass `0`. When starting as a flame child,
      the `:min` will be forced to zero to avoid recursively starting backend resources.

    * `:max` - The maximum number of runners to elastically grow to in the pool.

    * `:strategy` - The strategy to use. Defaults to `FLAME.Pool.PerRunnerMaxConcurrencyStrategy`.

    * `:single_use` - if `true`, runners will be terminated after each call completes.
      Defaults `false`.

    * `:backend` - The backend to use. Defaults to the configured `:flame, :backend` or
      `FLAME.LocalBackend` if not configured.

    * `:log` - The log level to use for verbose logging. Defaults to `false`.

    * `:timeout` - The time to allow functions to execute on a remote node. Defaults to 30 seconds.
      This value is also used as the default `FLAME.call/3` timeout for the caller.

    * `:boot_timeout` - The time to allow for booting and connecting to a remote node.
      Defaults to 30 seconds.

    * `:boot_max_concurrency` - The maximum number of concurrent booting runners.
      Defaults to 10.

    * `:shutdown_timeout` - The time to allow for graceful shutdown on the remote node.
      Defaults to 30 seconds.

    * `:idle_shutdown_after` - The amount of time and function check to idle a remote node
      down after a period of inactivity. Defaults to 30 seconds. A tuple may also be passed
      to check a specific condition, for example:

          {10_000, fn -> Supervisor.which_children(MySup) == []}

    * `:min_idle_shutdown_after` - The same behavior of `:idle_shutdown_after`, but applied
      to the the `:min` pool runners. Defaults to `:infinity`.

    * `min_blocking_threshold` - The number of min runners that are booted while blocking the
      parent application startup. Defaults to the value of `min`.

    * `:on_grow_start` - The optional function to be called when the pool starts booting a new
      runner beyond the configured `:min`. The function receives a map with the following metadata:

        * `:name` - The name of the pool
        * `:count` - The number of runners the pool is attempting to grow to
        * `:pid` - The pid of the async process that is booting the new runner

     * `:on_grow_end` - The optional 2-arity function to be called when the pool growth process completes.
      The 2-arity function receives either `:ok` or `{:exit, reason}`, and map with the following metadata:

        * `:name` - The name of the pool
        * `:count` - The number of runners the pool is now at
        * `:pid` - The pid of the async process that attempted to boot the new runner

    * `:on_shrink` - The optional function to be called when the pool shrinks.
      The function receives a map with the following metadata:

        * `:name` - The name of the pool
        * `:count` - The number of runners the pool is attempting to shrink to

    * `:track_resources` - When true, traverses the returned results from FLAME
     operations looking for resources that implement the `FLAME.Trackable` protocol
     and make sure the FLAME node does not terminate until the tracked resources are removed.
     Defaults `false`.

    * `:code_sync` – The optional list of options to enable copying and syncing code paths
      from the parent node to the runner node. Disabled by default. The options are:

      * `:start_apps` – Either a boolean or a list of specific OTP application names to start
        when the runner boots. When `true`, all applications currently running on the parent node
        are sent to the runner node to be started. Defaults to `false`. When set to `true`,
        `copy_apps` will also be set to `true` if not explicitly set to `false`.

      * `:copy_apps` – The boolean flag to copy all the application artifacts and their beam
        files from the parent node to the runner node on boot. Defaults `false`.
        When passing `start_apps: true`, automatically sets `copy_paths: true`.

      * `:copy_paths` – The list of arbitrary paths to copy from the parent node to the runner
        node on boot. Defaults to `[]`.

      * `:sync_beams` – A list of specific beam code paths to sync to the runner node. Useful
        when you want to sync specific beam code paths from the parent after sending all code
        paths from `:copy_apps` on initial boot. For example, with `copy_apps: true`,
        and `sync_beams: ["/home/app/.cache/.../ebin"]`, all the code from the parent will be
        copied on boot, but only the specific beam files will be synced on subsequent calls.
        With `copy_apps: false`, and `sync_beams: ["/home/app/.cache/.../ebin"]`,
        only the specific beam files will be synced on boot and for subsequent calls.
        Defaults to `[]`.

      * `:verbose` – If `true`, the pool will log verbose information about the code sync process.
        Defaults to `false`.

      * `:compress` – If `true`, the copy_apps, copy_paths, and sync_beams will be compressed
        before sending. Provides savings in network payload size at the cost of CPU time.
        Defaults to `true`.

      For example, in [Livebook](https://livebook.dev/), to start a pool with code sync enabled:

          Mix.install([:kino, :flame])

          Kino.start_child!(
            {FLAME.Pool,
              name: :my_flame,
              code_sync: [
                start_apps: true,
                sync_beams: [Path.join(System.tmp_dir!(), "livebook_runtime")]
              ],
              min: 1,
              max: 1,
              strategy: {FLAME.Pool.PerRunnerMaxConcurrencyStrategy, [max_concurrency: 10]},
              backend: {FLAME.FlyBackend,
                cpu_kind: "performance", cpus: 4, memory_mb: 8192,
                token: System.fetch_env!("FLY_API_TOKEN"),
                env: Map.take(System.get_env(), ["LIVEBOOK_COOKIE"]),
              },
              idle_shutdown_after: :timer.minutes(5)}
          )
  """
  def start_link(opts) do
    Keyword.validate!(opts, [
      :name,
      :runner_sup,
      :task_sup,
      :terminator_sup,
      :child_placement_sup,
      :idle_shutdown_after,
      :min,
      :min_idle_shutdown_after,
      :min_blocking_threshold,
      :max,
      :strategy,
      :backend,
      :log,
      :single_use,
      :timeout,
      :boot_timeout,
      :boot_max_concurrency,
      :shutdown_timeout,
      :on_grow_start,
      :on_grow_end,
      :on_shrink,
      :code_sync,
      :track_resources
    ])

    Keyword.validate!(opts[:code_sync] || [], [
      :copy_apps,
      :copy_paths,
      :sync_beams,
      :start_apps,
      :verbose,
      :compress,
      :chunk_size
    ])

    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Calls a function in a remote runner for the given `FLAME.Pool`.

  See `FLAME.call/3` for more information.
  """
  def call(name, func, opts \\ []) when is_function(func, 0) and is_list(opts) do
    caller_pid = self()
    do_call(name, func, caller_pid, opts)
  end

  defp do_call(name, func, caller_pid, opts) when is_pid(caller_pid) do
    caller_checkout!(name, opts, :call, [name, func, opts], fn runner_pid,
                                                               remaining_timeout,
                                                               track_resources ->
      opts =
        opts
        |> Keyword.put_new(:timeout, remaining_timeout)
        |> Keyword.put_new(:track_resources, track_resources)

      {:cancel, :ok, Runner.call(runner_pid, caller_pid, func, opts)}
    end)
  end

  @doc """
  Casts a function to a remote runner for the given `FLAME.Pool`.

  See `FLAME.cast/3` for more information.
  """
  def cast(name, func, opts) when is_function(func, 0) and is_list(opts) do
    %{task_sup: task_sup} = lookup_meta(name)

    caller_pid = self()
    opts = Keyword.put_new(opts, :timeout, :infinity)

    # we don't care about the result so don't copy it back to the caller
    wrapped = fn ->
      func.()
      :ok
    end

    {:ok, _pid} =
      Task.Supervisor.start_child(task_sup, fn -> do_call(name, wrapped, caller_pid, opts) end)

    :ok
  end

  @doc """
  See `FLAME.place_child/3` for more information.
  """
  def place_child(name, child_spec, opts) do
    caller_checkout!(name, opts, :place_child, [name, child_spec, opts], fn runner_pid,
                                                                            remaining_timeout,
                                                                            track_resources ->
      place_opts =
        opts
        |> Keyword.put(:track_resources, track_resources)
        |> Keyword.put_new(:timeout, remaining_timeout)
        |> Keyword.put_new(:link, true)

      case Runner.place_child(runner_pid, child_spec, place_opts) do
        {{:ok, child_pid}, _trackable_pids = []} = result ->
          # we are placing the link back on the parent node, but we are protected
          # from racing the link on the child FLAME because the terminator on
          # the remote flame is monitoring the caller and will terminator the child
          # if we go away
          if Keyword.fetch!(place_opts, :link), do: Process.link(child_pid)
          {:cancel, {:replace, [child_pid]}, result}

        :ignore ->
          {:cancel, :ok, :ignore}

        {:error, _reason} = result ->
          {:cancel, :ok, result}
      end
    end)
  end

  defp caller_checkout!(name, opts, fun_name, args, func) do
    %{boot_timeout: boot_timeout, track_resources: track_resources} = lookup_meta(name)
    timeout = opts[:timeout] || boot_timeout
    track_resources = Keyword.get(opts, :track_resources, track_resources)
    pid = Process.whereis(name) || exit({:noproc, {__MODULE__, fun_name, args}})
    ref = Process.monitor(pid)
    {start_time, deadline} = deadline(timeout)

    # Manually implement call to avoid double monitor.
    # Auto-connect is asynchronous. But we still use :noconnect to make sure
    # we send on the monitored connection, and not trigger a new auto-connect.
    Process.send(pid, {:"$gen_call", {self(), ref}, {:checkout, deadline}}, [:noconnect])

    receive do
      {^ref, runner_pid} ->
        try do
          Process.demonitor(ref, [:flush])
          remaining_timeout = remaining_timeout(opts, start_time)
          func.(runner_pid, remaining_timeout, track_resources)
        catch
          kind, reason ->
            send_cancel(pid, ref, :catch)
            :erlang.raise(kind, reason, __STACKTRACE__)
        else
          {:cancel, :ok, {result, [_ | _] = trackable_pids}} ->
            send_cancel(pid, ref, {:replace, trackable_pids})
            result

          {:cancel, reason, {result, [] = _trackable_pids}} ->
            send_cancel(pid, ref, reason)
            result
        end

      {:DOWN, ^ref, _, _, reason} ->
        exit({reason, {__MODULE__, fun_name, args}})
    after
      timeout ->
        send_cancel(pid, ref, :timeout)
        Process.demonitor(ref, [:flush])
        exit({:timeout, {__MODULE__, fun_name, args}})
    end
  end

  defp send_cancel(pid, ref, reason) when is_pid(pid) and is_reference(ref) do
    send(pid, {:cancel, ref, self(), reason})
  end

  defp remaining_timeout(opts, mono_start) do
    case Keyword.fetch(opts, :timeout) do
      {:ok, :infinity = inf} ->
        inf

      {:ok, nil} ->
        nil

      {:ok, ms} when is_integer(ms) ->
        elapsed_ms =
          System.convert_time_unit(System.monotonic_time() - mono_start, :native, :millisecond)

        ms - elapsed_ms

      :error ->
        nil
    end
  end

  defp lookup_meta(name) do
    :ets.lookup_element(name, :meta, 2)
  end

  def metrics(name) do
    GenServer.call(name, :metrics)
  end

  def reconfigure(name, opts) do
    Keyword.validate!(opts, [:min, :max])
    GenServer.call(name, {:reconfigure, Map.new(opts)})
  end

  def poll_unmet_demand(name, :scale) do
    GenServer.call(name, {:poll_unmet_demand, :scale})
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    task_sup = Keyword.fetch!(opts, :task_sup)
    boot_timeout = Keyword.get(opts, :boot_timeout, @boot_timeout)
    track_resources = Keyword.get(opts, :track_resources, false)
    :ets.new(name, [:set, :public, :named_table, read_concurrency: true])

    :ets.insert(
      name,
      {:meta, %{boot_timeout: boot_timeout, task_sup: task_sup, track_resources: track_resources}}
    )

    terminator_sup = Keyword.fetch!(opts, :terminator_sup)
    child_placement_sup = Keyword.fetch!(opts, :child_placement_sup)
    runner_opts = runner_opts(opts, terminator_sup)
    min = Keyword.fetch!(opts, :min)

    # we must avoid recursively booting remote runners if we are a child
    min =
      if FLAME.Parent.get() do
        0
      else
        min
      end

    base_sync_stream =
      if code_sync_opts = opts[:code_sync] do
        code_sync =
          code_sync_opts
          |> CodeSync.new()
          |> CodeSync.compute_changed_paths()

        %CodeSync.PackagedStream{} = parent_stream = CodeSync.package_to_stream(code_sync)
        parent_stream
      end

    state = %Pool{
      runner_sup: Keyword.fetch!(opts, :runner_sup),
      task_sup: task_sup,
      terminator_sup: terminator_sup,
      child_placement_sup: child_placement_sup,
      name: name,
      min: min,
      max: Keyword.fetch!(opts, :max),
      boot_timeout: boot_timeout,
      boot_max_concurrency: Keyword.get(opts, :boot_max_concurrency, 10),
      idle_shutdown_after: Keyword.get(opts, :idle_shutdown_after, @idle_shutdown_after),
      min_idle_shutdown_after: Keyword.get(opts, :min_idle_shutdown_after, :infinity),
      min_blocking_threshold: Keyword.get(opts, :min_blocking_threshold, min),
      strategy: Keyword.get(opts, :strategy, @default_strategy),
      on_grow_start: opts[:on_grow_start],
      on_grow_end: opts[:on_grow_end],
      on_shrink: opts[:on_shrink],
      track_resources: track_resources,
      runner_opts: runner_opts,
      base_sync_stream: base_sync_stream
    }

    {:ok, boot_min_runners(state)}
  end

  defp runner_opts(opts, terminator_sup) do
    defaults = [terminator_sup: terminator_sup, log: Keyword.get(opts, :log, false)]

    runner_opts =
      Keyword.take(
        opts,
        [
          :backend,
          :log,
          :single_use,
          :timeout,
          :boot_timeout,
          :shutdown_timeout,
          :idle_shutdown_after,
          :code_sync
        ]
      )

    case Keyword.fetch(opts, :backend) do
      {:ok, {backend, opts}} ->
        Keyword.put(runner_opts, :backend, {backend, Keyword.merge(opts, defaults)})

      {:ok, backend} ->
        Keyword.put(runner_opts, :backend, {backend, defaults})

      :error ->
        backend = FLAME.Backend.impl()
        backend_opts = Application.get_env(:flame, backend) || []
        Keyword.put(runner_opts, :backend, {backend, Keyword.merge(backend_opts, defaults)})
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason} = msg, %Pool{} = state) do
    {:noreply, handle_down(state, msg)}
  end

  def handle_info({ref, {:ok, pid}}, %Pool{} = state) when is_reference(ref) do
    {:noreply, handle_runner_async_up(state, pid, ref)}
  end

  def handle_info(:async_boot_continue, %Pool{} = state) do
    {:noreply, async_boot_runner(%Pool{state | async_boot_timer: nil})}
  end

  def handle_info({:cancel, ref, caller_pid, reason}, state) do
    case reason do
      {:replace, child_pids} ->
        {:noreply, replace_caller(state, ref, caller_pid, child_pids)}

      reason when reason in [:ok, :timeout, :catch] ->
        {:noreply, checkin_runner(state, ref, caller_pid, reason)}
    end
  end

  @impl true
  def handle_call({:checkout, deadline}, from, state) do
    {:noreply, checkout_runner(state, deadline, from)}
  end

  @impl true
  def handle_call(:metrics, _from, state) do
    metrics = %{
      runner_count: runner_count(state),
      waiting_count: waiting_count(state),
      pending_count: pending_count(state),
      available_count: available_runners_count(state),
      min: state.min,
      max: state.max
    }

    {:reply, metrics, state}
  end

  @impl true
  def handle_call({:poll_unmet_demand, :scale}, _from, state) do
    state = async_boot_runner(state)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:reconfigure, opts}, _from, state) do
    state = Map.merge(state, opts)
    {:reply, :ok, state}
  end

  def runner_count(state) do
    map_size(state.runners)
  end

  def waiting_count(%Pool{waiting: %Queue{} = waiting}) do
    Queue.size(waiting)
  end

  def pending_count(state) do
    map_size(state.pending_runners)
  end

  def desired_count(state) do
    {strategy_module, strategy_opts} = state.strategy
    strategy_module.desired_count(state, strategy_opts)
  end

  def available_runners_count(state) do
    {strategy_module, strategy_opts} = state.strategy
    runners = strategy_module.available_runners(state, strategy_opts)
    Enum.count(runners)
  end

  defp await_downs(child_pids) do
    if MapSet.size(child_pids) == 0 do
      :ok
    else
      receive do
        {:DOWN, _ref, :process, pid, _reason} -> await_downs(MapSet.delete(child_pids, pid))
      end
    end
  end

  defp replace_caller(%Pool{} = state, checkout_ref, caller_pid, [_ | _] = child_pids) do
    # replace caller with child pid and do not inc concurrency counts since we are replacing
    %{^caller_pid => %Caller{checkout_ref: ^checkout_ref} = caller} = state.callers
    Process.demonitor(caller.monitor_ref, [:flush])

    # if we have more than 1 child pid, such as for multiple trackables returned for a single
    # call, we monitor all of them under a new process and the new process takes the slot in the
    # pool. When all trackables are finished, the new process goes down and frees the slot.
    child_pid =
      case child_pids do
        [child_pid] ->
          child_pid

        [_ | _] ->
          {:ok, child_pid} =
            Task.Supervisor.start_child(state.task_sup, fn ->
              Enum.each(child_pids, &Process.monitor(&1))
              await_downs(MapSet.new(child_pids))
            end)

          child_pid
      end

    new_caller = %Caller{
      checkout_ref: checkout_ref,
      monitor_ref: Process.monitor(child_pid),
      runner_ref: caller.runner_ref
    }

    new_callers =
      state.callers
      |> Map.delete(caller_pid)
      |> Map.put(child_pid, new_caller)

    %Pool{state | callers: new_callers}
  end

  defp checkin_runner(state, ref, caller_pid, reason)
       when is_reference(ref) and is_pid(caller_pid) do
    case state.callers do
      %{^caller_pid => %Caller{checkout_ref: ^ref} = caller} ->
        Process.demonitor(caller.monitor_ref, [:flush])
        drop_caller(state, caller_pid, caller)

      # the only way to race a checkin is if the caller has expired while still in the
      # waiting state and checks in on the timeout before we lease it a runner.
      %{} when reason == :timeout ->
        maybe_drop_waiting(state, caller_pid)

      %{} ->
        Logger.info("Runner: #{inspect(caller_pid)}, went away -- maybe its draining")
        state
    end
  end

  defp checkout_runner(%Pool{} = state, deadline, from, monitor_ref \\ nil) do
    {strategy_module, strategy_opts} = state.strategy

    actions = strategy_module.checkout_runner(state, strategy_opts)

    Enum.reduce(actions, state, fn action, acc ->
      case action do
        :wait -> waiting_in(acc, deadline, from)
        :scale -> async_boot_runner(acc)
        {:checkout, runner} -> reply_runner_checkout(acc, runner, from, monitor_ref)
      end
    end)
  end

  defp reply_runner_checkout(state, %RunnerState{} = runner, from, monitor_ref) do
    # we pass monitor_ref down from waiting so we don't need to remonitor if already monitoring
    {from_pid, checkout_ref} = from

    caller_monitor_ref =
      if monitor_ref do
        monitor_ref
      else
        Process.monitor(from_pid)
      end

    GenServer.reply(from, runner.pid)

    new_caller = %Caller{
      checkout_ref: checkout_ref,
      monitor_ref: caller_monitor_ref,
      runner_ref: runner.monitor_ref
    }

    new_state = %Pool{state | callers: Map.put(state.callers, from_pid, new_caller)}

    inc_runner_count(new_state, runner.monitor_ref)
  end

  defp waiting_in(%Pool{} = state, deadline, {pid, _tag} = from) do
    ref = Process.monitor(pid)
    waiting = %WaitingState{from: from, monitor_ref: ref, deadline: deadline}
    %Pool{state | waiting: Queue.insert(state.waiting, waiting, pid)}
  end

  defp boot_min_runners(%Pool{on_grow_start: on_grow_start, name: name} = state) do
    to_be_started = min(state.min, state.min_blocking_threshold)

    if on_grow_start, do: on_grow_start.(%{count: to_be_started, name: name, pid: self()})

    # The min runners are special, we do not idle them down unless
    # `:min_idle_shutdown_after` is not infinity
    min_runner_opts = [idle_shutdown_after: state.min_idle_shutdown_after]

    if to_be_started > 0 do
      # TODO: allow % threshold of failed min's to continue startup?
      state =
        0..(to_be_started - 1)
        |> Task.async_stream(fn _ -> start_child_runner(state, min_runner_opts) end,
          max_concurrency: state.boot_max_concurrency,
          timeout: state.boot_timeout
        )
        |> Enum.reduce(state, fn
          {:ok, {:ok, pid}}, acc ->
            {_runner, new_acc} = put_runner(acc, pid)
            new_acc

          {:exit, reason}, _acc ->
            raise "failed to boot runner: #{inspect(reason)}"
        end)

      # We synchronously started the minimum amount of runers the pool wants to block
      # the calling process for (which may have been the configured :min.
      # Let's now check if there are any remaining runners which should start async.
      # This is a noop if min == min_blocking_threshold.
      async_boot_runner(state, count: state.min, runner_opts: min_runner_opts)
    else
      state
    end
  end

  defp schedule_async_boot_runner(%Pool{} = state) do
    if state.async_boot_timer, do: Process.cancel_timer(state.async_boot_timer)

    %Pool{
      state
      | async_boot_timer: Process.send_after(self(), :async_boot_continue, @async_boot_debounce)
    }
  end

  # Starts runners asynchronously as to not block the pool. Note that boot_max_concurrency
  # does not bound the number of concurrent booting runners here.
  defp async_boot_runner(%Pool{on_grow_start: on_grow_start, name: name} = state, opts \\ []) do
    runner_opts = Keyword.get(opts, :runner_opts)

    # :count should be the _total_ count you want to spawn, not the delta
    new_count = Keyword.get_lazy(opts, :count, fn -> desired_count(state) end)
    current_count = runner_count(state) + pending_count(state)
    num_tasks = max(new_count - current_count, 0)

    # We need the Task.t() returned by async_nolink, we can't use async_stream which implements
    # the :max_concurrency option, so we emulate it here.
    tasks =
      for _ <- 1..num_tasks//1 do
        Task.Supervisor.async_nolink(state.task_sup, fn ->
          if on_grow_start, do: on_grow_start.(%{count: new_count, name: name, pid: self()})

          if runner_opts,
            do: start_child_runner(state, runner_opts),
            else: start_child_runner(state)
        end)
      end

    pending_runners = Map.new(tasks, &{&1.ref, &1.pid})
    new_pending = Map.merge(state.pending_runners, pending_runners)
    %Pool{state | pending_runners: new_pending}
  end

  defp start_child_runner(%Pool{} = state, runner_opts \\ []) do
    opts = Keyword.merge(state.runner_opts, runner_opts)
    name = Module.concat(state.name, "Runner#{map_size(state.runners) + 1}")

    spec = %{
      id: name,
      start: {FLAME.Runner, :start_link, [opts]},
      restart: :temporary
    }

    {:ok, pid} = DynamicSupervisor.start_child(state.runner_sup, spec)

    try do
      case Runner.remote_boot(pid, state.base_sync_stream) do
        :ok -> {:ok, pid}
        {:error, reason} -> {:error, reason}
      end
    catch
      {:exit, reason} -> {:error, {:exit, reason}}
    end
  end

  defp put_runner(%Pool{} = state, pid) when is_pid(pid) do
    ref = Process.monitor(pid)
    runner = %RunnerState{count: 0, pid: pid, monitor_ref: ref}
    new_state = %Pool{state | runners: Map.put(state.runners, runner.monitor_ref, runner)}
    {runner, new_state}
  end

  defp inc_runner_count(%Pool{} = state, ref) do
    new_runners =
      Map.update!(state.runners, ref, fn %RunnerState{} = runner ->
        %RunnerState{runner | count: runner.count + 1}
      end)

    %Pool{state | runners: new_runners}
  end

  defp dec_runner_count(%Pool{} = state, ref) do
    new_runners =
      Map.update!(state.runners, ref, fn %RunnerState{} = runner ->
        %RunnerState{runner | count: runner.count - 1}
      end)

    %Pool{state | runners: new_runners}
  end

  defp drop_child_runner(%Pool{} = state, runner_ref) when is_reference(runner_ref) do
    %{^runner_ref => %RunnerState{}} = state.runners
    Process.demonitor(runner_ref, [:flush])
    # kill all callers that still had a checkout for this runner
    new_state =
      Enum.reduce(state.callers, state, fn
        {caller_pid, %Caller{monitor_ref: ref, runner_ref: ^runner_ref}}, acc ->
          Process.demonitor(ref, [:flush])
          Process.exit(caller_pid, :kill)
          %Pool{acc | callers: Map.delete(acc.callers, caller_pid)}

        {_caller_pid, %Caller{}}, acc ->
          acc
      end)

    maybe_on_shrink(%Pool{new_state | runners: Map.delete(new_state.runners, runner_ref)})
  end

  defp drop_caller(%Pool{} = state, caller_pid, %Caller{} = caller) when is_pid(caller_pid) do
    new_state = %Pool{state | callers: Map.delete(state.callers, caller_pid)}

    new_state
    |> dec_runner_count(caller.runner_ref)
    |> call_next_waiting_caller()
    |> maybe_mark_unschedulable(caller.runner_ref)
  end

  defp maybe_mark_unschedulable(%Pool{} = state, ref) do
    # If the runner is single use, don't allow it to be rescheduled during the runner's async shutdown
    single_use? = get_in(state, [Access.key(:runner_opts), :single_use])

    if single_use? do
      new_runners =
        Map.update!(state.runners, ref, fn %RunnerState{} = runner ->
          %RunnerState{runner | schedulable?: false}
        end)

      %Pool{state | runners: new_runners}
    else
      state
    end
  end

  defp maybe_drop_waiting(%Pool{} = state, caller_pid) when is_pid(caller_pid) do
    %Pool{state | waiting: Queue.delete_by_key(state.waiting, caller_pid)}
  end

  defp pop_next_waiting_caller(%Pool{} = state) do
    result =
      Queue.pop_until(state.waiting, fn _pid, %WaitingState{} = waiting ->
        %WaitingState{from: {pid, _}, monitor_ref: ref, deadline: deadline} = waiting
        # we don't need to reply to waiting callers because they will either have died
        # or execeeded their own deadline handled by receive + after
        if Process.alive?(pid) and not deadline_expired?(deadline) do
          true
        else
          Process.demonitor(ref, [:flush])
          false
        end
      end)

    case result do
      {nil, %Queue{} = new_waiting} -> {nil, %Pool{state | waiting: new_waiting}}
      {{_pid, %WaitingState{} = first}, %Queue{} = rest} -> {first, %Pool{state | waiting: rest}}
    end
  end

  defp call_next_waiting_caller(%Pool{} = state) do
    case pop_next_waiting_caller(state) do
      {nil, new_state} ->
        new_state

      {%WaitingState{} = first, new_state} ->
        # checkout_runner will borrow already running monitor
        checkout_runner(new_state, first.deadline, first.from, first.monitor_ref)
    end
  end

  defp handle_down(%Pool{} = state, {:DOWN, ref, :process, pid, reason}) do
    state = maybe_drop_waiting(state, pid)

    %{
      callers: callers,
      runners: runners,
      pending_runners: pending_runners,
      strategy: {strategy_module, strategy_opts}
    } = state

    state =
      case callers do
        %{^pid => %Caller{monitor_ref: ^ref} = caller} ->
          drop_caller(state, pid, caller)

        %{} ->
          state
      end

    state =
      case runners do
        %{^ref => _} -> drop_child_runner(state, ref)
        %{} -> state
      end

    case pending_runners do
      %{^ref => _} ->
        state = %Pool{state | pending_runners: Map.delete(state.pending_runners, ref)}
        # we rate limit this to avoid many failed async boot attempts
        if strategy_module.has_unmet_servicable_demand?(state, strategy_opts) do
          state
          |> maybe_on_grow_end(pid, {:exit, reason})
          |> schedule_async_boot_runner()
        else
          maybe_on_grow_end(state, pid, {:exit, reason})
        end

      %{} ->
        state
    end
  end

  defp maybe_on_grow_end(%Pool{on_grow_end: on_grow_end} = state, pid, result) do
    new_count = runner_count(state) + pending_count(state)
    meta = %{count: new_count, name: state.name, pid: pid}

    case result do
      :ok -> if on_grow_end, do: on_grow_end.(:ok, meta)
      {:exit, reason} -> if on_grow_end, do: on_grow_end.({:exit, reason}, meta)
    end

    state
  end

  defp maybe_on_shrink(%Pool{} = state) do
    new_count = runner_count(state) + pending_count(state)
    if state.on_shrink, do: state.on_shrink.(%{count: new_count, name: state.name})

    state
  end

  defp handle_runner_async_up(%Pool{} = state, pid, ref) when is_pid(pid) and is_reference(ref) do
    %{^ref => task_pid} = state.pending_runners
    Process.demonitor(ref, [:flush])

    new_state = %Pool{state | pending_runners: Map.delete(state.pending_runners, ref)}
    {runner, new_state} = put_runner(new_state, pid)
    new_state = maybe_on_grow_end(new_state, task_pid, :ok)

    {strategy_module, strategy_opts} = state.strategy

    pop = fn state -> pop_next_waiting_caller(state) end

    checkout = fn state, runner, from, monitor_ref ->
      reply_runner_checkout(state, runner, from, monitor_ref)
    end

    strategy_module.assign_waiting_callers(new_state, runner, pop, checkout, strategy_opts)
  end

  defp deadline(timeout) when is_integer(timeout) do
    t1 = System.monotonic_time()
    {t1, t1 + System.convert_time_unit(timeout, :millisecond, :native)}
  end

  defp deadline(:infinity) do
    {System.monotonic_time(), :infinity}
  end

  defp deadline_expired?(deadline) when is_integer(deadline) do
    System.monotonic_time() >= deadline
  end

  defp deadline_expired?(:infinity), do: false
end
