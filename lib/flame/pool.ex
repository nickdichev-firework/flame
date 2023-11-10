defmodule FLAME.Pool.RunnerState do
  defstruct count: nil, pid: nil, monitor_ref: nil
end

defmodule FLAME.Pool.WaitingState do
  defstruct from: nil, monitor_ref: nil
end

defmodule FLAME.Pool do
  @moduledoc """
  Manages a pool of `FLAME.Runner`'s.

  Pools support elastic growth and shrinking of the number of runners.

  ## Examples

      children = [
        ...,
        {FLAME.Pool, name: MyRunner, min: 1, max: 10, max_concurrency: 100}
      ]

  See `start_link/1` for supported options.

  ## TODO

  - interface to configure min/max at runtime
  - callbacks for pool events so folks can hook into pool growth/shrinkage

  """
  use GenServer

  alias FLAME.{Pool, Runner}
  alias FLAME.Pool.{RunnerState, WaitingState}

  @default_max_concurrency 100
  @boot_timeout 30_000
  @idle_shutdown_after 30_000

  defstruct name: nil,
            dynamic_sup: nil,
            terminator_sup: nil,
            child_placement_sup: nil,
            boot_timeout: nil,
            idle_shutdown_after: nil,
            min: nil,
            max: nil,
            max_concurrency: nil,
            callers: %{},
            waiting: [],
            runners: %{},
            runner_opts: []

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
      For "scale to zero" behavior you may pass `0`. When starting as a dragonfly child,
      the `:min` will be forced to zero to avoid recursively starting backend resources.

    * `:max` - The maximum number of runners to elastically grow to in the pool.

    * `:max_concurrency` - The maximum number of concurrent executions per runner before
      booting new runners or queueing calls. Defaults to `100`.

    * `:single_use` - if `true`, runners will be terminated after each call completes.
      Defaults `false`.

    * `:backend` - The backend to use. Defaults to the configured `:dragonfly, :backend` or
      `FLAME.LocalBackend` if not configured.

    * `:log` - The log level to use for verbose logging. Defaults to `false`.

    * `:timeout` - The time to allow functions to execute on a remote node. Defaults to 30 seconds.
      This value is also used as the default `FLAME.call/3` timeout for the caller.
    * `:boot_timeout` - The time to allow for booting and connecting to a remote node.
      Defaults to 30 seconds.

    * `:shutdown_timeout` - The time to allow for graceful shutdown on the remote node.
      Defaults to 30 seconds.

    * `:idle_shutdown_after` - The amount of time and function check to idle a remote node
      down after a period of inactivity. Defaults to 30 seconds. A tuple may also be passed
      to check a spefici condition, for example:

          {10_000, fn -> Supervisor.which_children(MySup) == []}
  """
  def start_link(opts) do
    Keyword.validate!(opts, [
      :name,
      :dynamic_sup,
      :terminator_sup,
      :child_placement_sup,
      :idle_shutdown_after,
      :min,
      :max,
      :max_concurrency,
      :backend,
      :log,
      :single_use,
      :timeout,
      :boot_timeout,
      :shutdown_timeout
    ])

    GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
  end

  @doc """
  Calls a function in a remote runner for the given `FLAME.Pool`.

  See `FLAME.call/3` for more information.
  """
  def call(name, func, opts \\ []) do
    opts = Keyword.put_new_lazy(opts, :timeout, fn -> lookup_boot_timeout(name) end)

    {{ref, _term_dyn_sup, runner_pid}, opts} =
      with_elapsed_timeout(opts, fn -> GenServer.call(name, :checkout, opts[:timeout]) end)

    result = Runner.call(runner_pid, func, opts[:timeout])
    :ok = GenServer.call(name, {:checkin, ref})
    result
  end

  @doc """
  Casts a function to a remote runner for the given `FLAME.Pool`.

  See `FLAME.cast/2` for more information.
  """
  def cast(name, func) do
    boot_timeout = lookup_boot_timeout(name)
    {ref, _term_dyn_sup, runner_pid} = GenServer.call(name, :checkout, boot_timeout)

    :ok = Runner.cast(runner_pid, func)
    :ok = GenServer.call(name, {:checkin, ref})
  end

  defp with_elapsed_timeout(opts, func) do
    {micro, result} = :timer.tc(func)
    elapsed_ms = div(micro, 1000)

    opts =
      case Keyword.fetch(opts, :timeout) do
        {:ok, :infinity} -> opts
        {:ok, ms} when is_integer(ms) -> Keyword.put(opts, :timeout, ms - elapsed_ms)
        {:ok, nil} -> opts
        :error -> opts
      end

    {result, opts}
  end

  @doc """
  TODO

  See `FLAME.start_child/3` for more information.
  """
  def start_child(name, _child_spec, _opts) do
    boot_timeout = lookup_boot_timeout(name)
    {ref, _term_dyn_sup, _runner_pid} = GenServer.call(name, :checkout, boot_timeout)

    raise "TODO"
    :ok = GenServer.call(name, {:checkin, ref})
  end


  defp lookup_boot_timeout(name) do
    :ets.lookup_element(name, :boot_timeout, 2)
  end

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    boot_timeout = Keyword.get(opts, :boot_timeout, @boot_timeout)
    :ets.new(name, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(name, {:boot_timeout, boot_timeout})
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

    state = %Pool{
      dynamic_sup: Keyword.fetch!(opts, :dynamic_sup),
      terminator_sup: terminator_sup,
      child_placement_sup: child_placement_sup,
      name: name,
      min: min,
      max: Keyword.fetch!(opts, :max),
      boot_timeout: boot_timeout,
      idle_shutdown_after: Keyword.get(opts, :idle_shutdown_after, @idle_shutdown_after),
      max_concurrency: Keyword.get(opts, :max_concurrency, @default_max_concurrency),
      runner_opts: runner_opts
    }

    {:ok, boot_runners(state)}
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
          :idle_shutdown_after
        ]
      )

    case Keyword.fetch(opts, :backend) do
      {:ok, {backend, opts}} ->
        Keyword.update!(runner_opts, :backend, {backend, Keyword.merge(opts, defaults)})

      {:ok, backend} ->
        Keyword.update!(runner_opts, :backend, {backend, defaults})

      :error ->
        backend = FLAME.Backend.impl()
        backend_opts = Application.get_env(:dragonfly, backend) || []
        Keyword.put(runner_opts, :backend, {backend, Keyword.merge(backend_opts, defaults)})
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason} = msg, %Pool{} = state) do
    {:noreply, handle_down(state, msg)}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    {:noreply, checkout_runner(state, from)}
  end

  def handle_call({:checkin, ref}, _from, state) do
    {:reply, :ok, checkin_runner(state, ref)}
  end

  defp min_runner(state) do
    if map_size(state.runners) == 0 do
      nil
    else
      {_ref, min} = Enum.min_by(state.runners, fn {_, %RunnerState{count: count}} -> count end)
      min
    end
  end

  defp checkin_runner(state, ref) do
    %{^ref => {_from, runner_ref}} = state.callers
    Process.demonitor(ref, [:flush])

    drop_caller(state, ref, runner_ref)
  end

  defp checkout_runner(%Pool{} = state, from, monitor_ref \\ nil) do
    min_runner = min_runner(state)
    runner_count = map_size(state.runners)

    cond do
      runner_count == 0 || (min_runner.count == state.max_concurrency && runner_count < state.max) ->
        case start_child_runner(state) do
          {:ok, %RunnerState{} = runner} ->
            state
            |> put_runner(runner)
            |> reply_runner_checkout(runner, from, monitor_ref)

          {:error, reason} ->
            GenServer.reply(from, {:error, reason})
            state
        end

      min_runner && min_runner.count < state.max_concurrency ->
        reply_runner_checkout(state, min_runner, from, monitor_ref)

      true ->
        waiting_in(state, from)
    end
  end

  defp reply_runner_checkout(state, %RunnerState{} = runner, from, monitor_ref) do
    # we pass monitor_ref down from waiting so we don't need to remonitor if already monitoring
    ref =
      if monitor_ref do
        monitor_ref
      else
        {from_pid, _tag} = from
        Process.monitor(from_pid)
      end

    GenServer.reply(from, {ref, state.child_placement_sup, runner.pid})
    new_state = %Pool{state | callers: Map.put(state.callers, ref, {from, runner.monitor_ref})}
    inc_runner_count(new_state, runner.monitor_ref)
  end

  defp waiting_in(%Pool{} = state, {pid, _tag} = from) do
    ref = Process.monitor(pid)
    waiting = %WaitingState{from: from, monitor_ref: ref}
    %Pool{state | waiting: state.waiting ++ [waiting]}
  end

  defp boot_runners(%Pool{} = state) do
    if state.min > 0 do
      # start min runners, and do not idle them down regardless of idle configuration
      0..(state.min - 1)
      |> Task.async_stream(fn _ -> start_child_runner(state, idle_shutdown_after: :infinity) end,
        max_concurrency: 10,
        timeout: state.boot_timeout
      )
      |> Enum.reduce(state, fn
        {:ok, {:ok, %RunnerState{} = runner}}, acc -> put_runner(acc, runner)
        {:exit, reason}, _acc -> raise "failed to boot runner: #{inspect(reason)}"
      end)
    else
      state
    end
  end

  defp start_child_runner(%Pool{} = state, runner_opts \\ []) do
    opts = Keyword.merge(state.runner_opts, runner_opts)

    name = Module.concat(state.name, "Runner#{map_size(state.runners) + 1}")

    spec = %{
      id: name,
      start: {FLAME.Runner, :start_link, [opts]},
      restart: :temporary
    }

    {:ok, pid} = DynamicSupervisor.start_child(state.dynamic_sup, spec)
    ref = Process.monitor(pid)

    try do
      :ok = Runner.remote_boot(pid)
      runner = %RunnerState{count: 0, pid: pid, monitor_ref: ref}
      {:ok, runner}
    catch
      {:exit, reason} ->
        Process.demonitor(ref, [:flush])
        {:error, {:exit, reason}}
    end
  end

  defp put_runner(%Pool{} = state, %RunnerState{} = runner) do
    %Pool{state | runners: Map.put(state.runners, runner.monitor_ref, runner)}
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

  defp drop_child_runner(%Pool{} = state, ref) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    %Pool{state | runners: Map.delete(state.runners, ref)}
  end

  defp drop_caller(%Pool{} = state, caller_ref, runner_ref) do
    new_state = %Pool{state | callers: Map.delete(state.callers, caller_ref)}

    new_state
    |> dec_runner_count(runner_ref)
    |> call_next_waiting_caller()
  end

  defp call_next_waiting_caller(%Pool{} = state) do
    # we flush DOWN's so we don't send a lease to a waiting caller that is already down
    new_state = flush_downs(state)

    case new_state.waiting do
      [] ->
        new_state

      [%WaitingState{} = first | rest] ->
        # checkout_runner will borrow already running monitor
        checkout_runner(%Pool{new_state | waiting: rest}, first.from, first.monitor_ref)
    end
  end

  defp handle_down(%Pool{} = state, {:DOWN, ref, :process, _pid, _reason}) do
    new_waiting =
      Enum.filter(state.waiting, fn %WaitingState{} = waiting ->
        waiting.monitor_ref != ref
      end)

    state = %Pool{state | waiting: new_waiting}

    state =
      case state.callers do
        %{^ref => {_from, runner_ref}} ->
          drop_caller(state, ref, runner_ref)

        %{} ->
          state
      end

    case state.runners do
      %{^ref => _} -> drop_child_runner(state, ref)
      %{} -> state
    end
  end

  defp flush_downs(%Pool{} = state) do
    receive do
      {:DOWN, _ref, :process, _pid, _reason} = msg ->
        state
        |> handle_down(msg)
        |> flush_downs()
    after
      0 -> state
    end
  end
end