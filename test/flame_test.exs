defmodule FLAME.FLAMETest do
  use ExUnit.Case, async: true

  alias FLAME.Pool

  defp sim_long_running(pool, time \\ 1_000) do
    ref = make_ref()
    parent = self()

    task =
      Task.start_link(fn ->
        FLAME.call(pool, fn ->
          send(parent, {ref, :called})
          Process.sleep(time)
        end)
      end)

    receive do
      {^ref, :called} -> task
    end
  end

  setup config do
    if runner_opts = Map.get(config, :runner) do
      runner_sup = Module.concat(config.test, "RunnerSup")
      pool_pid = start_supervised!({Pool, Keyword.merge(runner_opts, name: config.test)})

      {:ok, runner_sup: runner_sup, pool_pid: pool_pid}
    else
      :ok
    end
  end

  @tag runner: [
         min: 1,
         max: 1,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 1},
         single_use: true
       ]
  test "runner is marked unschedulable after single use", %{runner_sup: runner_sup} = config do
    [{:undefined, runner_pid, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)

    # Perform a call to use the runner
    assert FLAME.call(config.test, fn -> :works end) == :works

    # Check that the runner is marked as unschedulable
    %{runners: runners} = :sys.get_state(config.test)
    assert [%{pid: ^runner_pid, schedulable?: false, count: 0}] = Map.values(runners)
  end

  defmodule AlwaysScaleStrategy do
    @behaviour FLAME.Pool.Strategy
    def checkout_runner(_state, _opts), do: [:scale]
    def assign_waiting_callers(state, _runner, _pop, _reply, _opts), do: state
    def desired_count(state, _opts), do: FLAME.Pool.runner_count(state) + 1
    def has_unmet_servicable_demand?(_state, _strategy_opts), do: true

    def available_runners(state, _opts),
      do: Enum.map(state.runners, fn {_ref, runner} -> runner end)
  end

  def poll_unmet_demand_grow_end(_result, _meta) do
    maybe_send_to_named_process(:poll_unmet_demand_test, :grow_end)
  end

  @tag runner: [
         min: 0,
         max: 1,
         strategy: {AlwaysScaleStrategy, []},
         on_grow_end: &__MODULE__.poll_unmet_demand_grow_end/2
       ]
  test "Clients can scale the pool on demand if there is unmet demand",
       %{runner_sup: runner_sup} = config do
    Process.register(self(), :poll_unmet_demand_test)
    [] = Supervisor.which_children(runner_sup)

    # Our strategy defines that there is always demand on the pool
    FLAME.Pool.poll_unmet_demand(config.test, :scale)

    # The pool should scale
    assert_receive :grow_end

    assert [{:undefined, _runner_pid, :worker, [FLAME.Runner]}] =
             Supervisor.which_children(runner_sup)
  end

  @tag runner: [
         min: 1,
         max: 2,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2}
       ]
  test "init boots min runners synchronously and grows on demand",
       %{runner_sup: runner_sup} = config do
    min_pool = Supervisor.which_children(runner_sup)
    assert [{:undefined, _pid, :worker, [FLAME.Runner]}] = min_pool
    # execute against single runner
    assert FLAME.call(config.test, fn -> :works end) == :works

    # dynamically grows to max
    _task1 = sim_long_running(config.test)
    assert FLAME.call(config.test, fn -> :works end) == :works
    # max concurrency still below threshold
    assert Supervisor.which_children(runner_sup) == min_pool
    # max concurrency above threshold boots new runner
    _task2 = sim_long_running(config.test)
    assert FLAME.call(config.test, fn -> :works end) == :works
    new_pool = Supervisor.which_children(runner_sup)
    refute new_pool == min_pool
    assert length(new_pool) == 2
    # caller is now queued while waiting for available runner
    _task3 = sim_long_running(config.test)
    _task4 = sim_long_running(config.test)
    # task is queued and times out
    queued = spawn(fn -> FLAME.call(config.test, fn -> :queued end, timeout: 100) end)
    ref = Process.monitor(queued)
    assert_receive {:DOWN, ^ref, :process, _, {:timeout, _}}, 1000
    assert FLAME.call(config.test, fn -> :queued end) == :queued
    assert new_pool == Supervisor.which_children(runner_sup)
  end

  def min_blocking_threshold_grow_start(meta) do
    maybe_send_to_named_process(:min_blocking_threshold, {:grow_start, meta})
  end

  test "boots the min_blocking_threshold runners before the remaining min runners", config do
    runner_opts = [
      min: 3,
      min_blocking_threshold: 1,
      max: 3,
      on_grow_start: &__MODULE__.min_blocking_threshold_grow_start/1
    ]

    Process.register(self(), :min_blocking_threshold)

    runner_sup = Module.concat(config.test, "RunnerSup")
    start_supervised!({Pool, Keyword.merge(runner_opts, name: config.test)})

    assert_receive {:grow_start, %{count: 1}}, 1000
    assert_receive {:grow_start, %{count: 3}}, 1000

    min_pool = Supervisor.which_children(runner_sup)

    assert [
             {:undefined, _, :worker, [FLAME.Runner]},
             {:undefined, _, :worker, [FLAME.Runner]},
             {:undefined, _, :worker, [FLAME.Runner]}
           ] = min_pool
  end

  @tag runner: [
         min: 0,
         max: 1,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2}
       ]
  test "concurrent calls on fully pending runners",
       %{runner_sup: runner_sup} = config do
    assert Supervisor.which_children(runner_sup) == []
    parent = self()

    Task.start_link(fn ->
      FLAME.call(config.test, fn ->
        send(parent, :called)
        Process.sleep(:infinity)
      end)
    end)

    Task.start_link(fn ->
      FLAME.call(config.test, fn ->
        send(parent, :called)
        Process.sleep(:infinity)
      end)
    end)

    assert_receive :called
    assert_receive :called
  end

  def on_grow_start(meta) do
    maybe_send_to_named_process(:failure_test, {:grow_start, meta})

    if Process.whereis(:failure_test_counter) do
      if Agent.get_and_update(:failure_test_counter, &{&1 + 1, &1 + 1}) <= 1 do
        raise "boom"
      end
    end
  end

  def on_grow_end(result, meta) do
    maybe_send_to_named_process(:failure_test, {:grow_end, result, meta})
  end

  @tag runner: [
         min: 1,
         max: 2,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 1},
         on_grow_start: &__MODULE__.on_grow_start/1,
         on_grow_end: &__MODULE__.on_grow_end/2
       ]
  test "failure of pending async runner bootup", %{runner_sup: runner_sup} = config do
    parent = self()

    ExUnit.CaptureLog.capture_log(fn ->
      start_supervised!(
        {Agent,
         fn ->
           Process.register(self(), :failure_test_counter)
           0
         end}
      )

      Process.register(self(), :failure_test)
      assert [{:undefined, _pid, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)
      # max concurrency above threshold tries to boot new runner
      _task2 = sim_long_running(config.test, :infinity)

      spawn_link(fn ->
        FLAME.cast(config.test, fn -> send(parent, :fullfilled) end)
        Process.sleep(:infinity)
      end)

      # first attempt fails
      refute_receive :fullfilled
      assert_receive {:grow_start, %{count: 2, pid: pid}}
      assert_receive {:grow_end, {:exit, _}, %{pid: ^pid, count: 1}}
      assert length(Supervisor.which_children(runner_sup)) == 1

      # retry attempt succeeds
      assert_receive {:grow_start, %{count: 2, pid: pid}}, 1000
      assert_receive {:grow_end, :ok, %{pid: ^pid, count: 2}}
      # queued og caller is now fullfilled from retried runner boot
      assert_receive :fullfilled
      assert FLAME.call(config.test, fn -> :works end) == :works
      assert length(Supervisor.which_children(runner_sup)) == 2
    end)
  end

  @tag runner: [
         min: 1,
         max: 2,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
         idle_shutdown_after: 500
       ]
  test "idle shutdown", %{runner_sup: runner_sup} = config do
    sim_long_running(config.test, 100)
    sim_long_running(config.test, 100)
    sim_long_running(config.test, 100)

    # we've scaled from min 1 to max 2 at this point
    assert [
             {:undefined, runner1, :worker, [FLAME.Runner]},
             {:undefined, runner2, :worker, [FLAME.Runner]}
           ] = Supervisor.which_children(runner_sup)

    Process.monitor(runner1)
    Process.monitor(runner2)
    assert_receive {:DOWN, _ref, :process, ^runner2, {:shutdown, :idle}}, 1000
    refute_receive {:DOWN, _ref, :process, ^runner1, {:shutdown, :idle}}

    assert [{:undefined, ^runner1, :worker, [FLAME.Runner]}] =
             Supervisor.which_children(runner_sup)
  end

  @tag runner: [
         min: 1,
         max: 1,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
         idle_shutdown_after: 500
       ]
  test "pool runner DOWN exits any active checkouts", %{runner_sup: runner_sup} = config do
    {:ok, active_checkout} = sim_long_running(config.test, 10_000)
    Process.unlink(active_checkout)
    Process.monitor(active_checkout)
    assert [{:undefined, runner, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)
    Process.exit(runner, :brutal_kill)
    assert_receive {:DOWN, _ref, :process, ^active_checkout, :killed}
  end

  @tag runner: [
         min: 0,
         max: 1,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
         idle_shutdown_after: 50
       ]
  test "call links", %{runner_sup: runner_sup} = config do
    ExUnit.CaptureLog.capture_log(fn ->
      parent = self()
      # links by defaults
      Process.flag(:trap_exit, true)

      caught =
        try do
          FLAME.call(
            config.test,
            fn ->
              send(parent, {:called, self()})
              Process.exit(self(), :kill)
            end
          )
        catch
          kind, reason -> {kind, reason}
        end

      [{:undefined, runner, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)
      Process.monitor(runner)
      assert {:exit, :killed} = caught
      assert_receive {:called, _flame_pid}
      assert_receive {:DOWN, _ref, :process, ^runner, {:shutdown, :idle}}

      # link: false
      Process.flag(:trap_exit, false)
      assert Supervisor.which_children(runner_sup) == []
      parent = self()

      caught =
        try do
          FLAME.call(
            config.test,
            fn ->
              send(parent, {:called, self()})
              raise "boom"
            end,
            link: false
          )
        catch
          kind, reason -> {kind, reason}
        end

      [{:undefined, runner_pid, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)
      Process.monitor(runner_pid)
      assert {:exit, {%RuntimeError{message: "boom"}, _}} = caught
      assert_receive {:called, flame_pid}
      Process.monitor(flame_pid)
      assert_receive {:DOWN, _ref, :process, ^flame_pid, :noproc}
      assert_receive {:DOWN, _ref, :process, ^runner_pid, {:shutdown, :idle}}
      assert Supervisor.which_children(runner_sup) == []
    end)
  end

  @tag runner: [
         min: 0,
         max: 1,
         strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
         idle_shutdown_after: 50
       ]
  test "cast with link false", %{runner_sup: runner_sup} = config do
    ExUnit.CaptureLog.capture_log(fn ->
      assert Supervisor.which_children(runner_sup) == []
      parent = self()

      FLAME.cast(
        config.test,
        fn ->
          send(parent, {:called, self()})
          raise "boom"
        end,
        link: false
      )

      assert_receive {:called, flame_pid}
      Process.monitor(flame_pid)
      [{:undefined, runner_pid, :worker, [FLAME.Runner]}] = Supervisor.which_children(runner_sup)
      assert_receive {:DOWN, _ref, :process, ^flame_pid, :noproc}
      Process.monitor(runner_pid)
      assert_receive {:DOWN, _ref, :process, ^runner_pid, {:shutdown, :idle}}
      assert Supervisor.which_children(runner_sup) == []
    end)
  end

  describe "pool metrics" do
    @tag runner: [
           min: 1,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2}
         ]
    test "the min and max can be dynamically set", config do
      # Given
      %{min: 1, max: 2} = FLAME.Pool.metrics(config.test)

      # When
      FLAME.Pool.reconfigure(config.test, min: 2, max: 3)
      metrics = FLAME.Pool.metrics(config.test)

      # Then
      assert metrics.min == 2
      assert metrics.max == 3
    end
  end

  describe "cast" do
    @tag runner: [
           min: 1,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 500
         ]
    test "normal execution", %{} = config do
      sim_long_running(config.test, 100)
      parent = self()

      assert FLAME.cast(config.test, fn ->
               send(parent, {:ran, self()})

               receive do
                 :continue -> :ok
               end
             end) == :ok

      assert_receive {:ran, cast_pid}
      Process.monitor(cast_pid)
      send(cast_pid, :continue)
      assert_receive {:DOWN, _ref, :process, ^cast_pid, :normal}
    end

    def growth_grow_start(meta) do
      maybe_send_to_named_process(:pool_growth, {:grow_start, meta})
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 1},
           on_grow_start: &__MODULE__.growth_grow_start/1
         ]
    test "pool growth", %{} = config do
      Process.register(self(), :pool_growth)
      parent = self()

      for i <- [1, 2, 3] do
        assert FLAME.cast(config.test, fn ->
                 send(parent, {:ran, i, self()})
                 Process.sleep(500)
               end) == :ok
      end

      for i <- [1, 2, 3] do
        assert_receive {:ran, ^i, cast_pid}
        Process.monitor(cast_pid)
        assert_receive {:DOWN, _ref, :process, ^cast_pid, _}, 1000
      end

      assert_receive {:grow_start, %{count: 1}}, 1000
      assert_receive {:grow_start, %{count: 2}}, 1000
      refute_receive {:grow_start, _}, 1000
    end

    @tag runner: [
           min: 1,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 500
         ]
    test "with exit and default link", %{} = config do
      ExUnit.CaptureLog.capture_log(fn ->
        Process.flag(:trap_exit, true)
        sim_long_running(config.test, 100)
        parent = self()

        assert FLAME.cast(config.test, fn ->
                 send(parent, {:ran, self()})

                 receive do
                   :continue -> exit(:boom)
                 end
               end) == :ok

        assert_receive {:ran, cast_pid}
        Process.monitor(cast_pid)
        send(cast_pid, :continue)
        assert_receive {:EXIT, ^cast_pid, :boom}
      end)
    end
  end

  describe "process placement" do
    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100
         ]
    test "place_child/2", %{runner_sup: runner_sup} = config do
      assert [] = Supervisor.which_children(runner_sup)
      assert {:ok, pid} = FLAME.place_child(config.test, {Agent, fn -> 1 end})
      Process.monitor(pid)

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      assert Agent.get(pid, & &1) == 1
      # does not idle down runner or actively placed children
      refute_receive {:DOWN, _ref, :process, _, _}, 1000
      # active caller to prevent idle down
      assert FLAME.cast(config.test, fn ->
               Process.sleep(1_000)
             end) == :ok

      Agent.stop(pid)
      assert_receive {:DOWN, _ref, :process, ^pid, _}, 100

      # runner does not idle down with active checkout from cast
      refute_receive {:DOWN, _ref, :process, ^runner, _}, 1000

      # runner idles down now that placed child and cast callers are gone
      assert_receive {:DOWN, _ref, :process, ^runner, _}, 1000
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100
         ]
    test "place_child links", %{runner_sup: runner_sup} = config do
      # links by default
      Process.flag(:trap_exit, true)
      assert {:ok, pid} = FLAME.place_child(config.test, {Agent, fn -> 1 end})

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)

      Process.exit(pid, :kill)
      assert_receive {:EXIT, ^pid, :killed}, 100

      # runner idles down now that placed child and cast callers are gone
      assert_receive {:DOWN, _ref, :process, ^runner, _}, 1000

      # with explicit link: false
      Process.flag(:trap_exit, false)
      assert {:ok, pid} = FLAME.place_child(config.test, {Agent, fn -> 1 end}, link: false)
      Process.monitor(pid)

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)

      Process.exit(pid, :kill)
      assert_receive {:DOWN, _ref, :process, ^pid, :killed}, 100

      # runner idles down now that placed child and cast callers are gone
      assert_receive {:DOWN, _ref, :process, ^runner, _}, 1000
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100
         ]
    test "place_child when caller exits", %{runner_sup: runner_sup} = config do
      # links by default
      parent = self()

      caller =
        spawn(fn ->
          {:ok, pid} = FLAME.place_child(config.test, {Agent, fn -> 1 end})
          send(parent, {:child, pid})
          Process.sleep(:infinity)
        end)

      assert_receive {:child, placed_child}

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      Process.monitor(placed_child)

      Process.exit(caller, :kill)

      assert_receive {:DOWN, _ref, :process, ^placed_child, _}
      # runner idles down now that placed child and cast callers are gone
      assert_receive {:DOWN, _ref, :process, ^runner, _}, 1000

      # with link: false
      caller =
        spawn(fn ->
          {:ok, pid} = FLAME.place_child(config.test, {Agent, fn -> 1 end}, link: false)
          send(parent, {:child, pid})
          Process.sleep(:infinity)
        end)

      assert_receive {:child, placed_child}

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      Process.monitor(placed_child)
      Process.exit(caller, :kill)

      refute_receive {:DOWN, _ref, :process, ^placed_child, _}
      # runner does not idle down when caller goes away since placed child still running
      refute_receive {:DOWN, _ref, :process, ^runner, _}, 1000

      Process.exit(placed_child, :kill)
      assert_receive {:DOWN, _ref, :process, ^placed_child, _}
      # runner idles down now that placed child and cast callers are gone
      assert_receive {:DOWN, _ref, :process, ^runner, _}, 1000
    end
  end

  describe "resource tracking" do
    @tag runner: [min: 0, max: 1]
    test "local", config do
      name = :"#{config.test}_trackable"
      ref = make_ref()
      trackable = %MyTrackable{name: name, ref: ref}
      non_trackable = URI.new!("/")

      {[{map}], [pid]} =
        FLAME.track_resources([{%{"yes" => trackable, "no" => non_trackable}}], [], node())

      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert %MyTrackable{name: ^name, ref: ^ref, pid: ^pid} = map["yes"]
      assert Process.whereis(name) == pid

      monitor_ref = Process.monitor(pid)
      send(pid, {ref, :stop})
      assert_receive {:DOWN, ^monitor_ref, _, _, :normal}
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100
         ]
    test "remote without tracking", config do
      name = :"#{config.test}_trackable"
      non_trackable = URI.new!("/")

      [{map}] =
        FLAME.call(config.test, fn ->
          ref = make_ref()
          trackable = %MyTrackable{name: name, ref: ref}
          [{%{"yes" => trackable, "no" => non_trackable}}]
        end)

      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert %MyTrackable{pid: nil} = map["yes"]
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100
         ]
    test "remote with tracking", %{runner_sup: runner_sup} = config do
      name = :"#{config.test}_trackable"
      non_trackable = URI.new!("/")

      [{map}] =
        FLAME.call(
          config.test,
          fn ->
            ref = make_ref()
            trackable = %MyTrackable{name: name, ref: ref}
            [{%{"yes" => trackable, "no" => non_trackable}}]
          end,
          track_resources: true
        )

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert %MyTrackable{pid: pid} = trackable = map["yes"]
      assert Process.alive?(pid)
      refute_receive {:DOWN, _, _, ^runner, _}, 1000
      send(pid, {trackable.ref, :stop})
      assert_receive {:DOWN, _, _, ^runner, {:shutdown, :idle}}, 1000
    end

    @tag runner: [
           min: 0,
           max: 2,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 2},
           idle_shutdown_after: 100,
           track_resources: true
         ]
    test "remote with tracking enabled at pool level", %{runner_sup: runner_sup} = config do
      name = :"#{config.test}_trackable"
      non_trackable = URI.new!("/")

      [{map}] =
        FLAME.call(
          config.test,
          fn ->
            ref = make_ref()
            trackable = %MyTrackable{name: name, ref: ref}
            [{%{"yes" => trackable, "no" => non_trackable}}]
          end
        )

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert %MyTrackable{pid: pid} = trackable = map["yes"]
      assert Process.alive?(pid)
      refute_receive {:DOWN, _, _, ^runner, _}, 1000
      send(pid, {trackable.ref, :stop})
      assert_receive {:DOWN, _, _, ^runner, {:shutdown, :idle}}, 1000
    end

    @tag runner: [
           min: 0,
           max: 1,
           strategy: {Pool.PerRunnerMaxConcurrencyStrategy, max_concurrency: 1},
           idle_shutdown_after: 100,
           track_resources: true
         ]
    test "remote with tracking max concurrency", %{runner_sup: runner_sup} = config do
      non_trackable = URI.new!("/")

      call = fn count ->
        ref = make_ref()

        trackables =
          for _ <- 1..count,
              do: %MyTrackable{
                name: :"#{config.test}_trackable_#{System.unique_integer()}",
                ref: ref
              }

        [{%{"yes" => trackables, "no" => non_trackable}}]
      end

      [{map}] = FLAME.call(config.test, fn -> call.(2) end)

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert [%MyTrackable{} = trackable1, %MyTrackable{} = trackable2] = map["yes"]

      # original trackables still occupies the slots
      assert Process.alive?(trackable1.pid)
      assert Process.alive?(trackable2.pid)
      refute_receive {:DOWN, _, _, ^runner, _}, 1000

      # check in the trackable 1
      send(trackable1.pid, {trackable1.ref, :stop})

      # no idle down because second trackable still alive
      refute_receive {:DOWN, _, _, ^runner, _}, 1000

      # trackable2 occupies the only available slot, so next call times out
      caught =
        try do
          FLAME.call(config.test, fn -> call.(1) end, timeout: 1000)
        catch
          kind, reason -> {kind, reason}
        end

      assert {:exit, {:timeout, _}} = caught

      # check in the trackable 2
      send(trackable2.pid, {trackable2.ref, :stop})

      # runner is now free for more work on open slot
      [{map}] = FLAME.call(config.test, fn -> call.(1) end)

      assert [{:undefined, runner, :worker, [FLAME.Runner]}] =
               Supervisor.which_children(runner_sup)

      Process.monitor(runner)
      assert map_size(map) == 2
      assert ^non_trackable = map["no"]
      assert [%MyTrackable{pid: pid} = trackable] = map["yes"]

      # check in the trackable
      send(pid, {trackable.ref, :stop})

      # runner idles down
      assert_receive {:DOWN, _, _, ^runner, {:shutdown, :idle}}, 1000
    end
  end

  # I added an on_grow_start call on the min runner boot, but the problem is that
  # most tests which already exist spawn the pool before the named process which the
  # on_grow_{start,end} functions are supposed to send messages to is spawned.
  defp maybe_send_to_named_process(name, message) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> send(pid, message)
    end
  end
end
