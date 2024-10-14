defmodule FLAME.Pool.PerRunnerMaxConcurrencyStrategy do
  alias FLAME.Pool
  @behaviour FLAME.Pool.Strategy

  @impl true
  def checkout_runner(%Pool{} = pool, opts) do
    min_runner = pool |> available_runners(opts) |> min_runner()
    runner_count = Pool.runner_count(pool) + Pool.pending_count(pool)
    max_concurrency = Keyword.fetch!(opts, :max_concurrency)

    cond do
      min_runner && min_runner.count < max_concurrency ->
        [{:checkout, min_runner}]

      runner_count < pool.max ->
        if pool.async_boot_timer ||
             map_size(pool.pending_runners) * max_concurrency > Pool.waiting_count(pool) do
          [:wait]
        else
          [:scale, :wait]
        end

      true ->
        [:wait]
    end
  end

  @impl true
  def assign_waiting_callers(
        %Pool{} = pool,
        %Pool.RunnerState{} = runner,
        pop_next_waiting_caller,
        reply_runner_checkout,
        opts
      ) do
    max_concurrency = Keyword.fetch!(opts, :max_concurrency)

    # pop waiting callers up to max_concurrency, but we must handle:
    # 1. the case where we have no waiting callers
    # 2. the case where we process a DOWN for the new runner as we pop DOWNs
    #   looking for fresh waiting
    {pool, _assigned_concurrency} =
      Enum.reduce_while(1..max_concurrency, {pool, 0}, fn _i, {pool, assigned_concurrency} ->
        with {:ok, %Pool.RunnerState{} = runner} <- Map.fetch(pool.runners, runner.monitor_ref),
             true <- assigned_concurrency <= max_concurrency do
          case pop_next_waiting_caller.(pool) do
            {%Pool.WaitingState{} = next, pool} ->
              pool = reply_runner_checkout.(pool, runner, next.from, next.monitor_ref)
              {:cont, {pool, assigned_concurrency + 1}}

            {nil, pool} ->
              {:halt, {pool, assigned_concurrency}}
          end
        else
          _ -> {:halt, {pool, assigned_concurrency}}
        end
      end)

    pool
  end

  @impl true
  def desired_count(%Pool{} = pool, _opts) do
    Pool.runner_count(pool) + Pool.pending_count(pool) + 1
  end

  @impl true
  def has_unmet_servicable_demand?(%Pool{} = pool, _opts) do
    runner_count = Pool.runner_count(pool) + Pool.pending_count(pool)
    Pool.waiting_count(pool) > 0 and runner_count < pool.max
  end

  @impl true
  def available_runners(state, _opts) do
    Enum.map(state.runners, fn {_ref, runner} -> runner end)
  end

  defp min_runner(runners) do
    if Enum.empty?(runners) do
      nil
    else
      Enum.min_by(runners, fn %Pool.RunnerState{count: count} -> count end)
    end
  end
end
