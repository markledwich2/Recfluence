using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using SysExtensions.Collections;
using SysExtensions.Text;
using static System.Threading.Tasks.TaskStatus;

// ReSharper disable InconsistentNaming

namespace SysExtensions.Threading {
  public static class BlockExtensions {
    public static async Task<long> BlockDo<T>(this IEnumerable<T> source, Func<T, int, Task> action, int parallel = 1, int? capacity = null,
      CancellationToken cancel = default) {
      var options = ActionOptions(parallel, capacity, cancel);
      var block = new ActionBlock<(T, int)>(i => action(i.Item1, i.Item2), options);
      var produced = await ProduceAsync(source.WithIndex(), block, cancel: cancel).ConfigureAwait(false);
      await block.Completion.ConfigureAwait(false);
      return produced;
    }

    public static Task<long> BlockDo<T>(this IEnumerable<T> source, Func<T, Task> action, int parallel = 1, int? capacity = null,
      CancellationToken cancel = default) => source.BlockDo((o, _) => action(o), parallel, capacity, cancel);

    public static Task<long> BlockDo<T>(this IAsyncEnumerable<T> source, Func<T, Task> action, int parallel = 1, int? capacity = null,
      CancellationToken cancel = default) => source.BlockDo((o, _) => action(o), parallel, capacity, cancel);

    public static async Task<long> BlockDo<T>(this IAsyncEnumerable<T> source, Func<T, int, Task> action, int parallel = 1, int? capacity = null,
      CancellationToken cancel = default) {
      var options = ActionOptions(parallel, capacity, cancel);
      var block = new ActionBlock<(T, int)>(i => action(i.Item1, i.Item2), options);
      var produced = await ProduceAsync(source, block);
      await block.Completion.ConfigureAwait(false);
      return produced;
    }

    static ExecutionDataflowBlockOptions ActionOptions(int parallel, int? capacity, CancellationToken cancel) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      return options;
    }

    public static async IAsyncEnumerable<R> BlockMap<T, R>(this IEnumerable<T> source,
      Func<T, int, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      var block = GetBlock(func, parallel, capacity, cancel);
      var produceTask = ProduceAsync(source.WithIndex(), block, cancel: cancel);
      while (true) {
        if (produceTask.IsFaulted) {
          block.Complete();
          break;
        }
        if (!await block.OutputAvailableAsync().ConfigureAwait(false)) break;
        yield return await block.ReceiveAsync().ConfigureAwait(false);
      }
      await block.Completion.ConfigureAwait(false);
      await produceTask.ConfigureAwait(false);
    }

    public static IAsyncEnumerable<R> BlockMap<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) =>
      BlockMap(source, (o, _) => func(o), parallel, capacity, cancel);

    public static IAsyncEnumerable<R> BlockMap<T, R>(this IAsyncEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) =>
      source.BlockMap((r, _) => func(r), parallel, capacity, cancel);

    public static IAsyncEnumerable<R> BlockFlatMap<T, R>(this IAsyncEnumerable<T>[] sources,
      Func<T, R> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) =>
      sources.BlockFlatMap((r, _) => Task.FromResult(func(r)), parallel, capacity, cancel);

    public static IAsyncEnumerable<R> BlockFlatMap<T, R>(this IAsyncEnumerable<T>[] sources,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) =>
      sources.BlockFlatMap((r, _) => func(r), parallel, capacity, cancel);

    public static async IAsyncEnumerable<R> BlockFlatMap<T, R>(this IAsyncEnumerable<T>[] sources,
      Func<T, int, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      var block = GetBlock(func, parallel, capacity, cancel);

      async Task ProduceAll() {
        try {
          await sources.BlockDo(s => ProduceAsync(s, block, cancel, complete: false), parallel, cancel: cancel).ConfigureAwait(false);
        }
        finally {
          block.Complete();
        }
      }

      var produceTask = ProduceAll();
      while (true) {
        if (produceTask.IsFaulted) {
          block.Complete();
          break;
        }
        if (!await block.OutputAvailableAsync().ConfigureAwait(false)) break;
        yield return await block.ReceiveAsync().ConfigureAwait(false);
      }
      await block.Completion.ConfigureAwait(false);
      await produceTask.ConfigureAwait(false);
    }

    public static async IAsyncEnumerable<R> BlockMap<T, R>(this IAsyncEnumerable<T> source,
      Func<T, int, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      var block = GetBlock(func, parallel, capacity, cancel);
      var produceTask = ProduceAsync(source, block, cancel);
      while (true) {
        if (produceTask.IsFaulted) {
          block.Complete();
          break;
        }
        if (!await block.OutputAvailableAsync().ConfigureAwait(false)) break;
        yield return await block.ReceiveAsync().ConfigureAwait(false);
      }
      await block.Completion.ConfigureAwait(false);
      await produceTask.ConfigureAwait(false);
    }

    public static async IAsyncEnumerable<R> BlockMap<T, R>(this Task<IAsyncEnumerable<T>> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      await foreach (var i in (await source).BlockMap(func, parallel, capacity, cancel))
        yield return i;
    }

    static TransformBlock<(T, int), R> GetBlock<T, R>(Func<T, int, Task<R>> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var indexTupleFunc = new Func<(T, int), Task<R>>(t => func(t.Item1, t.Item2));
      return new(indexTupleFunc, options);
    }

    static async Task<long> ProduceAsync<T>(this IAsyncEnumerable<T> source, ITargetBlock<(T, int)> block, CancellationToken cancel = default,
      bool complete = true) {
      var produced = 0;
      try {
        await foreach (var item in source.Select((r, i) => (r, i)).WithCancellation(cancel)) {
          if (cancel.IsCancellationRequested || block.Completion.IsFaulted) return produced;
          await block.SendAsync(item).ConfigureAwait(false);
          produced++;
        }
      }
      finally {
        if (complete) {
          var sw = Stopwatch.StartNew();
          while (block.Completion.Status.In(Created, WaitingForActivation, WaitingToRun) && sw.Elapsed < 5.Seconds())
            await 10.Milliseconds().Delay().ConfigureAwait(false);
          block.Complete();
        }
      }
      return produced;
    }

    static async Task<long> ProduceAsync<T>(this IEnumerable<T> source, ITargetBlock<T> block, bool complete = true, CancellationToken cancel = default) {
      var produced = 0;
      try {
        foreach (var item in source) {
          if (cancel.IsCancellationRequested || block.Completion.IsFaulted) return produced;
          await block.SendAsync(item).ConfigureAwait(false);
          produced++;
        }
      }
      finally {
        if (complete) {
          var sw = Stopwatch.StartNew();
          while (block.Completion.Status.In(Created, WaitingForActivation, WaitingToRun) && sw.Elapsed < 5.Seconds())
            await 10.Milliseconds().Delay().ConfigureAwait(false);
          block.Complete();
        }
      }
      return produced;
    }

    /// <summary>Simplified method for async operations that don't need to be chained, and when the result can fit in memory.
    ///   Deprecated</summary>
    public static async Task<IReadOnlyCollection<R>> BlockMapList<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null,
      Action<BulkProgressInfo> progressUpdate = null, TimeSpan progressPeriod = default, CancellationToken cancel = default) {
      progressPeriod = progressPeriod == default ? 60.Seconds() : progressPeriod;
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var block = new TransformBlock<T, R>(func, options);

      var swProgress = Stopwatch.StartNew();

      // by producing asynchronously and using SendAsync we can throttle how much we can form the source and consume at the same time
      var produceTask = ProduceAsync(source, block, cancel: cancel);
      var result = new List<R>();
      var newResults = new List<R>();
      while (true) {
        if (produceTask.IsFaulted)
          break;

        var outputAvailableTask = block.OutputAvailableAsync();
        var completedTask = await Task.WhenAny(outputAvailableTask, Task.Delay(progressPeriod)).ConfigureAwait(false);
        if (completedTask == outputAvailableTask) {
          var available = await outputAvailableTask.ConfigureAwait(false);
          if (!available)
            break;
          var item = await block.ReceiveAsync().ConfigureAwait(false);
          newResults.Add(item);
          result.Add(item);
        }

        var elapsed = swProgress.Elapsed;
        if (elapsed > progressPeriod) {
          progressUpdate?.Invoke(new(newResults.Count, result.Count, elapsed));
          swProgress.Restart();
          newResults.Clear();
        }
      }

      await produceTask.ConfigureAwait(false);
      await block.Completion.ConfigureAwait(false);

      return result;
    }
  }

  public class BulkProgressInfo {
    public BulkProgressInfo(int completed, int completedTotal, TimeSpan elapsed) {
      Completed = completed;
      CompletedTotal = completedTotal;
      Elapsed = elapsed;
    }

    public int      Completed      { get; }
    public int      CompletedTotal { get; }
    public TimeSpan Elapsed        { get; }

    public Speed Speed(string units) => Completed.Speed(units, Elapsed);
  }
}