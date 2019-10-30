using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using SysExtensions.Text;

namespace SysExtensions.Threading {
  public static class BlockExtensions {
    public static async Task BlockAction<T>(this IEnumerable<T> source, Func<T, Task> action, int parallelism = 1, int? capacity = null) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism, EnsureOrdered = false};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;

      var block = new ActionBlock<T>(action, options);
      var produce = Produce(source, block);

      await Task.WhenAll(produce, block.Completion);
    }

    /// <summary>
    ///   Simplified method for async operations that don't need to be chained, and when the result can fit in memory
    /// </summary>
    public static async Task<IReadOnlyCollection<R>> BlockTransform<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> transform, int parallelism = 1, int? capacity = null,
      Action<BulkProgressInfo<R>> progressUpdate = null, TimeSpan progressPeriod = default(TimeSpan)) {
      progressPeriod = progressPeriod == default(TimeSpan) ? 60.Seconds() : progressPeriod;
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism, EnsureOrdered = false};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var block = new TransformBlock<T, R>(transform, options);

      var totalProgress = Stopwatch.StartNew();
      var swProgress = Stopwatch.StartNew();

      // by producing asynchronously and using SendAsync we can throttle how much we can form the source and consume at the same time
      var produce = Produce(source, block);
      var result = new List<R>();
      var newResults = new List<R>();
      while (true) {
        var outputAvailableTask = block.OutputAvailableAsync();

        var completedTask = await Task.WhenAny(outputAvailableTask, Task.Delay(progressPeriod));
        if (completedTask == outputAvailableTask) {
          var available = await outputAvailableTask;
          if (!available)
            break;
          var item = await block.ReceiveAsync();
          newResults.Add(item);
          result.Add(item);
        }

        var elapsed = swProgress.Elapsed;
        if (elapsed > progressPeriod) {
          progressUpdate?.Invoke(new BulkProgressInfo<R>(result, newResults, elapsed));
          swProgress.Restart();
          newResults.Clear();
        }
      }

      //progressUpdate?.Invoke(new BulkProgressInfo<R>(result, result, totalProgress.Elapsed));
      await Task.WhenAll(produce, block.Completion);

      return result;
    }

/*

        public static async Task BatchTransform<T, R>(this IEnumerable<T> source,
            Func<T, Task<R>> transform, Func<R, Task> batchAction, int parallelism = 1, int batch = 1000) {
            var options = new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = parallelism };
            var transformBlock = new TransformBlock<T, R>(transform, options);

            var batchBlock = new BatchBlock<R>(batch);

            batchBlock.LinkTo()
        }*/

    public static async Task Produce<T>(this IEnumerable<T> source, ITargetBlock<T> block) {
      foreach (var item in source)
        await block.SendAsync(item);
      block.Complete();
    }
  }

  public class BulkProgressInfo<T> {
    public IReadOnlyCollection<T> Results { get; }
    public IReadOnlyCollection<T> NewItems { get; }
    public TimeSpan Elapsed { get; }

    public BulkProgressInfo(IReadOnlyCollection<T> results, IReadOnlyCollection<T> newItems, TimeSpan elapsed) {
      Results = results;
      NewItems = newItems;
      Elapsed = elapsed;
    }

    public Speed Speed(string units) => NewItems.Count.Speed(units, Elapsed);
  }
}