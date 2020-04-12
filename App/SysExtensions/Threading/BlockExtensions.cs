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
      await ProduceAsync(source, block);
      await block.Completion;
    }

    /// <summary>Simplified method for async operations that don't need to be chained, and when the result can fit in memory</summary>
    public static async Task<IReadOnlyCollection<R>> BlockTransform<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> transform, int parallelism = 1, int? capacity = null,
      Action<BulkProgressInfo> progressUpdate = null, TimeSpan progressPeriod = default) {
      progressPeriod = progressPeriod == default ? 60.Seconds() : progressPeriod;
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism, EnsureOrdered = false};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var block = new TransformBlock<T, R>(transform, options);

      var swProgress = Stopwatch.StartNew();

      // by producing asynchronously and using SendAsync we can throttle how much we can form the source and consume at the same time
      var produceTask = ProduceAsync(source, block);
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
          progressUpdate?.Invoke(new BulkProgressInfo(newResults.Count, result.Count, elapsed));
          swProgress.Restart();
          newResults.Clear();
        }
      }

      await produceTask;
      await block.Completion;

      return result;
    }

    static async Task ProduceAsync<T>(this IEnumerable<T> source, ITargetBlock<T> block) {
      foreach (var item in source) await block.SendAsync(item);
      block.Complete();
    }
  }

  public class BulkProgressInfo {
    public int      Completed      { get; }
    public int      CompletedTotal { get; }
    public TimeSpan Elapsed        { get; }

    public BulkProgressInfo(int completed, int completedTotal, TimeSpan elapsed) {
      Completed = completed;
      CompletedTotal = completedTotal;
      Elapsed = elapsed;
    }

    public Speed Speed(string units) => Completed.Speed(units, Elapsed);
  }
}